import { Layout, message } from "antd";
import { useEffect, useMemo, useRef, useState } from "react";
import AdvancedModal from "./components/AdvancedModal";
import ConfigPanel from "./components/ConfigPanel";
import MakerRebateTable from "./components/MakerRebateTable";
import MarketTable from "./components/MarketTable";
import PnlCharts from "./components/PnlCharts";
import QuantMetricsPanel from "./components/QuantMetricsPanel";
import StatusCard from "./components/StatusCard";
import { buildDefaultForm, EMPTY_SUMMARY } from "./constants";
import { createRun, fetchRunResult, stopRun } from "./services/api";
import { parseDateTimeTextToUnixSeconds, toDateTimeText } from "./utils/dateTime";

const IDLE_WARNING = "No warnings. Strategy is running with live market aggregation.";
const MAX_DRAWDOWN_MARKERS = 8;
const MIN_DRAWDOWN_DELTA_USDC = 0.5;
const LIVE_CURVE_BUCKET_SEC = 15 * 60;
const MAX_LIVE_CURVE_POINT_EVENTS = 12000;
const ACTIVE_RUN_STATUSES = new Set(["PENDING", "RUNNING", "FINALIZING", "STOPPING"]);

function extractMarketPrefix(marketSlug) {
  const raw = String(marketSlug || "").trim().toLowerCase();
  if (!raw) {
    return "unknown";
  }

  const parts = raw.split("-").filter(Boolean);
  if (parts.length >= 3) {
    const interval = parts[2].endsWith("m") ? parts[2].slice(0, -1) : parts[2];
    if (parts[1] === "updown") {
      return `${parts[0]}-${interval}`;
    }
    return `${parts[0]}-${parts[1]}-${interval}`;
  }

  return parts[0] || "unknown";
}

function reduceCurveDelta(points) {
  const byTs = new Map();
  for (const point of points || []) {
    const ts = Number(point.timestamp || 0);
    const delta = Number(point.delta_realized_pnl_usdc || 0);
    byTs.set(ts, (byTs.get(ts) || 0) + delta);
  }
  return byTs;
}

function buildDrawdownMarkers(marketCurves) {
  const worstDropBySlug = new Map();

  Object.entries(marketCurves || {}).forEach(([marketSlug, points]) => {
    const byTs = reduceCurveDelta(points);
    const timestamps = [...byTs.keys()].sort((a, b) => a - b);
    if (!timestamps.length) {
      return;
    }

    let worst = null;
    timestamps.forEach((ts) => {
      const delta = Number(byTs.get(ts) || 0);
      if (delta >= -MIN_DRAWDOWN_DELTA_USDC) {
        return;
      }
      if (!worst || delta < worst.delta) {
        worst = {
          ts,
          delta,
          marketSlug,
          marketPrefix: extractMarketPrefix(marketSlug),
        };
      }
    });

    if (worst) {
      worstDropBySlug.set(marketSlug, worst);
    }
  });

  return [...worstDropBySlug.values()]
    .sort((a, b) => a.delta - b.delta)
    .slice(0, MAX_DRAWDOWN_MARKERS);
}

function hasTradeActivity(market) {
  const tokens = Array.isArray(market?.tokens) ? market.tokens : [];
  return tokens.some((token) => Number(token.trade_count || 0) > 0);
}

function isTruthyQueryFlag(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function parseBootstrapFromQuery(searchText) {
  const params = new URLSearchParams(searchText || "");
  const patch = {};

  const fieldMap = {
    address: "address",
    keywords: "keywords",
    start_time: "startTime",
    end_time: "endTime",
    fee_rate_bps: "feeRateBps",
    missing_cost_warn_qty: "missingCostWarnQty",
    concurrency: "concurrency",
    page_limit: "pageLimit",
  };

  Object.entries(fieldMap).forEach(([queryKey, formKey]) => {
    const value = params.get(queryKey);
    if (value !== null && String(value).trim() !== "") {
      patch[formKey] = value;
    }
  });

  if (!patch.keywords) {
    const legacyIntervals = params.get("intervals");
    if (legacyIntervals !== null && String(legacyIntervals).trim() !== "") {
      patch.keywords = legacyIntervals
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
        .map((item) => (item.endsWith("m") ? item : `${item}m`))
        .join(",");
    }
  }

  if (!patch.startTime) {
    const startTs = Number(params.get("start_ts"));
    if (Number.isFinite(startTs) && startTs > 0) {
      patch.startTime = toDateTimeText(new Date(startTs * 1000));
    }
  }

  if (!patch.endTime) {
    const endTs = Number(params.get("end_ts"));
    if (Number.isFinite(endTs) && endTs > 0) {
      patch.endTime = toDateTimeText(new Date(endTs * 1000));
    }
  }

  const autoStart = isTruthyQueryFlag(params.get("auto_start"));
  return { patch, autoStart };
}

function bucketLiveCurveTs(timestamp) {
  const ts = Number(timestamp || 0);
  if (!Number.isFinite(ts) || ts <= 0) {
    return 0;
  }
  return Math.floor(ts / LIVE_CURVE_BUCKET_SEC) * LIVE_CURVE_BUCKET_SEC;
}

export default function App({ serverDefaults }) {
  const [formData, setFormData] = useState(() => buildDefaultForm(serverDefaults));
  const [runId, setRunId] = useState(null);
  const [runStatus, setRunStatus] = useState("IDLE");
  const [running, setRunning] = useState(false);
  const [statusMessage, setStatusMessage] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);

  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [summary, setSummary] = useState(EMPTY_SUMMARY);
  const [markets, setMarkets] = useState([]);
  const [makerRebates, setMakerRebates] = useState([]);
  const [downloads, setDownloads] = useState(null);
  const [warnings, setWarnings] = useState([]);

  const [totalSeries, setTotalSeries] = useState([]);
  const [totalSeriesNoFee, setTotalSeriesNoFee] = useState([]);
  const [symbolSeries, setSymbolSeries] = useState({});
  const [symbolSeriesNoFee, setSymbolSeriesNoFee] = useState({});
  const [drawdownMarkers, setDrawdownMarkers] = useState([]);

  const eventSourceRef = useRef(null);
  const bootstrapHandledRef = useRef(false);
  const totalByTsRef = useRef(new Map());
  const symbolByTsRef = useRef(new Map());
  const totalByTsNoFeeRef = useRef(new Map());
  const symbolByTsNoFeeRef = useRef(new Map());
  const liveCurvePointCountRef = useRef(0);
  const liveCurvesPausedRef = useRef(false);

  useEffect(() => {
    const end = new Date();
    end.setSeconds(0, 0);
    const start = new Date(end);
    start.setHours(0, 0, 0, 0);
    setFormData((prev) => ({
      ...prev,
      startTime: prev.startTime || toDateTimeText(start),
      endTime: prev.endTime || toDateTimeText(end),
    }));
  }, []);

  useEffect(
    () => () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    },
    [],
  );

  useEffect(() => {
    if (bootstrapHandledRef.current) {
      return;
    }
    bootstrapHandledRef.current = true;

    const { patch, autoStart } = parseBootstrapFromQuery(window.location.search);
    if (!Object.keys(patch).length) {
      return;
    }

    setFormData((prev) => {
      const merged = { ...prev, ...patch };
      if (autoStart) {
        setTimeout(() => {
          handleStart(merged);
        }, 0);
      }
      return merged;
    });
  }, []);

  const roi = useMemo(() => {
    const base =
      Math.max(1, Math.abs(Number(summary.total_taker_fee_usdc || 0)) + Math.abs(Number(summary.total_maker_reward_usdc || 0)));
    return (Number(summary.total_realized_pnl_usdc || 0) / base) * 100;
  }, [summary]);

  const winRate = useMemo(() => {
    if (!markets.length) {
      return 0;
    }
    const winCount = markets.filter((market) => Number(market.realized_pnl_usdc || 0) > 0).length;
    return (winCount / markets.length) * 100;
  }, [markets]);

  const rawProgressPercent = progress.total > 0 ? Math.round((progress.current / progress.total) * 100) : 0;
  const progressPercent =
    runStatus === "COMPLETED"
      ? 100
      : ACTIVE_RUN_STATUSES.has(runStatus)
        ? Math.min(rawProgressPercent, 99)
        : rawProgressPercent;
  const latestWarning = warnings[0] || IDLE_WARNING;
  const statusText = statusMessage || latestWarning;

  function updateField(key, value) {
    setFormData((prev) => ({ ...prev, [key]: value }));
  }

  function pushWarning(text) {
    if (!text) {
      return;
    }
    setWarnings((prev) => [text, ...prev].slice(0, 200));
  }

  function clearCurves() {
    totalByTsRef.current = new Map();
    symbolByTsRef.current = new Map();
    totalByTsNoFeeRef.current = new Map();
    symbolByTsNoFeeRef.current = new Map();
    liveCurvePointCountRef.current = 0;
    liveCurvesPausedRef.current = false;
    setTotalSeries([]);
    setTotalSeriesNoFee([]);
    setSymbolSeries({});
    setSymbolSeriesNoFee({});
    setDrawdownMarkers([]);
  }

  function shouldSkipLiveCurveUpdate() {
    if (liveCurvesPausedRef.current) {
      return true;
    }
    liveCurvePointCountRef.current += 1;
    if (liveCurvePointCountRef.current <= MAX_LIVE_CURVE_POINT_EVENTS) {
      return false;
    }

    liveCurvesPausedRef.current = true;
    totalByTsRef.current = new Map();
    symbolByTsRef.current = new Map();
    totalByTsNoFeeRef.current = new Map();
    symbolByTsNoFeeRef.current = new Map();
    pushWarning("INFO: live curve updates paused for high-frequency runs; compact aggregated curves will load when the run completes");
    return true;
  }

  function clearRunData() {
    clearCurves();
    setProgress({ current: 0, total: 0 });
    setStatusMessage("");
    setSummary(EMPTY_SUMMARY);
    setMarkets([]);
    setMakerRebates([]);
    setDownloads(null);
    setWarnings([]);
  }

  function recomputeTotalSeries() {
    const byTs = totalByTsRef.current;
    const timestamps = [...byTs.keys()].sort((a, b) => a - b);
    let cumulative = 0;
    const points = timestamps.map((ts) => {
      cumulative += Number(byTs.get(ts) || 0);
      return { ts, value: cumulative };
    });
    setTotalSeries(points);
  }

  function recomputeSymbolSeries(symbol) {
    const map = symbolByTsRef.current.get(symbol);
    if (!map) {
      return;
    }
    const timestamps = [...map.keys()].sort((a, b) => a - b);
    let cumulative = 0;
    const points = timestamps.map((ts) => {
      cumulative += Number(map.get(ts) || 0);
      return { ts, value: cumulative };
    });

    setSymbolSeries((prev) => ({ ...prev, [symbol]: points }));
  }

  function recomputeAllSymbolSeries() {
    const next = {};
    for (const [symbol, byTs] of symbolByTsRef.current.entries()) {
      const timestamps = [...byTs.keys()].sort((a, b) => a - b);
      let cumulative = 0;
      next[symbol] = timestamps.map((ts) => {
        cumulative += Number(byTs.get(ts) || 0);
        return { ts, value: cumulative };
      });
    }
    setSymbolSeries(next);
  }

  function appendTotalDelta(timestamp, delta) {
    const ts = bucketLiveCurveTs(timestamp);
    const byTs = totalByTsRef.current;
    byTs.set(ts, (byTs.get(ts) || 0) + Number(delta || 0));
    recomputeTotalSeries();
  }

  function appendSymbolDelta(symbol, timestamp, delta) {
    const ts = bucketLiveCurveTs(timestamp);
    if (!symbolByTsRef.current.has(symbol)) {
      symbolByTsRef.current.set(symbol, new Map());
    }
    const byTs = symbolByTsRef.current.get(symbol);
    byTs.set(ts, (byTs.get(ts) || 0) + Number(delta || 0));
    recomputeSymbolSeries(symbol);
  }

  function rebuildCurvesFromReport(report) {
    totalByTsRef.current = reduceCurveDelta(report.total_curve || []);

    symbolByTsRef.current = new Map();
    const marketCurves = report.market_curves || {};
    setDrawdownMarkers(buildDrawdownMarkers(marketCurves));
    Object.entries(marketCurves).forEach(([marketSlug, points]) => {
      const marketPrefix = extractMarketPrefix(marketSlug);
      if (!symbolByTsRef.current.has(marketPrefix)) {
        symbolByTsRef.current.set(marketPrefix, new Map());
      }
      const symbolMap = symbolByTsRef.current.get(marketPrefix);
      const reduced = reduceCurveDelta(points);
      reduced.forEach((delta, ts) => {
        symbolMap.set(ts, (symbolMap.get(ts) || 0) + delta);
      });
    });

    recomputeTotalSeries();
    recomputeAllSymbolSeries();

    totalByTsNoFeeRef.current = reduceCurveDelta(report.total_curve_no_fee || []);
    const noFeeTimestamps = [...totalByTsNoFeeRef.current.keys()].sort((a, b) => a - b);
    let noFeeCumulative = 0;
    setTotalSeriesNoFee(
      noFeeTimestamps.map((ts) => {
        noFeeCumulative += Number(totalByTsNoFeeRef.current.get(ts) || 0);
        return { ts, value: noFeeCumulative };
      }),
    );

    symbolByTsNoFeeRef.current = new Map();
    const marketNoFeeCurves = report.market_curves_no_fee || {};
    Object.entries(marketNoFeeCurves).forEach(([marketSlug, points]) => {
      const marketPrefix = extractMarketPrefix(marketSlug);
      if (!symbolByTsNoFeeRef.current.has(marketPrefix)) {
        symbolByTsNoFeeRef.current.set(marketPrefix, new Map());
      }
      const byTs = symbolByTsNoFeeRef.current.get(marketPrefix);
      const reduced = reduceCurveDelta(points);
      reduced.forEach((delta, ts) => {
        byTs.set(ts, (byTs.get(ts) || 0) + delta);
      });
    });
    const nextNoFeeSymbolSeries = {};
    for (const [symbol, byTs] of symbolByTsNoFeeRef.current.entries()) {
      const timestamps = [...byTs.keys()].sort((a, b) => a - b);
      let cumulative = 0;
      nextNoFeeSymbolSeries[symbol] = timestamps.map((ts) => {
        cumulative += Number(byTs.get(ts) || 0);
        return { ts, value: cumulative };
      });
    }
    setSymbolSeriesNoFee(nextNoFeeSymbolSeries);
  }

  function recomputeTotalSeriesNoFee() {
    const byTs = totalByTsNoFeeRef.current;
    const timestamps = [...byTs.keys()].sort((a, b) => a - b);
    let cumulative = 0;
    setTotalSeriesNoFee(
      timestamps.map((ts) => {
        cumulative += Number(byTs.get(ts) || 0);
        return { ts, value: cumulative };
      }),
    );
  }

  function recomputeSymbolSeriesNoFee(symbol) {
    const byTs = symbolByTsNoFeeRef.current.get(symbol);
    if (!byTs) {
      return;
    }
    const timestamps = [...byTs.keys()].sort((a, b) => a - b);
    let cumulative = 0;
    setSymbolSeriesNoFee((prev) => ({
      ...prev,
      [symbol]: timestamps.map((ts) => {
        cumulative += Number(byTs.get(ts) || 0);
        return { ts, value: cumulative };
      }),
    }));
  }

  function appendTotalDeltaNoFee(timestamp, delta) {
    const ts = bucketLiveCurveTs(timestamp);
    const byTs = totalByTsNoFeeRef.current;
    byTs.set(ts, (byTs.get(ts) || 0) + Number(delta || 0));
    recomputeTotalSeriesNoFee();
  }

  function appendSymbolDeltaNoFee(symbol, timestamp, delta) {
    const ts = bucketLiveCurveTs(timestamp);
    if (!symbolByTsNoFeeRef.current.has(symbol)) {
      symbolByTsNoFeeRef.current.set(symbol, new Map());
    }
    const byTs = symbolByTsNoFeeRef.current.get(symbol);
    byTs.set(ts, (byTs.get(ts) || 0) + Number(delta || 0));
    recomputeSymbolSeriesNoFee(symbol);
  }

  async function loadResult(currentRunId) {
    const report = await fetchRunResult(currentRunId);
    if (!report) {
      return;
    }

    setMarkets((report.markets || []).filter(hasTradeActivity));
    setMakerRebates(report.maker_rebates || []);
    setSummary(report.summary || EMPTY_SUMMARY);
    setDownloads(report.artifacts || null);
    if (Array.isArray(report.warnings)) {
      const warningTexts = report.warnings
        .map((warning) => `${warning.code}: ${warning.message}`)
        .reverse();
      setWarnings((prev) => [...warningTexts, ...prev].slice(0, 200));
    }
    if (Array.isArray(report.total_series)) {
      totalByTsRef.current = new Map();
      symbolByTsRef.current = new Map();
      totalByTsNoFeeRef.current = new Map();
      symbolByTsNoFeeRef.current = new Map();
      setTotalSeries(report.total_series || []);
      setTotalSeriesNoFee(report.total_series_no_fee || []);
      setSymbolSeries(report.symbol_series || {});
      setSymbolSeriesNoFee(report.symbol_series_no_fee || {});
      setDrawdownMarkers(report.drawdown_markers || []);
      return;
    }

    rebuildCurvesFromReport(report);
  }

  function closeStream() {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }

  function attachStream(currentRunId) {
    closeStream();
    const stream = new EventSource(`/api/runs/${currentRunId}/stream`);
    eventSourceRef.current = stream;

    stream.addEventListener("run_started", (event) => {
      const data = JSON.parse(event.data || "{}");
      setRunStatus("RUNNING");
      setStatusMessage(data.message || "Processing markets");
      setProgress({ current: 0, total: Number(data.progress_total || 0) });
    });

    stream.addEventListener("status", (event) => {
      const data = JSON.parse(event.data || "{}");
      if (data.status) {
        setRunStatus(data.status);
      }
      if (data.message) {
        setStatusMessage(data.message);
      }
      if (data.progress_current !== undefined && data.progress_total !== undefined) {
        setProgress({
          current: Number(data.progress_current),
          total: Number(data.progress_total),
        });
      }
    });

    stream.addEventListener("progress", (event) => {
      const data = JSON.parse(event.data || "{}");
      if (data.current !== undefined && data.total !== undefined) {
        setProgress({ current: Number(data.current), total: Number(data.total) });
      }
    });

    stream.addEventListener("warning", (event) => {
      const data = JSON.parse(event.data || "{}");
      pushWarning(`${data.code}: ${data.message}`);
    });

    stream.addEventListener("point_total", (event) => {
      if (shouldSkipLiveCurveUpdate()) {
        return;
      }
      const data = JSON.parse(event.data || "{}");
      appendTotalDelta(data.timestamp, data.delta_realized_pnl_usdc);
    });

    stream.addEventListener("point_market", (event) => {
      if (shouldSkipLiveCurveUpdate()) {
        return;
      }
      const data = JSON.parse(event.data || "{}");
      appendSymbolDelta(extractMarketPrefix(data.market_slug), data.timestamp, data.delta_realized_pnl_usdc);
    });

    stream.addEventListener("point_total_no_fee", (event) => {
      if (shouldSkipLiveCurveUpdate()) {
        return;
      }
      const data = JSON.parse(event.data || "{}");
      appendTotalDeltaNoFee(data.timestamp, data.delta_realized_pnl_usdc);
    });

    stream.addEventListener("point_market_no_fee", (event) => {
      if (shouldSkipLiveCurveUpdate()) {
        return;
      }
      const data = JSON.parse(event.data || "{}");
      appendSymbolDeltaNoFee(extractMarketPrefix(data.market_slug), data.timestamp, data.delta_realized_pnl_usdc);
    });

    stream.addEventListener("completed", async () => {
      setRunStatus("COMPLETED");
      setRunning(false);
      setStatusMessage("");
      await loadResult(currentRunId);
      closeStream();
    });

    stream.addEventListener("stopped", async () => {
      setRunStatus("STOPPED");
      setRunning(false);
      setStatusMessage("");
      await loadResult(currentRunId);
      closeStream();
    });

    stream.addEventListener("run_error", (event) => {
      setRunStatus("FAILED");
      setRunning(false);
      setStatusMessage("");

      try {
        const data = JSON.parse(event.data || "{}");
        pushWarning(`ERROR: ${data.message || "run failed"}`);
      } catch (_error) {
        pushWarning("ERROR: run failed");
      }

      closeStream();
    });

    stream.addEventListener("error", () => {
      if (stream.readyState !== EventSource.CLOSED) {
        pushWarning("SSE reconnecting...");
      }
    });
  }

  function buildPayload(sourceFormData = formData) {
    const startTs = parseDateTimeTextToUnixSeconds(sourceFormData.startTime);
    const endTs = parseDateTimeTextToUnixSeconds(sourceFormData.endTime);
    if (startTs >= endTs) {
      throw new Error("end time must be later than start time");
    }

    return {
      address: sourceFormData.address.trim(),
      start_ts: startTs,
      end_ts: endTs,
      keywords: sourceFormData.keywords
        .split(",")
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean),
      fee_rate_bps: Number(sourceFormData.feeRateBps),
      missing_cost_warn_qty: Number(sourceFormData.missingCostWarnQty),
      concurrency: Number(sourceFormData.concurrency),
      page_limit: Number(sourceFormData.pageLimit),
      request_timeout_sec: 20,
      output_dir: "reports",
    };
  }

  async function handleStart(overrideFormData = null) {
    try {
      clearRunData();
      const payload = buildPayload(overrideFormData || formData);
      const created = await createRun(payload);

      setRunId(created.run_id);
      setRunStatus("PENDING");
      setStatusMessage("Starting analysis");
      setRunning(true);
      attachStream(created.run_id);
    } catch (error) {
      setRunStatus("FAILED");
      setStatusMessage("");
      pushWarning(`start failed: ${error.message || String(error)}`);
      message.error("Start failed");
    }
  }

  async function handleStop() {
    if (!runId) {
      return;
    }
    try {
      await stopRun(runId);
      setRunStatus("STOPPING");
      setStatusMessage("Stopping requested");
    } catch (error) {
      pushWarning(`stop failed: ${error.message || String(error)}`);
    }
  }

  async function handleToggleRun() {
    if (running) {
      await handleStop();
      return;
    }
    await handleStart();
  }

  function handleReset() {
    closeStream();
    setRunId(null);
    setRunStatus("IDLE");
    setRunning(false);
    setStatusMessage("");
    clearRunData();
  }

  return (
    <Layout className="page-layout">
      <div className="container">
        <StatusCard
          runStatus={runStatus}
          latestWarning={statusText}
          summary={summary}
          roi={roi}
          winRate={winRate}
          progressPercent={progressPercent}
        />

        <ConfigPanel
          formData={formData}
          updateField={updateField}
          downloads={downloads}
          onOpenAdvanced={() => setAdvancedOpen(true)}
          onToggleRun={handleToggleRun}
          onReset={handleReset}
          running={running}
          runStatus={runStatus}
        />

        <PnlCharts
          totalSeries={totalSeries}
          totalSeriesNoFee={totalSeriesNoFee}
          symbolSeries={symbolSeries}
          symbolSeriesNoFee={symbolSeriesNoFee}
          drawdownMarkers={drawdownMarkers}
        />

        <QuantMetricsPanel totalSeries={totalSeries} totalSeriesNoFee={totalSeriesNoFee} markets={markets} />

        <MakerRebateTable makerRebates={makerRebates} />

        <MarketTable markets={markets} />
      </div>

      <AdvancedModal open={advancedOpen} onClose={() => setAdvancedOpen(false)} formData={formData} updateField={updateField} />
    </Layout>
  );
}
