import { useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";
import dayjs from "dayjs";
import { Card, Col, Row, Segmented } from "antd";
import { symbolColor } from "../utils/format";

function axisTimeLabel(value) {
  return dayjs(Number(value) * 1000).format("MM/DD HH:mm");
}

function tooltipTimeLabel(value) {
  return dayjs(Number(value) * 1000).format("YYYY-MM-DD HH:mm:ss");
}

function tooltipFormatter(params) {
  const rows = Array.isArray(params) ? params : [params];
  if (!rows.length) {
    return "";
  }
  const ts = Number(rows[0].axisValue);
  const lines = [`Time: ${tooltipTimeLabel(ts)}`];
  for (const row of rows) {
    const marker = row.marker || "";
    const seriesName = row.seriesName || "-";
    const value = Array.isArray(row.value) ? row.value[1] : row.value;
    const display = value == null ? "-" : Number(value).toFixed(4);
    lines.push(`${marker} ${seriesName}: ${display}`);
  }
  return lines.join("<br/>");
}

function seriesMap(points) {
  const map = new Map();
  (points || []).forEach((point) => {
    map.set(Number(point.ts), Number(point.value || 0));
  });
  return map;
}

function unionSortedTimestamps(...maps) {
  const all = new Set();
  maps.forEach((map) => {
    map.forEach((_value, ts) => all.add(Number(ts)));
  });
  return [...all].sort((a, b) => a - b);
}

function hexToRgba(hex, alpha) {
  const value = String(hex || "").replace("#", "");
  if (value.length !== 6) {
    return `rgba(47,167,182,${alpha})`;
  }
  const r = Number.parseInt(value.slice(0, 2), 16);
  const g = Number.parseInt(value.slice(2, 4), 16);
  const b = Number.parseInt(value.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

export default function PnlCharts({ totalSeries, totalSeriesNoFee, symbolSeries, symbolSeriesNoFee }) {
  const [viewMode, setViewMode] = useState("net");

  const showNet = viewMode === "net" || viewMode === "compare";
  const showNoFee = viewMode === "no_fee" || viewMode === "compare";

  const totalOption = useMemo(() => {
    const netMap = seriesMap(totalSeries);
    const noFeeMap = seriesMap(totalSeriesNoFee);
    const xData = unionSortedTimestamps(showNet ? netMap : new Map(), showNoFee ? noFeeMap : new Map());

    const series = [];
    if (showNet) {
      series.push({
        name: "Net PnL",
        type: "line",
        smooth: 0.32,
        smoothMonotone: "x",
        showSymbol: false,
        connectNulls: true,
        lineStyle: {
          width: 3,
          color: "#2ca7b4",
          cap: "round",
          join: "round",
        },
        data: xData.map((ts) => (netMap.has(ts) ? netMap.get(ts) : null)),
      });
    }

    if (showNoFee) {
      series.push({
        name: "No-Fee PnL",
        type: "line",
        smooth: 0.32,
        smoothMonotone: "x",
        showSymbol: false,
        connectNulls: true,
        lineStyle: {
          width: 2.8,
          color: "#f59e0b",
          type: showNet ? "dashed" : "solid",
          cap: "round",
          join: "round",
        },
        data: xData.map((ts) => (noFeeMap.has(ts) ? noFeeMap.get(ts) : null)),
      });
    }

    return {
      animation: false,
      title: {
        text: "Total PnL Curve",
        left: 14,
        top: 8,
        textStyle: {
          color: "#213047",
          fontWeight: 700,
          fontSize: 18,
        },
      },
      tooltip: {
        trigger: "axis",
        formatter: tooltipFormatter,
      },
      legend: {
        top: 8,
        right: 18,
      },
      grid: {
        left: 78,
        right: 30,
        top: 84,
        bottom: 44,
        containLabel: true,
      },
      xAxis: {
        type: "category",
        position: "bottom",
        boundaryGap: false,
        data: xData,
        axisLine: {
          onZero: false,
        },
        axisLabel: {
          color: "#617089",
          hideOverlap: true,
          formatter: axisTimeLabel,
        },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          color: "#617089",
          formatter: (value) => Number(value).toFixed(2),
          margin: 12,
        },
        splitLine: {
          lineStyle: {
            color: "rgba(146,160,181,0.2)",
          },
        },
      },
      series,
    };
  }, [totalSeries, totalSeriesNoFee, showNet, showNoFee]);

  const symbolOption = useMemo(() => {
    const symbolSet = new Set();
    if (showNet) {
      Object.keys(symbolSeries || {}).forEach((symbol) => symbolSet.add(symbol));
    }
    if (showNoFee) {
      Object.keys(symbolSeriesNoFee || {}).forEach((symbol) => symbolSet.add(symbol));
    }
    const symbols = [...symbolSet].sort();

    const mapsBySymbolNet = {};
    const mapsBySymbolNoFee = {};
    symbols.forEach((symbol) => {
      mapsBySymbolNet[symbol] = seriesMap((symbolSeries || {})[symbol] || []);
      mapsBySymbolNoFee[symbol] = seriesMap((symbolSeriesNoFee || {})[symbol] || []);
    });

    const allTsSet = new Set();
    symbols.forEach((symbol) => {
      const netMap = mapsBySymbolNet[symbol];
      const noFeeMap = mapsBySymbolNoFee[symbol];
      if (showNet) {
        netMap.forEach((_value, ts) => allTsSet.add(ts));
      }
      if (showNoFee) {
        noFeeMap.forEach((_value, ts) => allTsSet.add(ts));
      }
    });
    const xData = [...allTsSet].sort((a, b) => a - b);

    const series = [];
    symbols.forEach((symbol) => {
      const baseColor = symbolColor(symbol);

      if (showNet) {
        series.push({
          name: symbol.toUpperCase(),
          type: "line",
          smooth: 0.32,
          smoothMonotone: "x",
          showSymbol: false,
          connectNulls: true,
          lineStyle: {
            width: 2.6,
            color: baseColor,
            cap: "round",
            join: "round",
          },
          data: xData.map((ts) => (mapsBySymbolNet[symbol].has(ts) ? mapsBySymbolNet[symbol].get(ts) : null)),
        });
      }

      if (showNoFee) {
        series.push({
          name: showNet ? `${symbol.toUpperCase()} (No Fee)` : symbol.toUpperCase(),
          type: "line",
          smooth: 0.32,
          smoothMonotone: "x",
          showSymbol: false,
          connectNulls: true,
          lineStyle: {
            width: 2.4,
            color: showNet ? hexToRgba(baseColor, 0.8) : baseColor,
            type: showNet ? "dashed" : "solid",
            cap: "round",
            join: "round",
          },
          data: xData.map((ts) => (mapsBySymbolNoFee[symbol].has(ts) ? mapsBySymbolNoFee[symbol].get(ts) : null)),
        });
      }
    });

    return {
      animation: false,
      title: {
        text: "Symbol PnL Curve",
        left: 14,
        top: 8,
        textStyle: {
          color: "#213047",
          fontWeight: 700,
          fontSize: 18,
        },
      },
      tooltip: {
        trigger: "axis",
        formatter: tooltipFormatter,
      },
      legend: {
        type: "scroll",
        top: 8,
        left: 200,
        right: 16,
      },
      grid: {
        left: 78,
        right: 30,
        top: 96,
        bottom: 44,
        containLabel: true,
      },
      xAxis: {
        type: "category",
        position: "bottom",
        boundaryGap: false,
        data: xData,
        axisLine: {
          onZero: false,
        },
        axisLabel: {
          color: "#617089",
          hideOverlap: true,
          formatter: axisTimeLabel,
        },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          color: "#617089",
          formatter: (value) => Number(value).toFixed(2),
          margin: 12,
        },
        splitLine: {
          lineStyle: {
            color: "rgba(146,160,181,0.2)",
          },
        },
      },
      series,
    };
  }, [symbolSeries, symbolSeriesNoFee, showNet, showNoFee]);

  return (
    <Card
      className="chart-card"
      bodyStyle={{ padding: 12 }}
      extra={
        <Segmented
          value={viewMode}
          onChange={setViewMode}
          options={[
            { label: "Net", value: "net" },
            { label: "No Fee", value: "no_fee" },
            { label: "Compare", value: "compare" },
          ]}
        />
      }
    >
      <Row gutter={[12, 12]}>
        <Col xs={24} lg={12}>
          <div className="chart-wrap">
            <ReactECharts option={totalOption} notMerge lazyUpdate style={{ height: "100%", width: "100%" }} />
          </div>
        </Col>
        <Col xs={24} lg={12}>
          <div className="chart-wrap">
            <ReactECharts option={symbolOption} notMerge lazyUpdate style={{ height: "100%", width: "100%" }} />
          </div>
        </Col>
      </Row>
    </Card>
  );
}
