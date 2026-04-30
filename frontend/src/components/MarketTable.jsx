import { useMemo, useState } from "react";
import { Card, Col, Divider, Input, Row, Table } from "antd";
import dayjs from "dayjs";
import ReactECharts from "echarts-for-react";
import { formatPct, formatUsd } from "../utils/format";

function marketTs(slug) {
  const ts = Number(String(slug || "").split("-").pop());
  if (!Number.isFinite(ts) || ts <= 0) {
    return null;
  }
  return ts;
}

function marketLocalTime(slug) {
  const ts = marketTs(slug);
  if (!ts) {
    return "-";
  }
  return dayjs(ts * 1000).format("YYYY-MM-DD HH:mm:ss");
}

function marketPrefix(slug) {
  const raw = String(slug || "").trim().toLowerCase();
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

function formatTokenQty(value) {
  return Number(value || 0).toFixed(4);
}

function formatTokenPrice(value) {
  if (value == null) {
    return "-";
  }
  return Number(value).toFixed(4);
}

function entryDirection(record) {
  const tokens = Array.isArray(record?.tokens) ? record.tokens : [];
  const sides = tokens
    .filter((token) => Number(token.buy_qty || 0) > 0 || Number(token.entry_amount_usdc || 0) > 0)
    .map((token) => String(token.outcome || "").trim())
    .filter(Boolean);

  const uniqueSides = [...new Set(sides)];
  if (!uniqueSides.length) {
    return "-";
  }
  if (uniqueSides.length === 1) {
    return uniqueSides[0];
  }
  return "Both";
}

function entryAmount(record) {
  const tokens = Array.isArray(record?.tokens) ? record.tokens : [];
  return tokens.reduce((sum, token) => sum + Number(token.entry_amount_usdc || 0), 0);
}

function avgEntryPrice(record) {
  const tokens = Array.isArray(record?.tokens) ? record.tokens : [];
  const totalBuyQty = tokens.reduce((sum, token) => sum + Number(token.buy_qty || 0), 0);
  if (totalBuyQty <= 1e-12) {
    return null;
  }
  const totalEntryAmount = entryAmount(record);
  return totalEntryAmount / totalBuyQty;
}

function buildHistogram(values, bins = 12) {
  if (!values.length) {
    return { labels: [], counts: [] };
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (Math.abs(max - min) < 1e-12) {
    return { labels: [min.toFixed(2)], counts: [values.length] };
  }
  const step = (max - min) / bins;
  const counts = new Array(bins).fill(0);
  values.forEach((value) => {
    const idx = Math.min(bins - 1, Math.max(0, Math.floor((value - min) / step)));
    counts[idx] += 1;
  });
  const labels = counts.map((_item, idx) => {
    const start = min + idx * step;
    const end = start + step;
    return `${start.toFixed(1)}~${end.toFixed(1)}`;
  });
  return { labels, counts };
}

const columns = [
  {
    title: "Market",
    dataIndex: "market_slug",
    key: "market_slug",
    ellipsis: true,
    width: 280,
    sorter: (a, b) => String(a.market_slug || "").localeCompare(String(b.market_slug || "")),
  },
  {
    title: "Trade Time",
    key: "trade_time",
    width: 180,
    render: (_value, record) => marketLocalTime(record.market_slug),
    sorter: (a, b) => Number(marketTs(a.market_slug) || 0) - Number(marketTs(b.market_slug) || 0),
  },
  {
    title: "Realized PnL",
    dataIndex: "realized_pnl_usdc",
    key: "realized_pnl_usdc",
    render: (value) => formatUsd(value),
    sorter: (a, b) => Number(a.realized_pnl_usdc || 0) - Number(b.realized_pnl_usdc || 0),
  },
  {
    title: "Taker Fee",
    dataIndex: "taker_fee_usdc",
    key: "taker_fee_usdc",
    render: (value) => formatUsd(value),
    sorter: (a, b) => Number(a.taker_fee_usdc || 0) - Number(b.taker_fee_usdc || 0),
  },
  {
    title: "Entry Side",
    key: "entry_side",
    width: 120,
    render: (_value, record) => entryDirection(record),
    sorter: (a, b) => entryDirection(a).localeCompare(entryDirection(b)),
  },
  {
    title: "Entry Amt",
    key: "entry_amount_usdc",
    width: 130,
    render: (_value, record) => formatUsd(entryAmount(record)),
    sorter: (a, b) => entryAmount(a) - entryAmount(b),
  },
  {
    title: "Avg Entry",
    key: "avg_entry_price",
    width: 120,
    render: (_value, record) => formatTokenPrice(avgEntryPrice(record)),
    sorter: (a, b) => Number(avgEntryPrice(a) || 0) - Number(avgEntryPrice(b) || 0),
  },
];

const makerRebateColumns = [
  {
    title: "Payout Time",
    dataIndex: "timestamp",
    key: "timestamp",
    width: 220,
    render: (value) => dayjs(Number(value || 0) * 1000).format("YYYY-MM-DD HH:mm:ss"),
    sorter: (a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0),
    defaultSortOrder: "descend",
  },
  {
    title: "Rebate",
    dataIndex: "usdc_size",
    key: "usdc_size",
    width: 140,
    render: (value) => formatUsd(value),
    sorter: (a, b) => Number(a.usdc_size || 0) - Number(b.usdc_size || 0),
  },
];

export default function MarketTable({ markets, makerRebates }) {
  const [marketQuery, setMarketQuery] = useState("");
  const [marketPage, setMarketPage] = useState(1);

  const filteredMarkets = useMemo(() => {
    const query = marketQuery.trim().toLowerCase();
    if (!query) {
      return markets || [];
    }
    return (markets || []).filter((market) => String(market.market_slug || "").toLowerCase().includes(query));
  }, [markets, marketQuery]);

  const pivotRows = useMemo(() => {
    const agg = new Map();
    (markets || []).forEach((market) => {
      const prefix = marketPrefix(market.market_slug);
      if (!agg.has(prefix)) {
        agg.set(prefix, {
          market_prefix: prefix,
          markets_count: 0,
          win_count: 0,
          pnl_sum: 0,
          taker_fee_sum: 0,
        });
      }
      const row = agg.get(prefix);
      const pnl = Number(market.realized_pnl_usdc || 0);
      row.markets_count += 1;
      if (pnl > 0) {
        row.win_count += 1;
      }
      row.pnl_sum += pnl;
      row.taker_fee_sum += Number(market.taker_fee_usdc || 0);
    });

    return [...agg.values()]
      .map((row) => {
        const count = Number(row.markets_count || 0);
        return {
          ...row,
          avg_pnl: count > 0 ? row.pnl_sum / count : 0,
          win_rate: count > 0 ? (row.win_count / count) * 100 : 0,
        };
      })
      .sort((a, b) => Number(b.pnl_sum || 0) - Number(a.pnl_sum || 0));
  }, [markets]);

  const tokenColumns = [
    {
      title: "Outcome",
      dataIndex: "outcome",
      key: "outcome",
      width: 100,
    },
    {
      title: "Avg Entry",
      dataIndex: "avg_entry_price",
      key: "avg_entry_price",
      width: 110,
      render: (value) => formatTokenPrice(value),
    },
    {
      title: "Entry Amt",
      dataIndex: "entry_amount_usdc",
      key: "entry_amount_usdc",
      width: 120,
      render: (value) => formatUsd(value),
    },
    {
      title: "Buy Qty",
      dataIndex: "buy_qty",
      key: "buy_qty",
      width: 110,
      render: (value) => formatTokenQty(value),
    },
    {
      title: "Sell Qty",
      dataIndex: "sell_qty",
      key: "sell_qty",
      width: 110,
      render: (value) => formatTokenQty(value),
    },
    {
      title: "Redeem Qty",
      dataIndex: "redeem_qty",
      key: "redeem_qty",
      width: 120,
      render: (value) => formatTokenQty(value),
    },
    {
      title: "End Pos",
      dataIndex: "ending_position_qty",
      key: "ending_position_qty",
      width: 110,
      render: (value) => formatTokenQty(value),
    },
    {
      title: "PnL",
      dataIndex: "realized_pnl_usdc",
      key: "realized_pnl_usdc",
      width: 110,
      render: (value) => formatUsd(value),
    },
    {
      title: "Trades",
      dataIndex: "trade_count",
      key: "trade_count",
      width: 90,
    },
  ];

  const pivotColumns = [
    {
      title: "Market Prefix",
      dataIndex: "market_prefix",
      key: "market_prefix",
      width: 160,
      sorter: (a, b) => String(a.market_prefix || "").localeCompare(String(b.market_prefix || "")),
    },
    {
      title: "Markets",
      dataIndex: "markets_count",
      key: "markets_count",
      width: 100,
      sorter: (a, b) => Number(a.markets_count || 0) - Number(b.markets_count || 0),
    },
    {
      title: "Win Rate",
      dataIndex: "win_rate",
      key: "win_rate",
      width: 120,
      render: (value) => formatPct(value),
      sorter: (a, b) => Number(a.win_rate || 0) - Number(b.win_rate || 0),
    },
    {
      title: "PnL Sum",
      dataIndex: "pnl_sum",
      key: "pnl_sum",
      width: 130,
      render: (value) => formatUsd(value),
      sorter: (a, b) => Number(a.pnl_sum || 0) - Number(b.pnl_sum || 0),
    },
    {
      title: "Avg PnL",
      dataIndex: "avg_pnl",
      key: "avg_pnl",
      width: 130,
      render: (value) => formatUsd(value),
      sorter: (a, b) => Number(a.avg_pnl || 0) - Number(b.avg_pnl || 0),
    },
    {
      title: "Taker Fee",
      dataIndex: "taker_fee_sum",
      key: "taker_fee_sum",
      width: 130,
      render: (value) => formatUsd(value),
      sorter: (a, b) => Number(a.taker_fee_sum || 0) - Number(b.taker_fee_sum || 0),
    },
  ];

  const pnlByPrefixOption = useMemo(() => {
    const xData = pivotRows.map((row) => row.market_prefix);
    const yData = pivotRows.map((row) => Number(row.pnl_sum || 0));
    return {
      animation: false,
      title: {
        text: "PnL by Prefix",
        left: 12,
        top: 8,
        textStyle: { fontSize: 14, fontWeight: 700, color: "#2b3950" },
      },
      grid: { left: 52, right: 16, top: 44, bottom: 42, containLabel: true },
      tooltip: { trigger: "axis" },
      xAxis: { type: "category", data: xData, axisLabel: { color: "#607089" } },
      yAxis: {
        type: "value",
        axisLabel: { color: "#607089", formatter: (value) => Number(value).toFixed(2) },
        splitLine: { lineStyle: { color: "rgba(146,160,181,0.2)" } },
      },
      series: [
        {
          type: "bar",
          data: yData,
          itemStyle: {
            color: (params) => (Number(params.value || 0) >= 0 ? "#2ca7b4" : "#e05757"),
            borderRadius: [4, 4, 0, 0],
          },
          barMaxWidth: 28,
        },
      ],
    };
  }, [pivotRows]);

  const winRateOption = useMemo(() => {
    const xData = pivotRows.map((row) => row.market_prefix);
    const yData = pivotRows.map((row) => Number(row.win_rate || 0));
    return {
      animation: false,
      title: {
        text: "Win Rate by Prefix",
        left: 12,
        top: 8,
        textStyle: { fontSize: 14, fontWeight: 700, color: "#2b3950" },
      },
      grid: { left: 50, right: 16, top: 44, bottom: 42, containLabel: true },
      tooltip: { trigger: "axis", formatter: (params) => `${params?.[0]?.axisValue}: ${Number(params?.[0]?.value || 0).toFixed(1)}%` },
      xAxis: { type: "category", data: xData, axisLabel: { color: "#607089" } },
      yAxis: {
        type: "value",
        min: 0,
        max: 100,
        axisLabel: { color: "#607089", formatter: (value) => `${Number(value).toFixed(0)}%` },
        splitLine: { lineStyle: { color: "rgba(146,160,181,0.2)" } },
      },
      series: [
        {
          type: "bar",
          data: yData,
          itemStyle: { color: "#6f5ef9", borderRadius: [4, 4, 0, 0] },
          barMaxWidth: 28,
        },
      ],
    };
  }, [pivotRows]);

  const pnlDistOption = useMemo(() => {
    const values = (markets || []).map((item) => Number(item.realized_pnl_usdc || 0));
    const histogram = buildHistogram(values, 12);
    return {
      animation: false,
      title: {
        text: "PnL Distribution",
        left: 12,
        top: 8,
        textStyle: { fontSize: 14, fontWeight: 700, color: "#2b3950" },
      },
      grid: { left: 50, right: 16, top: 44, bottom: 58, containLabel: true },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: histogram.labels,
        axisLabel: { color: "#607089", rotate: 30, fontSize: 11 },
      },
      yAxis: {
        type: "value",
        axisLabel: { color: "#607089" },
        splitLine: { lineStyle: { color: "rgba(146,160,181,0.2)" } },
      },
      series: [
        {
          type: "bar",
          data: histogram.counts,
          itemStyle: { color: "#4f7cff", borderRadius: [4, 4, 0, 0] },
          barMaxWidth: 20,
        },
      ],
    };
  }, [markets]);

  return (
    <Card className="market-card" title="Market Summary" bodyStyle={{ padding: 0 }}>
      <div className="market-section">
        <div className="market-toolbar">
          <Input.Search
            allowClear
            value={marketQuery}
            onChange={(event) => {
              setMarketQuery(event.target.value);
              setMarketPage(1);
            }}
            placeholder="Search market name"
            style={{ maxWidth: 360 }}
          />
        </div>
        <Table
          size="small"
          rowKey="market_slug"
          columns={columns}
          dataSource={filteredMarkets}
          expandable={{
            expandedRowRender: (record) => (
              <Table
                size="small"
                rowKey={(token) => token.token_id}
                columns={tokenColumns}
                dataSource={record.tokens || []}
                pagination={false}
                scroll={{ x: 860 }}
              />
            ),
            rowExpandable: (record) => Array.isArray(record.tokens) && record.tokens.length > 0,
          }}
          pagination={{
            current: marketPage,
            pageSize: 8,
            onChange: (page) => setMarketPage(page),
          }}
          scroll={{ x: 1020 }}
          sortDirections={["descend", "ascend"]}
        />
      </div>

      <Divider orientation="left" style={{ margin: "8px 0 12px" }}>
        Pivot Summary
      </Divider>

      <div className="market-section">
        <Table
          size="small"
          rowKey="market_prefix"
          columns={pivotColumns}
          dataSource={pivotRows}
          pagination={false}
          scroll={{ x: 820 }}
          sortDirections={["descend", "ascend"]}
        />
      </div>

      <Divider orientation="left" style={{ margin: "8px 0 12px" }}>
        Daily Maker Rebate
      </Divider>

      <div className="market-section">
        <Table
          size="small"
          rowKey={(record) => `${record.timestamp}-${record.usdc_size}`}
          columns={makerRebateColumns}
          dataSource={makerRebates || []}
          pagination={{ pageSize: 10 }}
          locale={{ emptyText: "No maker rebate records" }}
          scroll={{ x: 360 }}
          sortDirections={["descend", "ascend"]}
        />
      </div>

      <Divider orientation="left" style={{ margin: "8px 0 12px" }}>
        Statistics
      </Divider>

      <div className="market-section">
        <Row gutter={[12, 12]}>
          <Col xs={24} xl={8}>
            <div className="market-chart-wrap">
              <ReactECharts option={pnlByPrefixOption} notMerge lazyUpdate style={{ height: "100%", width: "100%" }} />
            </div>
          </Col>
          <Col xs={24} xl={8}>
            <div className="market-chart-wrap">
              <ReactECharts option={winRateOption} notMerge lazyUpdate style={{ height: "100%", width: "100%" }} />
            </div>
          </Col>
          <Col xs={24} xl={8}>
            <div className="market-chart-wrap">
              <ReactECharts option={pnlDistOption} notMerge lazyUpdate style={{ height: "100%", width: "100%" }} />
            </div>
          </Col>
        </Row>
      </div>
    </Card>
  );
}
