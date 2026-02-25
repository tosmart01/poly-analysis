import { Card, Table } from "antd";
import dayjs from "dayjs";
import { formatUsd } from "../utils/format";

function marketLocalTime(slug) {
  const ts = Number(String(slug || "").split("-").pop());
  if (!Number.isFinite(ts) || ts <= 0) {
    return "-";
  }
  return dayjs(ts * 1000).format("YYYY-MM-DD HH:mm:ss");
}

const columns = [
  {
    title: "Market",
    dataIndex: "market_slug",
    key: "market_slug",
    ellipsis: true,
    width: 280,
  },
  {
    title: "Trade Time",
    key: "trade_time",
    width: 180,
    render: (_value, record) => marketLocalTime(record.market_slug),
  },
  {
    title: "Realized PnL",
    dataIndex: "realized_pnl_usdc",
    key: "realized_pnl_usdc",
    render: (value) => formatUsd(value),
  },
  {
    title: "Taker Fee",
    dataIndex: "taker_fee_usdc",
    key: "taker_fee_usdc",
    render: (value) => formatUsd(value),
  },
  {
    title: "Maker Reward",
    dataIndex: "maker_reward_usdc",
    key: "maker_reward_usdc",
    render: (value) => formatUsd(value),
  },
  {
    title: "End Pos Up",
    dataIndex: "ending_position_up",
    key: "ending_position_up",
    render: (value) => Number(value || 0).toFixed(4),
  },
  {
    title: "End Pos Down",
    dataIndex: "ending_position_down",
    key: "ending_position_down",
    render: (value) => Number(value || 0).toFixed(4),
  },
];

export default function MarketTable({ markets }) {
  return (
    <Card className="market-card" title="Market Summary" bodyStyle={{ padding: 0 }}>
      <Table
        size="small"
        rowKey="market_slug"
        columns={columns}
        dataSource={markets}
        pagination={{ pageSize: 8 }}
        scroll={{ x: 1160 }}
      />
    </Card>
  );
}
