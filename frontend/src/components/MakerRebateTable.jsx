import { Card, Table } from "antd";
import dayjs from "dayjs";
import { formatUsd } from "../utils/format";

const columns = [
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

export default function MakerRebateTable({ makerRebates }) {
  return (
    <Card className="market-card" title="Daily Maker Rebate" bodyStyle={{ padding: 0 }}>
      <div className="market-section">
        <Table
          size="small"
          rowKey={(record) => `${record.timestamp}-${record.usdc_size}`}
          columns={columns}
          dataSource={makerRebates}
          pagination={{ pageSize: 10 }}
          locale={{ emptyText: "No maker rebate records" }}
          scroll={{ x: 360 }}
          sortDirections={["descend", "ascend"]}
        />
      </div>
    </Card>
  );
}
