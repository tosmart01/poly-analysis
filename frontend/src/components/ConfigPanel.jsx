import { Button, Col, Dropdown, Form, Input, Row } from "antd";
import { DownloadOutlined } from "@ant-design/icons";

export default function ConfigPanel({
  formData,
  updateField,
  onQuickRange,
  downloads,
  onOpenAdvanced,
  onToggleRun,
  onReset,
  running,
  runStatus,
}) {
  const exportItems = [
    {
      key: "json",
      disabled: !downloads,
      label: downloads ? (
        <a href={downloads.json} target="_blank" rel="noreferrer">
          JSON
        </a>
      ) : (
        "JSON"
      ),
    },
    {
      key: "total",
      disabled: !downloads,
      label: downloads ? (
        <a href={downloads.total_curve_csv} target="_blank" rel="noreferrer">
          Total CSV
        </a>
      ) : (
        "Total CSV"
      ),
    },
    {
      key: "market",
      disabled: !downloads,
      label: downloads ? (
        <a href={downloads.market_curve_csv} target="_blank" rel="noreferrer">
          Market CSV
        </a>
      ) : (
        "Market CSV"
      ),
    },
  ];

  return (
    <section className="config-card">
      <Form layout="vertical" requiredMark={false} className="config-form">
        <Row gutter={[12, 4]} align="bottom">
          <Col xs={24} lg={13}>
            <Form.Item label="Address">
              <Input value={formData.address} onChange={(event) => updateField("address", event.target.value)} />
            </Form.Item>
          </Col>
          <Col xs={18} lg={8}>
            <Form.Item label="Keywords">
              <Input
                value={formData.keywords}
                onChange={(event) => updateField("keywords", event.target.value)}
                placeholder="updown,15m"
              />
            </Form.Item>
          </Col>
          <Col xs={6} lg={3}>
            <Form.Item label=" ">
              <Dropdown menu={{ items: exportItems }} trigger={["click"]}>
                <Button icon={<DownloadOutlined />} className="export-btn">
                  Export
                </Button>
              </Dropdown>
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={[12, 4]} align="bottom">
          <Col xs={24} lg={17}>
            <Form.Item label="Time Window">
              <div className="time-line">
                <div className="time-row">
                  <Input value={formData.startTime} onChange={(event) => updateField("startTime", event.target.value)} placeholder="YYYY-MM-DD HH:MM" />
                  <span className="time-separator">--</span>
                  <Input value={formData.endTime} onChange={(event) => updateField("endTime", event.target.value)} placeholder="YYYY-MM-DD HH:MM" />
                </div>
                <div className="quick-range-row">
                  <Button size="small" onClick={() => onQuickRange(3)}>
                    Last 3D
                  </Button>
                  <Button size="small" onClick={() => onQuickRange(7)}>
                    1W
                  </Button>
                  <Button size="small" onClick={() => onQuickRange(30)}>
                    1M
                  </Button>
                </div>
              </div>
            </Form.Item>
          </Col>

          <Col xs={24} lg={7}>
            <div className="action-row">
              <Button onClick={onOpenAdvanced}>Advanced</Button>
              <Button
                type="primary"
                onClick={onToggleRun}
                disabled={runStatus === "STOPPING"}
                danger={running}
                className="session-btn"
              >
                {running ? "Stop Session" : "Start Session"}
              </Button>
              <Button onClick={onReset}>Reset</Button>
            </div>
          </Col>
        </Row>
      </Form>
    </section>
  );
}
