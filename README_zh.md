# analysis-poly

**真实利润（去除手续费影响）**

![PnL](assets/pnl.png)
![Ratio](assets/ratio.png)

Polymarket 市场 PnL 分析 Web 工具。

## 适用范围（重要）

- 主面板会先按所选时间范围查询用户 activity，再回放命中的市场。
- 适用于 Polymarket 市场通用分析，不仅限于 crypto 市场。
- 主要目的：量化并可视化真实 PnL、手续费影响、市场级表现，以及 `Net PnL` 对比 `No-Fee PnL`。

## 环境要求

- Python `3.11+`（推荐使用 `3.11`）
- `uv` 包管理器仅本地开发时需要

## 快速开始

```bash
pip install analysis-poly
analysis-poly-server --host 127.0.0.1 --port 8000
```

打开 [http://localhost:8000](http://localhost:8000)。

## 从源码本地安装

```bash
uv pip install .
# 或
pip install .
```

随后使用同一条命令启动 Web 服务：

```bash
analysis-poly-server --host 127.0.0.1 --port 8000
```

如果是在源码目录中开发，也可以使用：

```bash
uv sync
uv run python main.py
```

## 命令行打开并自动启动

使用独立脚本启动服务、打开浏览器，并把参数放进 URL：

```bash
uv run -m analysis_poly.open_with_params \
  --address 0xabc \
  --keywords updown,15m \
  --start-time "2026-03-01 00:00" \
  --end-time "2026-03-02 00:00" \
  --concurrency 8
```

前端会读取 URL 参数，自动填充表单并直接启动。

## 首次拉取说明

仓库已提交 `analysis_poly/static/dist` 构建产物，首次启动不需要先构建前端。

如果你修改了 `frontend/src`，需要重新构建：

```bash
npm install
npm run build
```

## API

- `POST /api/runs`
- `GET /api/runs/{run_id}/stream`（SSE）
- `POST /api/runs/{run_id}/stop`
- `GET /api/runs/{run_id}/result`
- `GET /api/runs/{run_id}/state`

## 测试

```bash
uv run pytest
```

## 前端

- 源码：`frontend/src`
- 构建产物：`analysis_poly/static/dist/app.js`、`analysis_poly/static/dist/app.css`
