# analysis-poly

**Real Profit (Fee-Excluded View)**

![PnL](assets/pnl.png)
![Ratio](assets/ratio.png)

中文说明见：[README_zh.md](README_zh.md)

Polymarket real-profit analyzer with a web UI.

## Scope (Important)

- This tool is primarily designed for Polymarket crypto `updown` markets with taker fees, especially `5m` and `15m` intervals.
- It is **not** intended as a universal PnL engine for all Polymarket market types.
- Main purpose: quantify and visualize the impact of trading fees on real profitability (`Net PnL` vs `No-Fee PnL`).

## Requirements

- Python `3.11+` (recommended: `3.11`)
- `uv` package manager

## Install As Package

```bash
uv pip install .
# or
pip install .
```

After install:

```bash
analysis-poly
analysis-poly-open --address 0xabc --symbols btc,eth --intervals 5,15
```

## Run

```bash
uv sync
uv run python main.py
```

Open [http://localhost:8000](http://localhost:8000).

## CLI Open + Auto Start

Use a standalone script to start server, open browser, and pass params in URL.

```bash
uv run -m app.open_with_params \
  --address 0xabc \
  --symbols btc,eth \
  --intervals 5,15 \
  --start-time "2026-03-01 00:00" \
  --end-time "2026-03-02 00:00" \
  --concurrency 8
```

Frontend will read query params, fill form fields, and auto start the run.

## First Clone

`app/static/dist` is committed in the repository, so first startup does not require front-end build.

If you modify `frontend/src`, rebuild assets:

```bash
npm install
npm run build
```

## API

- `POST /api/runs`
- `GET /api/runs/{run_id}/stream` (SSE)
- `POST /api/runs/{run_id}/stop`
- `GET /api/runs/{run_id}/result`
- `GET /api/runs/{run_id}/state`

## Test

```bash
uv run pytest
```

## Frontend

- Source: `frontend/src`
- Build output: `app/static/dist/app.js` and `app/static/dist/app.css`
