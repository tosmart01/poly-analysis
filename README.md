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

## Run

```bash
uv sync
uv run python main.py
```

Open [http://localhost:8000](http://localhost:8000).

## First Clone

`static/dist` is committed in the repository, so first startup does not require front-end build.

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
- Build output: `static/dist/app.js` and `static/dist/app.css`
