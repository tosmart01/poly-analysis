from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import HTTPException
from loguru import logger

from .analyzer import AnalyzerHooks, PolymarketProfitAnalyzer
from .models import (
    AnalysisReport,
    AnalysisRequest,
    CurvePoint,
    MarketReport,
    RunCreated,
    RunState,
    RunStatus,
    RunStopAck,
    TokenReport,
    WarningItem,
    utc_now,
)

RESULT_MAX_CURVE_POINTS = 2000
RESULT_MAX_DRAWDOWN_MARKERS = 8
RESULT_MIN_DRAWDOWN_DELTA_USDC = 0.5


@dataclass
class RunContext:
    state: RunState
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    queue: asyncio.Queue[str] = field(default_factory=asyncio.Queue)
    result: AnalysisReport | None = None
    task: asyncio.Task | None = None


class RunHooks(AnalyzerHooks):
    def __init__(self, manager: "RunManager", run_id: str):
        self._manager = manager
        self._run_id = run_id

    async def on_run_started(self, total_markets: int) -> None:
        ctx = self._manager._runs[self._run_id]
        ctx.state.progress_total = total_markets
        ctx.state.message = "Processing markets"
        await self._manager._emit(
            self._run_id,
            "run_started",
            {"run_id": self._run_id, "progress_total": total_markets, "message": ctx.state.message},
        )

    async def on_phase(self, status: str, message: str) -> None:
        ctx = self._manager._runs[self._run_id]
        try:
            ctx.state.status = RunStatus(status)
        except ValueError:
            logger.warning("ignore unknown run phase status run_id={} status={}", self._run_id, status)
        ctx.state.message = message
        await self._manager._emit(
            self._run_id,
            "status",
            {
                "run_id": self._run_id,
                "status": ctx.state.status,
                "message": message,
                "progress_current": ctx.state.progress_current,
                "progress_total": ctx.state.progress_total,
            },
        )

    async def on_progress(self, current: int, total: int, market_slug: str) -> None:
        ctx = self._manager._runs[self._run_id]
        ctx.state.progress_current = current
        ctx.state.progress_total = total
        ctx.state.message = market_slug
        await self._manager._emit(
            self._run_id,
            "progress",
            {
                "current": current,
                "total": total,
                "market_slug": market_slug,
            },
        )

    async def on_warning(self, warning: WarningItem) -> None:
        logger.warning(
            "warning run_id={} code={} market={} token={} msg={}",
            self._run_id,
            warning.code,
            warning.market_slug or "-",
            warning.token_id or "-",
            warning.message,
        )
        await self._manager._emit(self._run_id, "warning", warning.model_dump())

    async def on_total_point(self, timestamp: int, delta: float, cumulative: float) -> None:
        await self._manager._emit(
            self._run_id,
            "point_total",
            {
                "timestamp": timestamp,
                "delta_realized_pnl_usdc": delta,
                "cumulative_realized_pnl_usdc": cumulative,
            },
        )

    async def on_market_point(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None:
        await self._manager._emit(
            self._run_id,
            "point_market",
            {
                "market_slug": market_slug,
                "timestamp": timestamp,
                "delta_realized_pnl_usdc": delta,
                "cumulative_realized_pnl_usdc": cumulative,
            },
        )

    async def on_total_point_no_fee(self, timestamp: int, delta: float, cumulative: float) -> None:
        await self._manager._emit(
            self._run_id,
            "point_total_no_fee",
            {
                "timestamp": timestamp,
                "delta_realized_pnl_usdc": delta,
                "cumulative_realized_pnl_usdc": cumulative,
            },
        )

    async def on_market_point_no_fee(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None:
        await self._manager._emit(
            self._run_id,
            "point_market_no_fee",
            {
                "market_slug": market_slug,
                "timestamp": timestamp,
                "delta_realized_pnl_usdc": delta,
                "cumulative_realized_pnl_usdc": cumulative,
            },
        )


class RunManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._runs: dict[str, RunContext] = {}
        self._active_run_id: str | None = None
        self._analyzer = PolymarketProfitAnalyzer()

    async def create_run(self, req: AnalysisRequest) -> RunCreated:
        async with self._lock:
            if self._active_run_id:
                active = self._runs.get(self._active_run_id)
                if active and active.state.status in {
                    RunStatus.PENDING,
                    RunStatus.RUNNING,
                    RunStatus.FINALIZING,
                    RunStatus.STOPPING,
                }:
                    raise HTTPException(status_code=409, detail="another run is in progress")

            run_id = uuid.uuid4().hex
            state = RunState(run_id=run_id, status=RunStatus.PENDING)
            ctx = RunContext(state=state)
            self._runs[run_id] = ctx
            self._active_run_id = run_id

            ctx.task = asyncio.create_task(self._execute_run(run_id, req))
            logger.info(
                "create run run_id={} address={} range=[{}, {}] keywords={}",
                run_id,
                req.address,
                req.start_ts,
                req.end_ts,
                ",".join(req.keywords),
            )
            return RunCreated(run_id=run_id, status=state.status)

    async def stop_run(self, run_id: str) -> RunStopAck:
        ctx = self._runs.get(run_id)
        if not ctx:
            raise HTTPException(status_code=404, detail="run not found")

        if ctx.state.status in {RunStatus.COMPLETED, RunStatus.STOPPED, RunStatus.FAILED}:
            return RunStopAck(run_id=run_id, status=ctx.state.status)

        ctx.stop_event.set()
        ctx.state.status = RunStatus.STOPPING
        logger.warning("stop requested run_id={}", run_id)
        await self._emit(run_id, "progress", {"message": "stopping requested"})
        return RunStopAck(run_id=run_id, status=ctx.state.status)

    async def get_result(self, run_id: str) -> dict:
        ctx = self._runs.get(run_id)
        if not ctx:
            raise HTTPException(status_code=404, detail="run not found")

        if not ctx.result:
            raise HTTPException(status_code=202, detail="run not finished")
        started = time.perf_counter()
        payload = _compact_analysis_report(ctx.result, self._to_public_artifact_paths(ctx.result.artifacts))
        logger.info(
            "result compact complete run_id={} markets={} total_series={} symbol_series={} elapsed_sec={:.3f}",
            run_id,
            len(payload.get("markets", [])),
            len(payload.get("total_series", [])),
            len(payload.get("symbol_series", {})),
            time.perf_counter() - started,
        )
        return payload

    async def get_state(self, run_id: str) -> RunState:
        ctx = self._runs.get(run_id)
        if not ctx:
            raise HTTPException(status_code=404, detail="run not found")
        return ctx.state

    async def stream(self, run_id: str):
        ctx = self._runs.get(run_id)
        if not ctx:
            raise HTTPException(status_code=404, detail="run not found")

        while True:
            if ctx.task and ctx.task.done() and ctx.queue.empty():
                break

            try:
                payload = await asyncio.wait_for(ctx.queue.get(), timeout=10)
                yield payload
            except asyncio.TimeoutError:
                yield ": keep-alive\n\n"

    async def _execute_run(self, run_id: str, req: AnalysisRequest) -> None:
        ctx = self._runs[run_id]
        hooks = RunHooks(self, run_id)

        try:
            ctx.state.status = RunStatus.RUNNING
            ctx.state.started_at = utc_now()
            logger.info("run started run_id={}", run_id)

            report = await self._analyzer.run(req=req, stop_event=ctx.stop_event, hooks=hooks)

            output_dir = Path(req.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            suffix = f"{run_id}_{'partial' if report.is_partial else 'final'}"

            ctx.state.status = RunStatus.FINALIZING
            ctx.state.message = "Saving report artifacts"
            await self._emit(
                run_id,
                "status",
                {
                    "run_id": run_id,
                    "status": ctx.state.status,
                    "message": ctx.state.message,
                    "progress_current": ctx.state.progress_current,
                    "progress_total": ctx.state.progress_total,
                },
            )

            save_started = time.perf_counter()
            json_path = self._analyzer.save_json(report, str(output_dir / f"pnl_summary_{suffix}.json"))
            logger.info("artifact save complete run_id={} kind=json elapsed_sec={:.3f}", run_id, time.perf_counter() - save_started)
            save_started = time.perf_counter()
            total_csv_path = self._analyzer.save_total_curve_csv(
                report, str(output_dir / f"pnl_total_curve_{suffix}.csv")
            )
            logger.info(
                "artifact save complete run_id={} kind=total_curve_csv elapsed_sec={:.3f}",
                run_id,
                time.perf_counter() - save_started,
            )
            save_started = time.perf_counter()
            market_csv_path = self._analyzer.save_market_curve_csv(
                report, str(output_dir / f"pnl_market_curve_{suffix}.csv")
            )
            logger.info(
                "artifact save complete run_id={} kind=market_curve_csv elapsed_sec={:.3f}",
                run_id,
                time.perf_counter() - save_started,
            )

            report.artifacts = {
                "json": json_path,
                "total_curve_csv": total_csv_path,
                "market_curve_csv": market_csv_path,
            }

            ctx.result = report
            ctx.state.ended_at = utc_now()

            if report.is_partial:
                ctx.state.status = RunStatus.STOPPED
                ctx.state.message = "Stopped"
                logger.warning("run stopped run_id={} (partial result saved)", run_id)
                await self._emit(
                    run_id,
                    "stopped",
                    {
                        "run_id": run_id,
                        "status": ctx.state.status,
                        "artifacts": self._to_public_artifact_paths(report.artifacts),
                    },
                )
            else:
                ctx.state.status = RunStatus.COMPLETED
                ctx.state.message = "Completed"
                logger.info("run completed run_id={} markets_processed={}", run_id, report.summary.markets_processed)
                await self._emit(
                    run_id,
                    "completed",
                    {
                        "run_id": run_id,
                        "status": ctx.state.status,
                        "artifacts": self._to_public_artifact_paths(report.artifacts),
                        "summary": report.summary.model_dump(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            ctx.state.status = RunStatus.FAILED
            ctx.state.ended_at = utc_now()
            logger.exception("run failed run_id={} error={}", run_id, exc)
            await self._emit(run_id, "run_error", {"message": str(exc)})
        finally:
            async with self._lock:
                if self._active_run_id == run_id:
                    self._active_run_id = None

    async def _emit(self, run_id: str, event_name: str, data: dict) -> None:
        ctx = self._runs.get(run_id)
        if not ctx:
            return
        payload = self._format_sse(event_name, data)
        await ctx.queue.put(payload)

    @staticmethod
    def _format_sse(event_name: str, data: dict) -> str:
        return f"event: {event_name}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"

    @staticmethod
    def _to_public_artifact_paths(artifacts: dict[str, str]) -> dict[str, str]:
        public: dict[str, str] = {}
        for key, path in artifacts.items():
            filename = Path(path).name
            public[key] = f"/reports/{filename}"
        return public


def _compact_analysis_report(report: AnalysisReport, public_artifacts: dict[str, str]) -> dict:
    symbol_curves = _aggregate_symbol_curves(report.market_curves)
    symbol_curves_no_fee = _aggregate_symbol_curves(report.market_curves_no_fee)
    return {
        "summary": report.summary.model_dump(),
        "markets": [_compact_market_report(market) for market in report.markets],
        "maker_rebates": [item.model_dump() for item in report.maker_rebates],
        "total_series": _curve_points_to_series(report.total_curve),
        "symbol_series": {key: _curve_points_to_series(points) for key, points in symbol_curves.items()},
        "total_series_no_fee": _curve_points_to_series(report.total_curve_no_fee),
        "symbol_series_no_fee": {
            key: _curve_points_to_series(points) for key, points in symbol_curves_no_fee.items()
        },
        "drawdown_markers": _build_drawdown_markers(report.market_curves),
        "warnings": [warning.model_dump() for warning in report.warnings[-200:]],
        "artifacts": public_artifacts,
        "is_partial": report.is_partial,
    }


def _compact_market_report(market: MarketReport) -> dict:
    return {
        "market_slug": market.market_slug,
        "realized_pnl_usdc": market.realized_pnl_usdc,
        "taker_fee_usdc": market.taker_fee_usdc,
        "maker_reward_usdc": market.maker_reward_usdc,
        "ending_position_up": market.ending_position_up,
        "ending_position_down": market.ending_position_down,
        "tokens": [_compact_token_report(token) for token in market.tokens],
    }


def _compact_token_report(token: TokenReport) -> dict:
    return {
        "token_id": token.token_id,
        "outcome": token.outcome,
        "entry_amount_usdc": token.entry_amount_usdc,
        "avg_entry_price": token.avg_entry_price,
        "realized_pnl_usdc": token.realized_pnl_usdc,
        "buy_qty": token.buy_qty,
        "sell_qty": token.sell_qty,
        "redeem_qty": token.redeem_qty,
        "ending_position_qty": token.ending_position_qty,
        "trade_count": token.trade_count,
    }


def _curve_points_to_series(points: list[CurvePoint]) -> list[dict]:
    sampled = _downsample_curve_points(points, RESULT_MAX_CURVE_POINTS)
    return [
        {
            "ts": point.timestamp,
            "value": point.cumulative_realized_pnl_usdc,
        }
        for point in sampled
    ]


def _downsample_curve_points(points: list[CurvePoint], max_points: int) -> list[CurvePoint]:
    if len(points) <= max_points:
        return points
    step = max(1, len(points) // max_points)
    sampled = [points[idx] for idx in range(0, len(points), step)]
    if sampled[-1] is not points[-1]:
        sampled.append(points[-1])
    return sampled


def _aggregate_symbol_curves(market_curves: dict[str, list[CurvePoint]]) -> dict[str, list[CurvePoint]]:
    symbol_by_ts: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for market_slug, points in market_curves.items():
        market_prefix = _extract_market_prefix(market_slug)
        for point in points:
            symbol_by_ts[market_prefix][point.timestamp] += point.delta_realized_pnl_usdc

    symbol_curves: dict[str, list[CurvePoint]] = {}
    for market_prefix, by_ts in symbol_by_ts.items():
        cumulative = 0.0
        symbol_curves[market_prefix] = []
        for ts in sorted(by_ts.keys()):
            delta = by_ts[ts]
            cumulative += delta
            symbol_curves[market_prefix].append(
                CurvePoint(
                    timestamp=ts,
                    delta_realized_pnl_usdc=round(delta, 10),
                    cumulative_realized_pnl_usdc=round(cumulative, 10),
                )
            )
    return symbol_curves


def _build_drawdown_markers(market_curves: dict[str, list[CurvePoint]]) -> list[dict]:
    markers: list[dict] = []
    for market_slug, points in market_curves.items():
        worst_point: CurvePoint | None = None
        for point in points:
            if point.delta_realized_pnl_usdc >= -RESULT_MIN_DRAWDOWN_DELTA_USDC:
                continue
            if worst_point is None or point.delta_realized_pnl_usdc < worst_point.delta_realized_pnl_usdc:
                worst_point = point
        if worst_point is None:
            continue
        markers.append(
            {
                "ts": worst_point.timestamp,
                "delta": worst_point.delta_realized_pnl_usdc,
                "marketSlug": market_slug,
                "marketPrefix": _extract_market_prefix(market_slug),
            }
        )
    markers.sort(key=lambda item: item["delta"])
    return markers[:RESULT_MAX_DRAWDOWN_MARKERS]


def _extract_market_prefix(market_slug: str) -> str:
    raw = str(market_slug or "").strip().lower()
    if not raw:
        return "unknown"
    parts = [part for part in raw.split("-") if part]
    if len(parts) >= 3:
        interval = parts[2][:-1] if parts[2].endswith("m") else parts[2]
        if parts[1] == "updown":
            return f"{parts[0]}-{interval}"
        return f"{parts[0]}-{parts[1]}-{interval}"
    return parts[0] if parts else "unknown"
