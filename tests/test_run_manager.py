import asyncio

import pytest
from fastapi import HTTPException

from analysis_poly.models import (
    AnalysisReport,
    AnalysisRequest,
    CurvePoint,
    MarketReport,
    RunState,
    RunStatus,
    SummaryStats,
    TokenReport,
)
from analysis_poly.run_manager import RunContext, RunManager


def test_single_run_lock_conflict(monkeypatch):
    async def runner():
        manager = RunManager()

        async def fake_run(req, stop_event=None, hooks=None):
            await asyncio.sleep(0.2)
            return AnalysisReport(
                request=req,
                summary=SummaryStats(markets_total=1, markets_processed=1),
                markets=[],
                total_curve=[],
                market_curves={},
                warnings=[],
                is_partial=False,
            )

        monkeypatch.setattr(manager._analyzer, "run", fake_run)

        req = AnalysisRequest(
            address="0xe00740bce98a594e26861838885ab310ec3b548c",
            start_ts=100,
            end_ts=200,
        )

        await manager.create_run(req)
        with pytest.raises(HTTPException) as exc:
            await manager.create_run(req)
        assert exc.value.status_code == 409

    asyncio.run(runner())


def test_stop_run_updates_status(monkeypatch):
    async def runner():
        manager = RunManager()

        async def fake_run(req, stop_event=None, hooks=None):
            while not stop_event.is_set():
                await asyncio.sleep(0.05)
            return AnalysisReport(
                request=req,
                summary=SummaryStats(markets_total=1, markets_processed=0),
                markets=[],
                total_curve=[],
                market_curves={},
                warnings=[],
                is_partial=True,
            )

        monkeypatch.setattr(manager._analyzer, "run", fake_run)

        req = AnalysisRequest(
            address="0xe00740bce98a594e26861838885ab310ec3b548c",
            start_ts=100,
            end_ts=200,
        )

        created = await manager.create_run(req)
        ack = await manager.stop_run(created.run_id)
        assert ack.status.name == "STOPPING"

    asyncio.run(runner())


def test_finalizing_state_is_observable_and_blocks_new_runs(monkeypatch):
    async def runner():
        manager = RunManager()
        finalizing_seen = asyncio.Event()
        finish = asyncio.Event()

        async def fake_run(req, stop_event=None, hooks=None):
            await hooks.on_run_started(1)
            await hooks.on_progress(1, 1, "btc-updown-15m-1000")
            await hooks.on_phase("FINALIZING", "Collecting maker rebates")
            finalizing_seen.set()
            await finish.wait()
            return AnalysisReport(
                request=req,
                summary=SummaryStats(markets_total=1, markets_processed=1),
                markets=[],
                total_curve=[],
                market_curves={},
                warnings=[],
                is_partial=False,
            )

        monkeypatch.setattr(manager._analyzer, "run", fake_run)

        req = AnalysisRequest(
            address="0xe00740bce98a594e26861838885ab310ec3b548c",
            start_ts=100,
            end_ts=200,
        )

        created = await manager.create_run(req)
        await asyncio.wait_for(finalizing_seen.wait(), timeout=1)

        state = await manager.get_state(created.run_id)
        assert state.status == RunStatus.FINALIZING
        assert state.progress_current == 1
        assert state.progress_total == 1
        assert state.message == "Collecting maker rebates"

        with pytest.raises(HTTPException) as exc:
            await manager.create_run(req)
        assert exc.value.status_code == 409

        finish.set()
        await manager._runs[created.run_id].task

    asyncio.run(runner())


def test_get_result_returns_compact_payload():
    async def runner():
        manager = RunManager()
        req = AnalysisRequest(
            address="0xe00740bce98a594e26861838885ab310ec3b548c",
            start_ts=100,
            end_ts=200,
            keywords=["updown"],
        )
        report = AnalysisReport(
            request=req,
            summary=SummaryStats(total_realized_pnl_usdc=1.2, markets_total=1, markets_processed=1),
            markets=[
                MarketReport(
                    market_slug="btc-updown-5m-1000",
                    condition_id="cond",
                    up_token_id="up",
                    down_token_id="down",
                    realized_pnl_usdc=1.2,
                    tokens=[
                        TokenReport(
                            token_id="up",
                            outcome="Up",
                            entry_amount_usdc=4.2,
                            avg_entry_price=0.42,
                            buy_qty=10,
                            realized_pnl_usdc=1.2,
                            trade_count=2,
                        )
                    ],
                )
            ],
            total_curve=[
                CurvePoint(timestamp=1000, delta_realized_pnl_usdc=0.5, cumulative_realized_pnl_usdc=0.5),
                CurvePoint(timestamp=1010, delta_realized_pnl_usdc=0.7, cumulative_realized_pnl_usdc=1.2),
            ],
            market_curves={
                "btc-updown-5m-1000": [
                    CurvePoint(timestamp=1000, delta_realized_pnl_usdc=0.5, cumulative_realized_pnl_usdc=0.5),
                    CurvePoint(timestamp=1010, delta_realized_pnl_usdc=0.7, cumulative_realized_pnl_usdc=1.2),
                ]
            },
            warnings=[],
            is_partial=False,
            artifacts={"json": "/tmp/report.json"},
        )

        run_id = "run_compact"
        manager._runs[run_id] = RunContext(
            state=RunState(run_id=run_id, status=RunStatus.COMPLETED),
            result=report,
        )

        payload = await manager.get_result(run_id)

        assert "market_curves" not in payload
        assert payload["total_series"] == [{"ts": 1000, "value": 0.5}, {"ts": 1010, "value": 1.2}]
        assert payload["symbol_series"]["btc-5"] == [{"ts": 1000, "value": 0.5}, {"ts": 1010, "value": 1.2}]
        assert payload["markets"][0]["tokens"][0]["entry_amount_usdc"] == 4.2
        assert payload["markets"][0]["tokens"][0]["avg_entry_price"] == 0.42
        assert payload["artifacts"]["json"] == "/reports/report.json"

    asyncio.run(runner())
