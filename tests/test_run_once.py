import asyncio
import argparse

from analysis_poly.models import AnalysisReport, AnalysisRequest, CurvePoint, MarketReport, SummaryStats, TokenReport
from analysis_poly.run_once import _request_from_args, _run_once


def _args(**kwargs):
    defaults = dict(
        address="0xabc",
        keywords="updown,15m",
        start_time="2026-03-01 00:00",
        end_time="2026-03-02 00:00",
        start_ts=None,
        end_ts=None,
        fee_rate_bps=1000.0,
        missing_cost_warn_qty=0.5,
        activity_window_sec=3600,
        concurrency=8,
        page_limit=1000,
        request_timeout_sec=20.0,
        output_dir="reports",
        indent=2,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_request_from_args_builds_analysis_request():
    req = _request_from_args(_args())

    assert req.address == "0xabc"
    assert req.keywords == ["15m", "updown"]
    assert req.activity_window_sec == 3600
    assert req.concurrency == 8


def test_request_from_args_uses_timestamps_when_text_missing():
    req = _request_from_args(
        _args(
            start_time=None,
            end_time=None,
            start_ts=1709251200,
            end_ts=1709337600,
            keywords="",
        )
    )

    assert req.start_ts == 1709251200
    assert req.end_ts == 1709337600
    assert req.keywords == []


def test_run_once_returns_completed_payload(monkeypatch, tmp_path):
    req = AnalysisRequest(
        address="0xabc",
        start_ts=100,
        end_ts=200,
        output_dir=str(tmp_path),
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
                tokens=[TokenReport(token_id="up", outcome="Up", trade_count=1, realized_pnl_usdc=1.2)],
            )
        ],
        maker_rebates=[],
        total_curve=[
            CurvePoint(timestamp=1000, delta_realized_pnl_usdc=1.2, cumulative_realized_pnl_usdc=1.2),
        ],
        market_curves={
            "btc-updown-5m-1000": [
                CurvePoint(timestamp=1000, delta_realized_pnl_usdc=1.2, cumulative_realized_pnl_usdc=1.2),
            ]
        },
        total_curve_no_fee=[
            CurvePoint(timestamp=1000, delta_realized_pnl_usdc=1.2, cumulative_realized_pnl_usdc=1.2),
        ],
        market_curves_no_fee={
            "btc-updown-5m-1000": [
                CurvePoint(timestamp=1000, delta_realized_pnl_usdc=1.2, cumulative_realized_pnl_usdc=1.2),
            ]
        },
        warnings=[],
    )

    class FakeAnalyzer:
        async def run(self, request_arg):
            assert request_arg == req
            return report

        def save_json(self, report_arg, path):
            assert report_arg == report
            return path

        def save_total_curve_csv(self, report_arg, path):
            assert report_arg == report
            return path

        def save_market_curve_csv(self, report_arg, path):
            assert report_arg == report
            return path

    monkeypatch.setattr("analysis_poly.run_once.PolymarketProfitAnalyzer", FakeAnalyzer)

    payload = asyncio.run(_run_once(req))

    assert payload["status"] == "COMPLETED"
    assert payload["result"]["summary"]["markets_processed"] == 1
    assert payload["result"]["artifacts"]["json"].endswith("_final.json")
