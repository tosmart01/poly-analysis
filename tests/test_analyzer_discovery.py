from analysis_poly.models import DataApiPage, DataApiPagination
import asyncio

import pytest

from analysis_poly.activity_discovery import (
    dedupe_activity_records,
    iter_calendar_day_windows,
    iter_day_windows,
    iter_week_windows,
    summarize_discovered_markets,
)
from analysis_poly.analyzer import INCOME_ACTIVITY_TYPES, PolymarketProfitAnalyzer
from analysis_poly.models import ActivityRecord, AnalysisRequest, MarketReport, PolymarketMarket, TokenReport
from analysis_poly.profit_engine import PnlDelta


def test_run_discovers_markets_in_range_and_filters_keywords(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = []

        async def get_user_activity_page(
            self,
            user,
            activity_types=None,
            start_ts=None,
            end_ts=None,
            limit=500,
            cursor=None,
            sort_direction="ASC",
        ):
            activity_key = tuple(activity_types or [])
            self.calls.append((activity_key, start_ts, end_ts, cursor))
            pages = {
                (("TRADE",), 10, 7199): {
                    None: [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xa",
                                "timestamp": 100,
                                "type": "TRADE",
                                "conditionId": "cond_a",
                                "slug": "btc-updown-5m-100",
                            }
                        ),
                    ]
                },
                (("TRADE",), 86400, 86420): {
                    None: [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xb",
                                "timestamp": 86410,
                                "type": "TRADE",
                                "conditionId": "cond_b",
                                "slug": "eth-updown-15m-86400",
                            }
                        ),
                    ]
                },
            }
            return DataApiPage(data=pages.get((activity_key, start_ts, end_ts), {}).get(cursor, []), pagination=DataApiPagination(next_cursor=None))

        async def aclose(self):
            return

    async def fake_fetch_markets_with_status(_client, slugs, concurrency):
        markets = {
            "eth-updown-15m-86400": PolymarketMarket(
                slug="eth-updown-15m-86400",
                condition_id="cond_b",
                up_token_id="up_b",
                down_token_id="down_b",
                outcomes=["Up", "Down"],
                outcome_prices=[0.5, 0.5],
            )
        }
        return [(slug, markets.get(slug)) for slug in slugs]

    async def fake_process_single_market(client, engine, engine_no_fee, address, address_market_cache, req, market):
        report = MarketReport(
            market_slug=market.slug,
            condition_id=market.condition_id,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            realized_pnl_usdc=1.25,
            tokens=[TokenReport(token_id=market.up_token_id, outcome="Up", realized_pnl_usdc=1.25, trade_count=1)],
        )
        deltas = [PnlDelta(timestamp=86410, market_slug=market.slug, token_id=market.up_token_id, delta_pnl_usdc=1.25)]
        from analysis_poly.analyzer import _MarketProcessResult

        return _MarketProcessResult(
            market_slug=market.slug,
            market_report=report,
            market_report_no_fee=report,
            deltas=deltas,
            deltas_no_fee=deltas,
            warnings=[],
        )

    async def runner():
        fake_client = FakeClient()
        monkeypatch.setattr("analysis_poly.analyzer.PolymarketApiClient", lambda timeout_sec=20: fake_client)
        analyzer = PolymarketProfitAnalyzer()
        monkeypatch.setattr(analyzer, "_fetch_markets_with_status", fake_fetch_markets_with_status)
        monkeypatch.setattr(analyzer, "_process_single_market", fake_process_single_market)

        report = await analyzer.run(
            AnalysisRequest(
                address="0xabc",
                start_ts=10,
                end_ts=86420,
                keywords=["15m"],
                page_limit=1000,
                concurrency=2,
            )
        )

        expected_calls = [
            (("TRADE",), start, end, None)
            for start, end in iter_day_windows(10, 86420)
        ] + [
            (("SPLIT", "REDEEM"), start, end, None)
            for start, end in iter_calendar_day_windows(10, 86420)
        ] + [
            (INCOME_ACTIVITY_TYPES, start, end, None)
            for start, end in iter_week_windows(10, 86420)
        ]
        assert fake_client.calls == expected_calls
        assert report.summary.markets_total == 1
        assert report.summary.markets_processed == 1
        assert [market.market_slug for market in report.markets] == ["eth-updown-15m-86400"]

    asyncio.run(runner())


def test_iter_day_windows_uses_two_hour_windows():
    assert iter_day_windows(10, 7220) == [
        (10, 7199),
        (7200, 7220),
    ]


def test_iter_day_windows_accepts_custom_window_size():
    assert iter_day_windows(10, 7220, 3600) == [
        (10, 3599),
        (3600, 7199),
        (7200, 7220),
    ]


def test_iter_week_windows_groups_exact_week_in_one_window():
    assert iter_week_windows(10, 10 + 7 * 24 * 60 * 60) == [
        (10, 604810),
    ]


def test_run_filters_discovered_markets_by_slug_timestamp(monkeypatch):
    class FakeClient:
        async def get_user_activity_page(
            self,
            user,
            activity_types=None,
            start_ts=None,
            end_ts=None,
            limit=500,
            cursor=None,
            sort_direction="ASC",
        ):
            activity_key = tuple(activity_types or [])
            if activity_key == ("SPLIT", "REDEEM"):
                return DataApiPage(data=[
                    ActivityRecord.model_validate(
                        {
                            "transactionHash": "0xold_redeem",
                            "timestamp": 1776432712,
                            "type": "REDEEM",
                            "conditionId": "cond_old",
                            "slug": "btc-updown-15m-1767960900",
                            "size": 1,
                            "usdcSize": 1,
                        }
                    ),
                ], pagination=DataApiPagination(next_cursor=None))
            if activity_key == ("TRADE",):
                return DataApiPage(data=[
                    ActivityRecord.model_validate(
                        {
                            "transactionHash": "0xlive_trade",
                            "timestamp": 1776432800,
                            "type": "TRADE",
                            "conditionId": "cond_live",
                            "slug": "btc-updown-15m-1776432800",
                            "size": 1,
                            "usdcSize": 0.5,
                        }
                    ),
                ], pagination=DataApiPagination(next_cursor=None))
            return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))

        async def aclose(self):
            return

    fetched_slugs = []

    async def fake_fetch_markets_with_status(_client, slugs, concurrency):
        fetched_slugs.extend(slugs)
        market = PolymarketMarket(
            slug="btc-updown-15m-1776432800",
            condition_id="cond_live",
            up_token_id="up_live",
            down_token_id="down_live",
            outcomes=["Up", "Down"],
            outcome_prices=[0.5, 0.5],
        )
        return [(slug, market if slug == market.slug else None) for slug in slugs]

    async def fake_process_single_market(client, engine, engine_no_fee, address, address_market_cache, req, market):
        report = MarketReport(
            market_slug=market.slug,
            condition_id=market.condition_id,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            realized_pnl_usdc=1.0,
            tokens=[TokenReport(token_id=market.up_token_id, outcome="Up", realized_pnl_usdc=1.0, trade_count=1)],
        )
        delta = PnlDelta(timestamp=1776432800, market_slug=market.slug, token_id=market.up_token_id, delta_pnl_usdc=1.0)
        from analysis_poly.analyzer import _MarketProcessResult

        return _MarketProcessResult(
            market_slug=market.slug,
            market_report=report,
            market_report_no_fee=report,
            deltas=[delta],
            deltas_no_fee=[delta],
            warnings=[],
        )

    async def runner():
        monkeypatch.setattr("analysis_poly.analyzer.PolymarketApiClient", lambda timeout_sec=20: FakeClient())
        analyzer = PolymarketProfitAnalyzer()
        monkeypatch.setattr(analyzer, "_fetch_markets_with_status", fake_fetch_markets_with_status)
        monkeypatch.setattr(analyzer, "_process_single_market", fake_process_single_market)

        report = await analyzer.run(
            AnalysisRequest(
                address="0xabc",
                start_ts=1774972800,
                end_ts=1776679380,
                keywords=[],
                page_limit=1000,
                concurrency=5,
            )
        )

        assert fetched_slugs == ["btc-updown-15m-1776432800"]
        assert [market.market_slug for market in report.markets] == ["btc-updown-15m-1776432800"]

    asyncio.run(runner())


@pytest.mark.parametrize("income_type", INCOME_ACTIVITY_TYPES)
def test_run_adds_daily_maker_rebate_to_summary_and_total_curve(monkeypatch, income_type):
    class FakeClient:
        async def get_user_activity_page(
            self,
            user,
            activity_types=None,
            start_ts=None,
            end_ts=None,
            limit=500,
            cursor=None,
            sort_direction="ASC",
        ):
            activity_key = tuple(activity_types or [])
            if activity_key == ("TRADE",):
                if start_ts == 10 and cursor is None:
                    return DataApiPage(data=[
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xa",
                                "timestamp": 100,
                                "type": "TRADE",
                                "conditionId": "cond_a",
                                "slug": "eth-updown-15m-100",
                            }
                        )
                    ], pagination=DataApiPagination(next_cursor=None))
                return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
            if activity_key == INCOME_ACTIVITY_TYPES:
                if start_ts == 10 and cursor is None:
                    return DataApiPage(data=[
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xrebate",
                                "timestamp": 200,
                                "type": income_type,
                                "conditionId": "",
                                "slug": "",
                                "size": 23.3977,
                                "usdcSize": 23.3977,
                            }
                        )
                    ], pagination=DataApiPagination(next_cursor=None))
                return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
            return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))

        async def aclose(self):
            return

    async def fake_fetch_markets_with_status(_client, slugs, concurrency):
        market = PolymarketMarket(
            slug="eth-updown-15m-100",
            condition_id="cond_a",
            up_token_id="up_a",
            down_token_id="down_a",
            outcomes=["Up", "Down"],
            outcome_prices=[0.5, 0.5],
        )
        return [(slugs[0], market)]

    async def fake_process_single_market(client, engine, engine_no_fee, address, address_market_cache, req, market):
        report = MarketReport(
            market_slug=market.slug,
            condition_id=market.condition_id,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            realized_pnl_usdc=1.25,
            tokens=[TokenReport(token_id=market.up_token_id, outcome="Up", realized_pnl_usdc=1.25, trade_count=1)],
        )
        deltas = [PnlDelta(timestamp=100, market_slug=market.slug, token_id=market.up_token_id, delta_pnl_usdc=1.25)]
        from analysis_poly.analyzer import _MarketProcessResult

        return _MarketProcessResult(
            market_slug=market.slug,
            market_report=report,
            market_report_no_fee=report,
            deltas=deltas,
            deltas_no_fee=deltas,
            warnings=[],
        )

    async def runner():
        monkeypatch.setattr("analysis_poly.analyzer.PolymarketApiClient", lambda timeout_sec=20: FakeClient())
        analyzer = PolymarketProfitAnalyzer()
        monkeypatch.setattr(analyzer, "_fetch_markets_with_status", fake_fetch_markets_with_status)
        monkeypatch.setattr(analyzer, "_process_single_market", fake_process_single_market)

        report = await analyzer.run(
            AnalysisRequest(
                address="0xabc",
                start_ts=10,
                end_ts=300,
                keywords=["15m"],
                page_limit=100,
                concurrency=1,
            )
        )

        assert report.summary.total_maker_reward_usdc == 23.3977
        assert report.summary.total_realized_pnl_usdc == 24.6477
        assert [point.cumulative_realized_pnl_usdc for point in report.total_curve] == [1.25, 24.6477]
        assert [point.cumulative_realized_pnl_usdc for point in report.total_curve_no_fee] == [1.25, 24.6477]
        assert report.summary.total_taker_fee_usdc == 0
        assert report.markets[0].maker_reward_usdc == 0
        assert [item.model_dump() for item in report.maker_rebates] == [{"type": income_type, "transaction_hash": "0xrebate", "timestamp": 200, "usdc_size": 23.3977}]

    asyncio.run(runner())


def test_discovery_ignores_zero_value_redeem_activity():
    warnings = []
    records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0xzero",
                "timestamp": 1776432712,
                "type": "REDEEM",
                "conditionId": "cond_old",
                "slug": "btc-updown-15m-1767960900",
                "size": 0,
                "usdcSize": 0,
            }
        ),
        ActivityRecord.model_validate(
            {
                "transactionHash": "0xtrade",
                "timestamp": 1776432800,
                "type": "TRADE",
                "conditionId": "cond_live",
                "slug": "btc-updown-15m-1776432800",
                "size": 1,
                "usdcSize": 0.5,
            }
        ),
    ]

    discovered = summarize_discovered_markets(records, warnings)

    assert [item.slug for item in discovered] == ["btc-updown-15m-1776432800"]
    assert warnings == []


def test_dedupe_activity_records_ignores_float_field_differences():
    records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0xsame",
                "timestamp": 1776432800,
                "type": "TRADE",
                "conditionId": "cond_live",
                "slug": "btc-updown-15m-1776432800",
                "size": 1,
                "usdcSize": 0.5,
            }
        ),
        ActivityRecord.model_validate(
            {
                "transactionHash": "0xsame",
                "timestamp": 1776432801,
                "type": "TRADE",
                "conditionId": "cond_live",
                "slug": "btc-updown-15m-1776432800",
                "size": 0.999999999,
                "usdcSize": 0.500000001,
            }
        ),
    ]

    deduped = dedupe_activity_records(records)

    assert len(deduped) == 1


def test_income_only_run_preserves_distinct_types_in_same_transaction(monkeypatch):
    class FakeClient:
        async def get_user_activity_page(self, user, activity_types=None, **kwargs):
            if tuple(activity_types or []) != INCOME_ACTIVITY_TYPES:
                return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
            records = [ActivityRecord.model_validate({
                'transactionHash': '0xshared', 'timestamp': 200,
                'type': kind, 'conditionId': '', 'usdcSize': i + 1,
            }) for i, kind in enumerate(INCOME_ACTIVITY_TYPES)]
            return DataApiPage(data=records + records, pagination=DataApiPagination(next_cursor=None))  # Overlapping API pages must not double count.

        async def aclose(self):
            pass

    async def runner():
        monkeypatch.setattr('analysis_poly.analyzer.PolymarketApiClient', lambda **kwargs: FakeClient())
        report = await PolymarketProfitAnalyzer().run(AnalysisRequest(
            address='0xabc', start_ts=100, end_ts=300, keywords=['unrelated'],
        ))
        assert report.markets == []
        assert len(report.maker_rebates) == 5
        assert report.summary.total_realized_pnl_usdc == 15
        assert report.summary.total_maker_reward_usdc == 15
        assert report.summary.total_taker_fee_usdc == 0
        assert report.total_curve[-1].cumulative_realized_pnl_usdc == 15
        assert report.total_curve_no_fee[-1].cumulative_realized_pnl_usdc == 15

    asyncio.run(runner())
