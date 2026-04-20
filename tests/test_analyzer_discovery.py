import asyncio

from analysis_poly.analyzer import PolymarketProfitAnalyzer
from analysis_poly.models import ActivityRecord, AnalysisRequest, MarketReport, PolymarketMarket, TokenReport
from analysis_poly.profit_engine import PnlDelta


def test_run_discovers_daily_markets_and_filters_keywords(monkeypatch):
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
            offset=0,
            sort_direction="ASC",
        ):
            activity_key = tuple(activity_types or [])
            self.calls.append((activity_key, start_ts, end_ts, offset))
            pages = {
                (("TRADE", "SPLIT", "REDEEM"), 10, 86399): {
                    0: [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xa",
                                "timestamp": 100,
                                "type": "TRADE",
                                "conditionId": "cond_a",
                                "slug": "btc-updown-5m-100",
                            }
                        )
                    ]
                },
                (("TRADE", "SPLIT", "REDEEM"), 86400, 86420): {
                    0: [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xb",
                                "timestamp": 86410,
                                "type": "TRADE",
                                "conditionId": "cond_b",
                                "slug": "eth-updown-15m-86400",
                            }
                        )
                    ]
                },
                (("MAKER_REBATE",), 10, 86399): {0: []},
                (("MAKER_REBATE",), 86400, 86420): {0: []},
            }
            return pages.get((activity_key, start_ts, end_ts), {}).get(offset, [])

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
                address="0xe00740bce98a594e26861838885ab310ec3b548c",
                start_ts=10,
                end_ts=86420,
                keywords=["15m"],
                page_limit=1000,
                concurrency=2,
            )
        )

        assert fake_client.calls == [
            (("TRADE", "SPLIT", "REDEEM"), 10, 86399, 0),
            (("TRADE", "SPLIT", "REDEEM"), 86400, 86420, 0),
            (("MAKER_REBATE",), 10, 86399, 0),
            (("MAKER_REBATE",), 86400, 86420, 0),
        ]
        assert report.summary.markets_total == 1
        assert report.summary.markets_processed == 1
        assert [market.market_slug for market in report.markets] == ["eth-updown-15m-86400"]

    asyncio.run(runner())


def test_run_adds_daily_maker_rebate_to_summary_and_total_curve(monkeypatch):
    class FakeClient:
        async def get_user_activity_page(
            self,
            user,
            activity_types=None,
            start_ts=None,
            end_ts=None,
            limit=500,
            offset=0,
            sort_direction="ASC",
        ):
            activity_key = tuple(activity_types or [])
            if activity_key == ("TRADE", "SPLIT", "REDEEM"):
                if start_ts == 10 and offset == 0:
                    return [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xa",
                                "timestamp": 100,
                                "type": "TRADE",
                                "conditionId": "cond_a",
                                "slug": "eth-updown-15m-100",
                            }
                        )
                    ]
                return []
            if activity_key == ("MAKER_REBATE",):
                if start_ts == 10 and offset == 0:
                    return [
                        ActivityRecord.model_validate(
                            {
                                "transactionHash": "0xrebate",
                                "timestamp": 200,
                                "type": "MAKER_REBATE",
                                "conditionId": "",
                                "slug": "",
                                "size": 23.3977,
                                "usdcSize": 23.3977,
                            }
                        )
                    ]
                return []
            return []

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
                address="0xe00740bce98a594e26861838885ab310ec3b548c",
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
        assert report.markets[0].maker_reward_usdc == 0
        assert [item.model_dump() for item in report.maker_rebates] == [{"timestamp": 200, "usdc_size": 23.3977}]

    asyncio.run(runner())
