import csv
from datetime import datetime

from analysis_poly.analyzer import PolymarketProfitAnalyzer
from analysis_poly.models import AnalysisReport, AnalysisRequest, MarketReport, SummaryStats, TokenReport


def test_market_csv_matches_market_table_fields(tmp_path):
    analyzer = PolymarketProfitAnalyzer()
    req = AnalysisRequest(
        address="0xabc",
        start_ts=100,
        end_ts=200,
        output_dir=str(tmp_path),
    )
    report = AnalysisReport(
        request=req,
        summary=SummaryStats(markets_total=1, markets_processed=1),
        markets=[
            MarketReport(
                market_slug="btc-updown-5m-1000",
                condition_id="cond",
                up_token_id="up",
                down_token_id="down",
                realized_pnl_usdc=1.2,
                taker_fee_usdc=0.1,
                maker_reward_usdc=0.2,
                tokens=[
                    TokenReport(
                        token_id="up",
                        outcome="Up",
                        last_trade_timestamp=1010,
                        entry_amount_usdc=4.2,
                        buy_amount_usdc=4.2,
                        buy_avg_price=0.42,
                        buy_qty=10,
                        sell_amount_usdc=2.8,
                        sell_avg_price=0.56,
                        sell_qty=5,
                        realized_pnl_usdc=1.2,
                        taker_fee_usdc=0.1,
                        maker_reward_usdc=0.2,
                        trade_count=2,
                    ),
                    TokenReport(
                        token_id="down",
                        outcome="Down",
                        last_trade_timestamp=1020,
                        entry_amount_usdc=1.8,
                        buy_amount_usdc=1.8,
                        buy_avg_price=0.36,
                        buy_qty=5,
                        sell_amount_usdc=2.1,
                        sell_avg_price=0.42,
                        sell_qty=5,
                        realized_pnl_usdc=0.4,
                        taker_fee_usdc=0.02,
                        trade_count=2,
                    ),
                ],
            )
        ],
        total_curve=[],
        market_curves={},
        warnings=[],
    )

    csv_path = analyzer.save_market_curve_csv(report, str(tmp_path / "market.csv"))

    with open(csv_path, newline="", encoding="utf-8") as fp:
        rows = list(csv.reader(fp))

    assert rows == [
        [
            "Market",
            "Market Time",
            "Trade Time",
            "Realized PnL",
            "Taker Fee",
            "Maker Reward",
            "Outcome",
            "Buy Amt",
            "Sell Amt",
            "Buy Avg Price",
            "Sell Avg Price",
        ],
        [
            "btc-updown-5m-1000",
            datetime.fromtimestamp(1000).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.fromtimestamp(1010).strftime("%Y-%m-%d %H:%M:%S"),
            "1.2",
            "0.1",
            "0.2",
            "Up",
            "4.2",
            "2.8",
            "0.42",
            "0.56",
        ],
        [
            "btc-updown-5m-1000",
            datetime.fromtimestamp(1000).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.fromtimestamp(1020).strftime("%Y-%m-%d %H:%M:%S"),
            "0.4",
            "0.02",
            "0",
            "Down",
            "1.8",
            "2.1",
            "0.36",
            "0.42",
        ],
    ]
