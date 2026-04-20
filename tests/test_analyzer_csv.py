import csv
from datetime import datetime

from analysis_poly.analyzer import PolymarketProfitAnalyzer
from analysis_poly.models import AnalysisReport, AnalysisRequest, MarketReport, SummaryStats, TokenReport


def test_market_csv_matches_market_table_fields(tmp_path):
    analyzer = PolymarketProfitAnalyzer()
    req = AnalysisRequest(
        address="0xe00740bce98a594e26861838885ab310ec3b548c",
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
                        entry_amount_usdc=4.2,
                        buy_qty=10,
                        realized_pnl_usdc=1.2,
                        trade_count=2,
                    ),
                    TokenReport(token_id="down", outcome="Down"),
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
            "Trade Time",
            "Realized PnL",
            "Taker Fee",
            "Maker Reward",
            "Entry Side",
            "Entry Amt",
            "Avg Entry",
        ],
        [
            "btc-updown-5m-1000",
            datetime.fromtimestamp(1000).strftime("%Y-%m-%d %H:%M:%S"),
            "1.2",
            "0.1",
            "0.2",
            "Up",
            "4.2",
            "0.42",
        ],
    ]
