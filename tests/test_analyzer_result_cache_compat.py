from analysis_poly.analyzer import _result_from_cache_payload


def test_result_cache_payload_without_entry_fields_is_invalid():
    payload = {
        "market_slug": "btc-updown-5m-1000",
        "market_report": {
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "cond",
            "up_token_id": "up",
            "down_token_id": "down",
            "realized_pnl_usdc": 1.0,
            "taker_fee_usdc": 0.1,
            "maker_reward_usdc": 0.0,
            "ending_position_up": 0.0,
            "ending_position_down": 0.0,
            "tokens": [
                {
                    "token_id": "up",
                    "outcome": "Up",
                    "buy_qty": 10,
                    "sell_qty": 10,
                    "redeem_qty": 0,
                    "ending_position_qty": 0,
                    "trade_count": 2,
                }
            ],
        },
        "market_report_no_fee": {
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "cond",
            "up_token_id": "up",
            "down_token_id": "down",
            "realized_pnl_usdc": 1.1,
            "taker_fee_usdc": 0.0,
            "maker_reward_usdc": 0.0,
            "ending_position_up": 0.0,
            "ending_position_down": 0.0,
            "tokens": [
                {
                    "token_id": "up",
                    "outcome": "Up",
                    "buy_qty": 10,
                    "sell_qty": 10,
                    "redeem_qty": 0,
                    "ending_position_qty": 0,
                    "trade_count": 2,
                }
            ],
        },
        "deltas": [],
        "deltas_no_fee": [],
        "warnings": [],
    }

    assert _result_from_cache_payload("btc-updown-5m-1000", payload) is None
