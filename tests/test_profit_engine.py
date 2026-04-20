from analysis_poly.models import ActivityRecord, PolymarketMarket, TradeRecord
from analysis_poly.profit_engine import ProfitEngine, _fee_adjust


def test_fee_adjust_basic():
    adjusted, fee_token, fee_usdc = _fee_adjust(100.0, 0.5, "BUY", {"rate": 0.072})
    assert adjusted == 96.4
    assert fee_token == 3.6
    assert fee_usdc == 1.8


def test_fee_adjust_sell_keeps_size_and_charges_usdc_fee():
    adjusted, fee_token, fee_usdc = _fee_adjust(100.0, 0.5, "SELL", {"rate": 0.072})
    assert adjusted == 100.0
    assert fee_token == 0.0
    assert fee_usdc == 1.8


def test_profit_engine_taker_buy_sell_and_redeem_warning():
    market = PolymarketMarket(
        slug="btc-updown-5m-1000",
        condition_id="cond1",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fee_schedule={"rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0x01",
            "timestamp": 1774832400,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond1",
            "size": 10,
            "price": 0.5,
        }
    )
    taker_sell = TradeRecord.model_validate(
        {
            "transactionHash": "0x02",
            "timestamp": 1774832410,
            "side": "SELL",
            "asset": "up_token",
            "conditionId": "cond1",
            "size": 5,
            "price": 0.6,
        }
    )
    redeem = ActivityRecord.model_validate(
        {
            "transactionHash": "0x03",
            "timestamp": 1774832420,
            "type": "REDEEM",
            "conditionId": "cond1",
            "size": 1,
            "usdcSize": 1,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, deltas, warnings = engine.process_market(
        market=market,
        taker_trades=[taker_buy, taker_sell],
        all_trades=[taker_buy, taker_sell],
        split_activities=[],
        redeem_activities=[redeem],
    )

    assert report.market_slug == "btc-updown-5m-1000"
    assert len(deltas) >= 1
    # outcomePrices not unique winner => redeem skipped warning
    assert any(w.code == "REDEEM_SKIP_UNKNOWN_WINNER" for w in warnings)
    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.entry_amount_usdc == 5
    assert up.avg_entry_price is not None
    assert up.avg_entry_price > 0.5


def test_fee_before_hard_coded_window_is_zero_even_with_fee_schedule():
    market = PolymarketMarket(
        slug="btc-updown-5m-1767661199",
        condition_id="cond_fee_before",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fee_schedule={"rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0xbefore",
            "timestamp": 1767661199,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond_fee_before",
            "size": 100,
            "price": 0.5,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 100
    assert up.taker_fee_usdc == 0


def test_hard_coded_window_uses_default_fee_for_updown_5m_buy_and_sell():
    market = PolymarketMarket(
        slug="btc-updown-5m-1767661200",
        condition_id="cond_fee_window",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fee_schedule={"rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0xwindow_buy",
            "timestamp": 1767661200,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond_fee_window",
            "size": 100,
            "price": 0.5,
        }
    )
    taker_sell = TradeRecord.model_validate(
        {
            "transactionHash": "0xwindow_sell",
            "timestamp": 1767661210,
            "side": "SELL",
            "asset": "up_token",
            "conditionId": "cond_fee_window",
            "size": 50,
            "price": 0.6,
        }
    )

    engine = ProfitEngine(fee_rate_bps=123, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[taker_buy, taker_sell],
        all_trades=[taker_buy, taker_sell],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 98.4375
    assert up.taker_fee_usdc == 1.21325


def test_hard_coded_window_ignores_non_updown_5m_15m_slug():
    market = PolymarketMarket(
        slug="btc-updown-1h-1767661200",
        condition_id="cond_fee_window_other",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fee_schedule={"rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0xwindow_other",
            "timestamp": 1767661200,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond_fee_window_other",
            "size": 100,
            "price": 0.5,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 100
    assert up.taker_fee_usdc == 0


def test_fee_after_hard_coded_window_uses_fee_schedule_without_slug_filter():
    market = PolymarketMarket(
        slug="non-updown-market-1774832400",
        condition_id="cond_fee_after",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fee_schedule={"rate": 0.072, "takerOnly": True, "rebateRate": 0.2},
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0xafter",
            "timestamp": 1774832400,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond_fee_after",
            "size": 100,
            "price": 0.5,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 96.4
    assert up.taker_fee_usdc == 1.8


def test_profit_engine_split_allocation():
    market = PolymarketMarket(
        slug="eth-updown-15m-2000",
        condition_id="cond2",
        up_token_id="up",
        down_token_id="down",
        outcomes=["Up", "Down"],
        outcome_prices=[1, 0],
    )

    split = ActivityRecord.model_validate(
        {
            "transactionHash": "0x10",
            "timestamp": 2000,
            "type": "SPLIT",
            "conditionId": "cond2",
            "size": 6,
            "usdcSize": 6,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[],
        all_trades=[],
        split_activities=[split],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up")
    down = next(t for t in report.tokens if t.token_id == "down")
    assert up.split_qty == 3
    assert down.split_qty == 3


def test_avg_entry_price_is_weighted_by_all_buy_fills():
    market = PolymarketMarket(
        slug="btc-updown-5m-1500",
        condition_id="cond15",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
    )

    buy_a = TradeRecord.model_validate(
        {
            "transactionHash": "0x15a",
            "timestamp": 1490,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond15",
            "size": 10,
            "price": 0.2,
        }
    )
    buy_b = TradeRecord.model_validate(
        {
            "transactionHash": "0x15b",
            "timestamp": 1495,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond15",
            "size": 30,
            "price": 0.4,
        }
    )

    engine = ProfitEngine(fee_rate_bps=0, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[buy_a, buy_b],
        all_trades=[buy_a, buy_b],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 40
    assert up.entry_amount_usdc == 14
    assert up.avg_entry_price == 0.35


def test_fees_disabled_does_not_fall_back_to_request_bps():
    market = PolymarketMarket(
        slug="btc-updown-5m-1800",
        condition_id="cond18",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        fees_enabled=False,
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0x18",
            "timestamp": 1800,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond18",
            "size": 10,
            "price": 0.5,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, _ = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.buy_qty == 10
    assert up.taker_fee_usdc == 0


def test_closed_market_without_redeem_settles_remaining_position_by_outcome_prices():
    market = PolymarketMarket(
        slug="btc-updown-5m-3000",
        condition_id="cond3",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[1, 0],
        closed=True,
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0x11",
            "timestamp": 2990,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond3",
            "size": 10,
            "price": 0.4,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, warnings = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.ending_position_qty == 0
    assert report.ending_position_up == 0
    assert report.realized_pnl_usdc > 0
    assert up.realized_pnl_usdc > 0
    assert not any(w.code == "CLOSED_MARKET_UNKNOWN_OUTCOME" for w in warnings)


def test_closed_market_without_redeem_losing_position_counts_as_loss():
    market = PolymarketMarket(
        slug="btc-updown-5m-3500",
        condition_id="cond35",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0, 1],
        closed=True,
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0x115",
            "timestamp": 3490,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond35",
            "size": 10,
            "price": 0.4,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, warnings = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.ending_position_qty == 0
    assert up.realized_pnl_usdc < 0
    assert report.realized_pnl_usdc < 0
    assert not any(w.code == "CLOSED_MARKET_UNKNOWN_OUTCOME" for w in warnings)


def test_closed_market_without_resolved_outcome_prices_warns_and_keeps_position():
    market = PolymarketMarket(
        slug="btc-updown-5m-4000",
        condition_id="cond4",
        up_token_id="up_token",
        down_token_id="down_token",
        outcomes=["Up", "Down"],
        outcome_prices=[0.5, 0.5],
        closed=True,
    )

    taker_buy = TradeRecord.model_validate(
        {
            "transactionHash": "0x12",
            "timestamp": 3990,
            "side": "BUY",
            "asset": "up_token",
            "conditionId": "cond4",
            "size": 5,
            "price": 0.4,
        }
    )

    engine = ProfitEngine(fee_rate_bps=1000, missing_cost_warn_qty=0.5)
    report, _, warnings = engine.process_market(
        market=market,
        taker_trades=[taker_buy],
        all_trades=[taker_buy],
        split_activities=[],
        redeem_activities=[],
    )

    up = next(t for t in report.tokens if t.token_id == "up_token")
    assert up.ending_position_qty > 0
    assert any(w.code == "CLOSED_MARKET_UNKNOWN_OUTCOME" for w in warnings)
