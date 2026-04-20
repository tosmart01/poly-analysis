from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Mapping

from .models import (
    ActivityRecord,
    MarketReport,
    PolymarketMarket,
    TokenReport,
    WarningItem,
)
from .models import TradeRecord


_HARD_CODED_FEE_START_TS = 1767661200  # 2026-01-06 09:00:00 Asia/Shanghai
_HARD_CODED_FEE_END_TS = 1774832400  # 2026-03-30 09:00:00 Asia/Shanghai
_HARD_CODED_FEE_RATE_BPS = 1000.0
_HARD_CODED_FEE_SLUG_PARTS = ("updown-5m", "updown-15m")


@dataclass
class PnlDelta:
    timestamp: int
    market_slug: str
    token_id: str
    delta_pnl_usdc: float


@dataclass
class _Lot:
    qty: float
    cost_per_qty: float


@dataclass
class _TokenState:
    token_id: str
    outcome: str
    lots: deque[_Lot]
    buy_cost_usdc: float = 0.0
    realized_pnl_usdc: float = 0.0
    taker_fee_usdc: float = 0.0
    maker_reward_usdc: float = 0.0
    buy_qty: float = 0.0
    sell_qty: float = 0.0
    split_qty: float = 0.0
    redeem_qty: float = 0.0
    trade_count: int = 0

    @property
    def position_qty(self) -> float:
        return sum(lot.qty for lot in self.lots)

    @property
    def avg_entry_price(self) -> float | None:
        if self.buy_qty <= 1e-12:
            return None
        return self.buy_cost_usdc / self.buy_qty


@dataclass
class _Event:
    timestamp: int
    tx: str
    kind: str
    token_id: str | None = None
    side: str | None = None
    size: float = 0.0
    price: float = 0.0
    usdc_size: float = 0.0
    is_taker: bool = False


class ProfitEngine:
    def __init__(
        self,
        fee_rate_bps: float,
        missing_cost_warn_qty: float,
        charge_taker_fee: bool = True,
    ):
        self._fee_rate_bps = fee_rate_bps
        self._missing_cost_warn_qty = missing_cost_warn_qty
        self._charge_taker_fee = charge_taker_fee

    def process_market(
        self,
        market: PolymarketMarket,
        taker_trades: list[TradeRecord],
        all_trades: list[TradeRecord],
        split_activities: list[ActivityRecord],
        redeem_activities: list[ActivityRecord],
    ) -> tuple[MarketReport, list[PnlDelta], list[WarningItem]]:
        warnings: list[WarningItem] = []
        token_states: dict[str, _TokenState] = {
            market.up_token_id: _TokenState(token_id=market.up_token_id, outcome="Up", lots=deque()),
            market.down_token_id: _TokenState(token_id=market.down_token_id, outcome="Down", lots=deque()),
        }

        taker_keys = {_trade_key(t) for t in taker_trades}
        events: list[_Event] = []

        for trade in all_trades:
            is_taker = _trade_key(trade) in taker_keys
            events.append(
                _Event(
                    timestamp=trade.timestamp,
                    tx=trade.transaction_hash,
                    kind="TRADE",
                    token_id=trade.asset,
                    side=trade.side,
                    size=float(trade.size),
                    price=float(trade.price),
                    is_taker=is_taker,
                )
            )

        for split in split_activities:
            events.append(
                _Event(
                    timestamp=split.timestamp,
                    tx=split.transaction_hash,
                    kind="SPLIT",
                    size=float(split.size),
                    usdc_size=float(split.usdc_size),
                )
            )

        for redeem in redeem_activities:
            winner_token = _resolve_winner_token(market)
            if not winner_token:
                warnings.append(
                    WarningItem(
                        timestamp=redeem.timestamp,
                        market_slug=market.slug,
                        code="REDEEM_SKIP_UNKNOWN_WINNER",
                        message="skip redeem because winner outcome cannot be uniquely inferred",
                    )
                )
                continue

            events.append(
                _Event(
                    timestamp=redeem.timestamp,
                    tx=redeem.transaction_hash,
                    kind="REDEEM",
                    token_id=winner_token,
                    size=float(redeem.size),
                    usdc_size=float(redeem.usdc_size),
                )
            )

        events.sort(key=lambda e: (e.timestamp, e.tx, _event_priority(e.kind)))

        pnl_deltas: list[PnlDelta] = []
        for event in events:
            if event.kind == "TRADE" and event.token_id in token_states:
                    token_state = token_states[event.token_id]
                    token_state.trade_count += 1
                    delta, new_warnings = self._apply_trade(
                        market=market,
                        market_slug=market.slug,
                        token_state=token_state,
                        event=event,
                    )
                    pnl_deltas.extend(delta)
                    warnings.extend(new_warnings)
            elif event.kind == "SPLIT":
                up_state = token_states[market.up_token_id]
                down_state = token_states[market.down_token_id]

                qty_each = event.size / 2.0
                usdc_each = event.usdc_size / 2.0
                if qty_each > 0:
                    up_state.lots.append(_Lot(qty=qty_each, cost_per_qty=usdc_each / qty_each))
                    down_state.lots.append(_Lot(qty=qty_each, cost_per_qty=usdc_each / qty_each))
                up_state.split_qty += qty_each
                down_state.split_qty += qty_each
            elif event.kind == "REDEEM" and event.token_id in token_states:
                token_state = token_states[event.token_id]
                delta, new_warnings = self._close_position(
                    market_slug=market.slug,
                    token_state=token_state,
                    timestamp=event.timestamp,
                    quantity=event.size,
                    proceeds=event.usdc_size,
                    missing_cost_warn_code="REDEEM_OVERSELL_ZERO_COST",
                )
                token_state.redeem_qty += event.size
                pnl_deltas.extend(delta)
                warnings.extend(new_warnings)

        settlement_deltas, settlement_warnings = self._settle_closed_market_positions(
            market=market,
            token_states=token_states,
            events=events,
        )
        pnl_deltas.extend(settlement_deltas)
        warnings.extend(settlement_warnings)

        token_reports: list[TokenReport] = []
        for token_state in token_states.values():
            token_reports.append(
                TokenReport(
                    token_id=token_state.token_id,
                    outcome=token_state.outcome,
                    entry_amount_usdc=round(token_state.buy_cost_usdc, 10),
                    avg_entry_price=(
                        round(token_state.avg_entry_price, 10)
                        if token_state.avg_entry_price is not None
                        else None
                    ),
                    realized_pnl_usdc=round(token_state.realized_pnl_usdc, 10),
                    taker_fee_usdc=round(token_state.taker_fee_usdc, 10),
                    maker_reward_usdc=round(token_state.maker_reward_usdc, 10),
                    buy_qty=round(token_state.buy_qty, 10),
                    sell_qty=round(token_state.sell_qty, 10),
                    split_qty=round(token_state.split_qty, 10),
                    redeem_qty=round(token_state.redeem_qty, 10),
                    ending_position_qty=round(token_state.position_qty, 10),
                    trade_count=token_state.trade_count,
                )
            )

        market_report = MarketReport(
            market_slug=market.slug,
            condition_id=market.condition_id,
            up_token_id=market.up_token_id,
            down_token_id=market.down_token_id,
            realized_pnl_usdc=round(sum(t.realized_pnl_usdc for t in token_reports), 10),
            taker_fee_usdc=round(sum(t.taker_fee_usdc for t in token_reports), 10),
            maker_reward_usdc=round(sum(t.maker_reward_usdc for t in token_reports), 10),
            ending_position_up=round(token_states[market.up_token_id].position_qty, 10),
            ending_position_down=round(token_states[market.down_token_id].position_qty, 10),
            tokens=sorted(token_reports, key=lambda x: x.token_id),
        )

        return market_report, pnl_deltas, warnings

    def _apply_trade(
        self,
        market: PolymarketMarket,
        market_slug: str,
        token_state: _TokenState,
        event: _Event,
    ) -> tuple[list[PnlDelta], list[WarningItem]]:
        deltas: list[PnlDelta] = []
        warnings: list[WarningItem] = []

        adjusted_size, _, fee_usdc = _fee_adjust_for_trade(
            size=event.size,
            price=event.price,
            side=event.side,
            timestamp=event.timestamp,
            market_slug=market.slug,
            fee_schedule=_market_fee_schedule(market),
            fallback_fee_rate_bps=self._fee_rate_bps,
        )

        if event.side == "BUY":
            qty_add = event.size
            if event.is_taker and self._charge_taker_fee:
                qty_add = adjusted_size
            total_cost = event.size * event.price
            if qty_add > 0:
                token_state.lots.append(_Lot(qty=qty_add, cost_per_qty=total_cost / qty_add))
            token_state.buy_qty += qty_add
            token_state.buy_cost_usdc += total_cost
            if event.is_taker and self._charge_taker_fee:
                token_state.taker_fee_usdc += fee_usdc
        elif event.side == "SELL":
            token_state.sell_qty += event.size
            proceeds = event.size * event.price
            if event.is_taker and self._charge_taker_fee:
                proceeds -= fee_usdc
            close_deltas, close_warnings = self._close_position(
                market_slug=market_slug,
                token_state=token_state,
                timestamp=event.timestamp,
                quantity=event.size,
                proceeds=proceeds,
                missing_cost_warn_code="SELL_OVERSELL_ZERO_COST",
            )
            deltas.extend(close_deltas)
            warnings.extend(close_warnings)
            if event.is_taker and self._charge_taker_fee:
                token_state.taker_fee_usdc += fee_usdc

        return deltas, warnings

    def _settle_closed_market_positions(
        self,
        market: PolymarketMarket,
        token_states: dict[str, _TokenState],
        events: list[_Event],
    ) -> tuple[list[PnlDelta], list[WarningItem]]:
        if not market.closed:
            return [], []

        unsettled_states = [state for state in token_states.values() if state.position_qty > 1e-12]
        if not unsettled_states:
            return [], []

        winner_token = _resolve_winner_token(market)
        if not winner_token:
            return [], [
                WarningItem(
                    timestamp=_settlement_timestamp(market, events),
                    market_slug=market.slug,
                    code="CLOSED_MARKET_UNKNOWN_OUTCOME",
                    message="market is closed but winner outcome cannot be uniquely inferred",
                )
            ]

        settlement_ts = _settlement_timestamp(market, events)
        deltas: list[PnlDelta] = []
        warnings: list[WarningItem] = []
        for token_state in unsettled_states:
            quantity = token_state.position_qty
            proceeds = quantity if token_state.token_id == winner_token else 0.0
            close_deltas, close_warnings = self._close_position(
                market_slug=market.slug,
                token_state=token_state,
                timestamp=settlement_ts,
                quantity=quantity,
                proceeds=proceeds,
                missing_cost_warn_code="CLOSED_MARKET_SETTLEMENT_ZERO_COST",
            )
            deltas.extend(close_deltas)
            warnings.extend(close_warnings)

        return deltas, warnings

    def _close_position(
        self,
        market_slug: str,
        token_state: _TokenState,
        timestamp: int,
        quantity: float,
        proceeds: float,
        missing_cost_warn_code: str,
    ) -> tuple[list[PnlDelta], list[WarningItem]]:
        warnings: list[WarningItem] = []
        quantity = max(0.0, quantity)
        if quantity == 0:
            return [], warnings

        remaining = quantity
        realized_cost = 0.0
        while remaining > 1e-12 and token_state.lots:
            lot = token_state.lots[0]
            take = min(lot.qty, remaining)
            realized_cost += take * lot.cost_per_qty
            lot.qty -= take
            remaining -= take
            if lot.qty <= 1e-12:
                token_state.lots.popleft()

        if remaining > self._missing_cost_warn_qty:
            warnings.append(
                WarningItem(
                    timestamp=timestamp,
                    market_slug=market_slug,
                    token_id=token_state.token_id,
                    code=missing_cost_warn_code,
                    message=(
                        "position shortfall consumed at zero cost basis, "
                        f"missing_qty={remaining:.6f}"
                    ),
                )
            )

        realized = proceeds - realized_cost
        token_state.realized_pnl_usdc += realized

        return [
            PnlDelta(
                timestamp=timestamp,
                market_slug=market_slug,
                token_id=token_state.token_id,
                delta_pnl_usdc=realized,
            )
        ], warnings



def _fee_adjust_for_trade(
    size: float,
    price: float,
    side: str | None,
    timestamp: int,
    market_slug: str,
    fee_schedule: Mapping[str, Any] | None = None,
    fallback_fee_rate_bps: float = 0.0,
) -> tuple[float, float, float]:
    if timestamp < _HARD_CODED_FEE_START_TS:
        return size, 0.0, 0.0

    if timestamp < _HARD_CODED_FEE_END_TS:
        if _has_hard_coded_fee_slug(market_slug):
            return _legacy_fee_adjust(size, price, _HARD_CODED_FEE_RATE_BPS)
        return size, 0.0, 0.0

    return _fee_adjust(
        size=size,
        price=price,
        side=side,
        fee_schedule=fee_schedule,
        fallback_fee_rate_bps=fallback_fee_rate_bps,
    )


def _fee_adjust(
    size: float,
    price: float,
    side: str | None,
    fee_schedule: Mapping[str, Any] | None = None,
    fallback_fee_rate_bps: float = 0.0,
) -> tuple[float, float, float]:
    if size <= 0 or price <= 0:
        return size, 0.0, 0.0

    if fee_schedule is not None:
        rate = float(fee_schedule.get("rate") or 0.0)
        if rate <= 0:
            return size, 0.0, 0.0
        fee_usdc = round(max(size * rate * price * (1 - price), 0.0), 5)
        if fee_usdc <= 0:
            return size, 0.0, 0.0

        side_value = str(getattr(side, "value", side) or "").upper()
        if side_value == "BUY":
            fee_token = fee_usdc / price
            adjusted_size = max(size - fee_token, 0.0)
            return adjusted_size, fee_token, fee_usdc

        return size, 0.0, fee_usdc

    return _legacy_fee_adjust(size, price, fallback_fee_rate_bps)


def _legacy_fee_adjust(size: float, price: float, fee_rate_bps: float) -> tuple[float, float, float]:
    adjusted_size = _default_fee_calc(size, price, fee_rate_bps)
    fee_token = size - adjusted_size
    fee_usdc = fee_token * price
    return adjusted_size, fee_token, fee_usdc


def _default_fee_calc(size: float, price: float, fee_rate_bps: float) -> float:
    fee_multiplier = fee_rate_bps / 1000 if fee_rate_bps else 0.0
    fee = 0.25 * (price * (1 - price)) ** 2 * fee_multiplier
    return (1 - fee) * size


def _has_hard_coded_fee_slug(market_slug: str) -> bool:
    slug = market_slug.lower()
    return any(part in slug for part in _HARD_CODED_FEE_SLUG_PARTS)


def _market_fee_schedule(market: PolymarketMarket) -> Mapping[str, Any] | None:
    if not market.fees_enabled:
        return {"rate": 0.0, "rebateRate": 0.0}
    if market.fee_schedule is None:
        return None
    return market.fee_schedule.model_dump()



def _event_priority(kind: str) -> int:
    if kind == "SPLIT":
        return 0
    if kind == "TRADE":
        return 1
    if kind == "REDEEM":
        return 2
    return 9



def _trade_key(trade: TradeRecord) -> tuple[str, str, str, float, float, int]:
    return (
        trade.transaction_hash,
        trade.asset,
        trade.side,
        float(trade.price),
        float(trade.size),
        int(trade.timestamp),
    )



def _resolve_winner_token(market: PolymarketMarket) -> str | None:
    if len(market.outcome_prices) < 2:
        return None
    up_price = market.outcome_prices[0]
    down_price = market.outcome_prices[1]
    if up_price == 1 and down_price == 0:
        return market.up_token_id
    if up_price == 0 and down_price == 1:
        return market.down_token_id
    return None


def _market_ts_from_slug(market_slug: str) -> int | None:
    try:
        return int(str(market_slug).rsplit("-", 1)[-1])
    except Exception:  # noqa: BLE001
        return None


def _settlement_timestamp(market: PolymarketMarket, events: list[_Event]) -> int:
    event_ts = max((event.timestamp for event in events), default=0)
    market_ts = _market_ts_from_slug(market.slug) or 0
    return max(event_ts, market_ts)



def build_curve(deltas: list[PnlDelta]) -> list[tuple[int, float, float]]:
    by_ts: dict[int, float] = defaultdict(float)
    for delta in deltas:
        by_ts[delta.timestamp] += delta.delta_pnl_usdc

    cumulative = 0.0
    points: list[tuple[int, float, float]] = []
    for ts in sorted(by_ts.keys()):
        delta = by_ts[ts]
        cumulative += delta
        points.append((ts, delta, cumulative))
    return points
