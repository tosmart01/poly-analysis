from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FINALIZING = "FINALIZING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class AnalysisRequest(BaseModel):
    address: str
    start_ts: int
    end_ts: int
    keywords: list[str] = Field(default_factory=list)
    fee_rate_bps: float = 1000
    missing_cost_warn_qty: float = 0.5
    page_limit: int = 1000
    concurrency: int = 5
    request_timeout_sec: float = 20
    output_dir: str = "reports"

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        return _normalize_address(value)

    @field_validator("keywords")
    @classmethod
    def validate_keywords(cls, value: list[str]) -> list[str]:
        return _normalize_string_list(value)

    @model_validator(mode="after")
    def validate_time_range(self) -> "AnalysisRequest":
        if self.start_ts >= self.end_ts:
            raise ValueError("start_ts must be smaller than end_ts")
        return self


class RunCreated(BaseModel):
    run_id: str
    status: RunStatus


class RunStopAck(BaseModel):
    run_id: str
    status: RunStatus


class RunState(BaseModel):
    run_id: str
    status: RunStatus
    started_at: datetime | None = None
    ended_at: datetime | None = None
    progress_current: int = 0
    progress_total: int = 0
    message: str = ""


class WarningItem(BaseModel):
    timestamp: int | None = None
    market_slug: str | None = None
    token_id: str | None = None
    code: str
    message: str


class CurvePoint(BaseModel):
    timestamp: int
    delta_realized_pnl_usdc: float
    cumulative_realized_pnl_usdc: float


class TokenReport(BaseModel):
    token_id: str
    outcome: Literal["Up", "Down"]
    entry_amount_usdc: float = 0
    avg_entry_price: float | None = None
    realized_pnl_usdc: float = 0
    taker_fee_usdc: float = 0
    maker_reward_usdc: float = 0
    buy_qty: float = 0
    sell_qty: float = 0
    split_qty: float = 0
    redeem_qty: float = 0
    ending_position_qty: float = 0
    trade_count: int = 0


class MarketReport(BaseModel):
    market_slug: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    realized_pnl_usdc: float = 0
    taker_fee_usdc: float = 0
    maker_reward_usdc: float = 0
    ending_position_up: float = 0
    ending_position_down: float = 0
    tokens: list[TokenReport] = Field(default_factory=list)


class SummaryStats(BaseModel):
    total_realized_pnl_usdc: float = 0
    total_taker_fee_usdc: float = 0
    total_maker_reward_usdc: float = 0
    markets_total: int = 0
    markets_processed: int = 0


class MakerRebateRecord(BaseModel):
    timestamp: int
    usdc_size: float


class AnalysisReport(BaseModel):
    request: AnalysisRequest
    summary: SummaryStats
    markets: list[MarketReport]
    maker_rebates: list[MakerRebateRecord] = Field(default_factory=list)
    total_curve: list[CurvePoint]
    market_curves: dict[str, list[CurvePoint]]
    total_curve_no_fee: list[CurvePoint] = Field(default_factory=list)
    market_curves_no_fee: dict[str, list[CurvePoint]] = Field(default_factory=dict)
    warnings: list[WarningItem]
    is_partial: bool = False
    artifacts: dict[str, str] = Field(default_factory=dict)


class FeeSchedule(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    exponent: int | None = None
    rate: float = 0
    taker_only: bool = Field(default=False, alias="takerOnly")
    rebate_rate: float = Field(default=0, alias="rebateRate")


class PolymarketMarket(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    slug: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    outcomes: list[str]
    outcome_prices: list[float]
    fees_enabled: bool = Field(default=True, alias="feesEnabled")
    fee_schedule: FeeSchedule | None = Field(default=None, alias="feeSchedule")
    closed: bool = False
    outcome: str | None = None


class TradeRecord(BaseModel):
    transaction_hash: str = Field(alias="transactionHash")
    timestamp: int
    side: Literal["BUY", "SELL"]
    asset: str
    condition_id: str = Field(alias="conditionId")
    size: float
    price: float
    outcome: str | None = None


class ActivityRecord(BaseModel):
    transaction_hash: str = Field(alias="transactionHash")
    timestamp: int
    type: str
    condition_id: str = Field(alias="conditionId")
    slug: str = ""
    size: float = 0
    usdc_size: float = Field(default=0, alias="usdcSize")


class StreamEvent(BaseModel):
    event: str
    data: dict


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_address(value: str) -> str:
    lowered = value.lower().strip()
    if not lowered.startswith("0x"):
        raise ValueError("address must start with 0x")
    return lowered


def _normalize_string_list(value: list[str]) -> list[str]:
    return sorted({str(item).strip().lower() for item in value if str(item).strip()})
