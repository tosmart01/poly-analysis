from __future__ import annotations

import asyncio
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from datetime import datetime, timezone

from loguru import logger

from .activity_discovery import (
    DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX,
    DiscoveredMarket,
    collect_user_activity,
    discover_user_markets_by_day,
    iter_day_windows,
)
from .market_cache import MarketMetadataCache
from .market_result_cache import AddressMarketResultCache
from .models import (
    AnalysisReport,
    AnalysisRequest,
    CurvePoint,
    MakerRebateRecord,
    MarketReport,
    SummaryStats,
    WarningItem,
)
from .polymarket_client import PolymarketApiClient
from .profit_engine import PnlDelta, ProfitEngine, build_curve

MARKET_FETCH_CONCURRENCY_DEFAULT = 10
MARKET_TIMESTAMP_CHUNK_SIZE_DEFAULT = 20
MARKET_RESULT_CACHE_RECENT_WINDOW_SEC = 30 * 60


@dataclass
class _MarketProcessResult:
    market_slug: str
    market_report: MarketReport
    market_report_no_fee: MarketReport
    deltas: list[PnlDelta]
    deltas_no_fee: list[PnlDelta]
    warnings: list[WarningItem]
    cache_updated: bool = False


class AnalyzerHooks(Protocol):
    async def on_run_started(self, total_markets: int) -> None: ...

    async def on_progress(self, current: int, total: int, market_slug: str) -> None: ...

    async def on_warning(self, warning: WarningItem) -> None: ...

    async def on_total_point(self, timestamp: int, delta: float, cumulative: float) -> None: ...

    async def on_market_point(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None: ...

    async def on_total_point_no_fee(self, timestamp: int, delta: float, cumulative: float) -> None: ...

    async def on_market_point_no_fee(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None: ...


class NullHooks:
    async def on_run_started(self, total_markets: int) -> None:
        return

    async def on_progress(self, current: int, total: int, market_slug: str) -> None:
        return

    async def on_warning(self, warning: WarningItem) -> None:
        return

    async def on_total_point(self, timestamp: int, delta: float, cumulative: float) -> None:
        return

    async def on_market_point(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None:
        return

    async def on_total_point_no_fee(self, timestamp: int, delta: float, cumulative: float) -> None:
        return

    async def on_market_point_no_fee(
        self, market_slug: str, timestamp: int, delta: float, cumulative: float
    ) -> None:
        return


class PolymarketProfitAnalyzer:
    def __init__(self):
        self._market_cache = MarketMetadataCache()
        self._market_result_cache = AddressMarketResultCache()
        self._market_fetch_concurrency = MARKET_FETCH_CONCURRENCY_DEFAULT
        self._timestamp_chunk_size = MARKET_TIMESTAMP_CHUNK_SIZE_DEFAULT

    async def run(
        self,
        req: AnalysisRequest,
        stop_event: asyncio.Event | None = None,
        hooks: AnalyzerHooks | None = None,
    ) -> AnalysisReport:
        stop_event = stop_event or asyncio.Event()
        hooks = hooks or NullHooks()

        client = PolymarketApiClient(timeout_sec=req.request_timeout_sec)
        engine = ProfitEngine(
            fee_rate_bps=req.fee_rate_bps,
            missing_cost_warn_qty=req.missing_cost_warn_qty,
        )
        engine_no_fee = ProfitEngine(
            fee_rate_bps=req.fee_rate_bps,
            missing_cost_warn_qty=req.missing_cost_warn_qty,
            charge_taker_fee=False,
        )

        all_warnings: list[WarningItem] = []
        market_reports: list[MarketReport] = []
        total_deltas: list[PnlDelta] = []
        total_deltas_no_fee: list[PnlDelta] = []
        market_deltas: dict[str, list[PnlDelta]] = defaultdict(list)
        market_deltas_no_fee: dict[str, list[PnlDelta]] = defaultdict(list)

        total_by_ts: dict[int, float] = defaultdict(float)
        market_by_ts: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        total_by_ts_no_fee: dict[int, float] = defaultdict(float)
        market_by_ts_no_fee: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        address_market_cache = self._market_result_cache.load(req.address)
        result_cache_dirty = False

        try:
            discovered_markets = await discover_user_markets_by_day(
                client=client,
                address=req.address,
                start_ts=req.start_ts,
                end_ts=req.end_ts,
                page_limit=req.page_limit,
                warnings=all_warnings,
            )
            discovered_markets = _filter_discovered_markets(
                discovered_markets,
                keywords=req.keywords,
            )
            total_markets = len(discovered_markets)
            logger.info(
                "analyzer discovered markets address={} market_count={} keywords={} market_fetch_concurrency={} process_concurrency={}",
                req.address,
                total_markets,
                ",".join(req.keywords),
                self._market_fetch_concurrency,
                max(1, req.concurrency),
            )
            await hooks.on_run_started(total_markets)

            process_concurrency = max(1, req.concurrency)
            processed_count = 0

            if stop_event.is_set():
                discovered_markets = []

            slug_to_market = {
                slug: market
                for slug, market in await self._fetch_markets_with_status(
                    client,
                    [item.slug for item in discovered_markets],
                    self._market_fetch_concurrency,
                )
                if market is not None
            }

            for batch_start in range(0, len(discovered_markets), process_concurrency):
                if stop_event.is_set():
                    break

                batch_refs = discovered_markets[batch_start : batch_start + process_concurrency]
                batch_markets = []
                missing_refs: list[DiscoveredMarket] = []
                for ref in batch_refs:
                    market = slug_to_market.get(ref.slug)
                    if market is None:
                        missing_refs.append(ref)
                        warning = WarningItem(
                            timestamp=ref.first_activity_ts,
                            market_slug=ref.slug,
                            code="DISCOVERY_MARKET_METADATA_MISSING",
                            message="market metadata not found for discovered slug",
                        )
                        all_warnings.append(warning)
                        await hooks.on_warning(warning)
                        continue
                    batch_markets.append(market)

                if missing_refs:
                    processed_count += len(missing_refs)
                    await hooks.on_progress(processed_count, total_markets, missing_refs[-1].slug)

                if not batch_markets:
                    continue

                batch_results = await asyncio.gather(
                    *(
                        self._process_single_market(
                            client=client,
                            engine=engine,
                            engine_no_fee=engine_no_fee,
                            address=req.address,
                            address_market_cache=address_market_cache,
                            req=req,
                            market=market,
                        )
                        for market in batch_markets
                    )
                )
                batch_results.sort(key=lambda x: _market_order_key(x.market_slug))

                for result in batch_results:
                    if stop_event.is_set():
                        break

                    processed_count += 1
                    has_trade_activity = _has_market_trade_activity(result.market_report)
                    if has_trade_activity:
                        market_reports.append(result.market_report)
                        total_deltas.extend(result.deltas)
                        total_deltas_no_fee.extend(result.deltas_no_fee)
                        if result.deltas:
                            market_deltas[result.market_slug].extend(result.deltas)
                        if result.deltas_no_fee:
                            market_deltas_no_fee[result.market_slug].extend(result.deltas_no_fee)
                    else:
                        logger.debug("skip market without trades in output slug={}", result.market_slug)

                    for warning in result.warnings:
                        all_warnings.append(warning)
                        await hooks.on_warning(warning)
                    if result.cache_updated:
                        result_cache_dirty = True

                    for delta in result.deltas:
                        total_by_ts[delta.timestamp] += delta.delta_pnl_usdc
                        market_by_ts[delta.market_slug][delta.timestamp] += delta.delta_pnl_usdc

                        total_cumulative = _cumulative_at(total_by_ts, delta.timestamp)
                        market_cumulative = _cumulative_at(market_by_ts[delta.market_slug], delta.timestamp)

                        await hooks.on_total_point(
                            delta.timestamp,
                            delta.delta_pnl_usdc,
                            total_cumulative,
                        )
                        await hooks.on_market_point(
                            delta.market_slug,
                            delta.timestamp,
                            delta.delta_pnl_usdc,
                            market_cumulative,
                        )

                    for delta in result.deltas_no_fee:
                        total_by_ts_no_fee[delta.timestamp] += delta.delta_pnl_usdc
                        market_by_ts_no_fee[delta.market_slug][delta.timestamp] += delta.delta_pnl_usdc

                        total_cumulative_no_fee = _cumulative_at(total_by_ts_no_fee, delta.timestamp)
                        market_cumulative_no_fee = _cumulative_at(
                            market_by_ts_no_fee[delta.market_slug], delta.timestamp
                        )

                        await hooks.on_total_point_no_fee(
                            delta.timestamp,
                            delta.delta_pnl_usdc,
                            total_cumulative_no_fee,
                        )
                        await hooks.on_market_point_no_fee(
                            delta.market_slug,
                            delta.timestamp,
                            delta.delta_pnl_usdc,
                            market_cumulative_no_fee,
                        )

                    await hooks.on_progress(processed_count, total_markets, result.market_slug)

            maker_rebate_records, maker_rebate_deltas = await self._collect_maker_rebate_deltas(
                client=client,
                address=req.address,
                start_ts=req.start_ts,
                end_ts=req.end_ts,
                page_limit=req.page_limit,
                warnings=all_warnings,
            )
            for delta in maker_rebate_deltas:
                total_deltas.append(delta)
                total_deltas_no_fee.append(delta)
                total_by_ts[delta.timestamp] += delta.delta_pnl_usdc
                total_by_ts_no_fee[delta.timestamp] += delta.delta_pnl_usdc

                total_cumulative = _cumulative_at(total_by_ts, delta.timestamp)
                total_cumulative_no_fee = _cumulative_at(total_by_ts_no_fee, delta.timestamp)

                await hooks.on_total_point(
                    delta.timestamp,
                    delta.delta_pnl_usdc,
                    total_cumulative,
                )
                await hooks.on_total_point_no_fee(
                    delta.timestamp,
                    delta.delta_pnl_usdc,
                    total_cumulative_no_fee,
                )

            total_curve = [
                CurvePoint(
                    timestamp=ts,
                    delta_realized_pnl_usdc=round(delta, 10),
                    cumulative_realized_pnl_usdc=round(cum, 10),
                )
                for ts, delta, cum in build_curve(total_deltas)
            ]

            market_curves: dict[str, list[CurvePoint]] = {}
            for market_slug, deltas in market_deltas.items():
                if not deltas:
                    continue
                market_curves[market_slug] = [
                    CurvePoint(
                        timestamp=ts,
                        delta_realized_pnl_usdc=round(delta, 10),
                        cumulative_realized_pnl_usdc=round(cum, 10),
                    )
                    for ts, delta, cum in build_curve(deltas)
                ]

            total_curve_no_fee = [
                CurvePoint(
                    timestamp=ts,
                    delta_realized_pnl_usdc=round(delta, 10),
                    cumulative_realized_pnl_usdc=round(cum, 10),
                )
                for ts, delta, cum in build_curve(total_deltas_no_fee)
            ]

            market_curves_no_fee: dict[str, list[CurvePoint]] = {}
            for market_slug, deltas in market_deltas_no_fee.items():
                if not deltas:
                    continue
                market_curves_no_fee[market_slug] = [
                    CurvePoint(
                        timestamp=ts,
                        delta_realized_pnl_usdc=round(delta, 10),
                        cumulative_realized_pnl_usdc=round(cum, 10),
                    )
                    for ts, delta, cum in build_curve(deltas)
                ]

            summary = SummaryStats(
                total_realized_pnl_usdc=round(
                    sum(m.realized_pnl_usdc for m in market_reports)
                    + sum(delta.delta_pnl_usdc for delta in maker_rebate_deltas),
                    10,
                ),
                total_taker_fee_usdc=round(sum(m.taker_fee_usdc for m in market_reports), 10),
                total_maker_reward_usdc=round(sum(delta.delta_pnl_usdc for delta in maker_rebate_deltas), 10),
                markets_total=total_markets,
                markets_processed=len(market_reports),
            )

            report = AnalysisReport(
                request=req,
                summary=summary,
                markets=sorted(market_reports, key=lambda x: x.market_slug),
                maker_rebates=maker_rebate_records,
                total_curve=total_curve,
                market_curves=market_curves,
                total_curve_no_fee=total_curve_no_fee,
                market_curves_no_fee=market_curves_no_fee,
                warnings=all_warnings,
                is_partial=stop_event.is_set() and len(market_reports) < total_markets,
            )

            return report
        finally:
            if result_cache_dirty:
                try:
                    self._market_result_cache.save(req.address, address_market_cache)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("market result cache save failed address={} error={}", req.address, exc)
            await client.aclose()

    async def _collect_maker_rebate_deltas(
        self,
        client: PolymarketApiClient,
        address: str,
        start_ts: int,
        end_ts: int,
        page_limit: int,
        warnings: list[WarningItem],
    ) -> tuple[list[MakerRebateRecord], list[PnlDelta]]:
        records_out: list[MakerRebateRecord] = []
        deltas: list[PnlDelta] = []
        capped_page_limit = min(max(1, page_limit), DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX)

        for window_start, window_end in iter_day_windows(start_ts, end_ts):
            day_records = await collect_user_activity(
                client=client,
                address=address,
                start_ts=window_start,
                end_ts=window_end,
                page_limit=capped_page_limit,
                warnings=warnings,
                activity_types=("MAKER_REBATE",),
            )
            for record in day_records:
                rebate_value = float(record.usdc_size or record.size or 0.0)
                if rebate_value == 0:
                    continue
                records_out.append(
                    MakerRebateRecord(
                        timestamp=int(record.timestamp),
                        usdc_size=rebate_value,
                    )
                )
                deltas.append(
                    PnlDelta(
                        timestamp=int(record.timestamp),
                        market_slug="__maker_rebate__",
                        token_id="",
                        delta_pnl_usdc=rebate_value,
                    )
                )

        records_out.sort(key=lambda item: item.timestamp, reverse=True)
        return records_out, sorted(deltas, key=lambda item: item.timestamp)

    async def _fetch_markets_with_status(
        self,
        client: PolymarketApiClient,
        slugs: list[str],
        concurrency: int,
    ) -> list[tuple[str, object | None]]:
        sem = asyncio.Semaphore(max(1, concurrency))

        async def fetch(slug: str):
            async with sem:
                market = await self._fetch_market_with_cache(client, slug)
                return slug, market

        return await asyncio.gather(*(fetch(s) for s in slugs))

    async def _fetch_market_with_cache(
        self,
        client: PolymarketApiClient,
        slug: str,
    ):
        now_ts = int(datetime.now(timezone.utc).timestamp())
        use_cache = self._market_cache.is_cache_eligible(slug, now_ts=now_ts)

        if use_cache:
            cached = self._market_cache.get(slug)
            if cached is not None:
                logger.debug("market cache hit slug={}", slug)
                return cached

        market = await client.get_market_by_slug(slug)
        if market is None:
            logger.warning("market not found slug={}", slug)
            return None

        if market is not None and use_cache:
            self._market_cache.set(slug, market)
            logger.debug("market cache write slug={}", slug)

        return market

    async def _process_single_market(
        self,
        client: PolymarketApiClient,
        engine: ProfitEngine,
        engine_no_fee: ProfitEngine,
        address: str,
        address_market_cache: dict[str, dict],
        req: AnalysisRequest,
        market,
    ) -> _MarketProcessResult:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        use_result_cache = _is_market_result_cache_eligible(
            market.slug,
            now_ts=now_ts,
            recent_window_sec=MARKET_RESULT_CACHE_RECENT_WINDOW_SEC,
        )

        if use_result_cache:
            cached_payload = address_market_cache.get(market.slug)
            if cached_payload is not None:
                cached_result = _result_from_cache_payload(market.slug, cached_payload)
                if cached_result is not None:
                    logger.debug("market result cache hit address={} slug={}", address, market.slug)
                    return cached_result
                logger.warning("market result cache invalid address={} slug={}", address, market.slug)

        taker_trades, all_trades, split_acts, redeem_acts = await asyncio.gather(
            client.get_trades(req.address, market.condition_id, True, req.page_limit),
            client.get_trades(req.address, market.condition_id, False, req.page_limit),
            client.get_activity(req.address, market.condition_id, "SPLIT", req.page_limit),
            client.get_activity(req.address, market.condition_id, "REDEEM", req.page_limit),
        )

        market_report, deltas, warnings = engine.process_market(
            market=market,
            taker_trades=taker_trades,
            all_trades=all_trades,
            split_activities=split_acts,
            redeem_activities=redeem_acts,
        )
        market_report_no_fee, deltas_no_fee, _ = engine_no_fee.process_market(
            market=market,
            taker_trades=taker_trades,
            all_trades=all_trades,
            split_activities=split_acts,
            redeem_activities=redeem_acts,
        )
        result = _MarketProcessResult(
            market_slug=market.slug,
            market_report=market_report,
            market_report_no_fee=market_report_no_fee,
            deltas=deltas,
            deltas_no_fee=deltas_no_fee,
            warnings=warnings,
        )
        if use_result_cache:
            new_payload = _result_to_cache_payload(result)
            if address_market_cache.get(market.slug) != new_payload:
                address_market_cache[market.slug] = new_payload
                result.cache_updated = True
        return result

    def save_json(self, report: AnalysisReport, path: str | None = None) -> str:
        output_dir = Path(report.request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not path:
            suffix = "partial" if report.is_partial else "final"
            path = str(output_dir / f"pnl_summary_{suffix}.json")

        Path(path).write_text(report.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_total_curve_csv(self, report: AnalysisReport, path: str | None = None) -> str:
        output_dir = Path(report.request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not path:
            suffix = "partial" if report.is_partial else "final"
            path = str(output_dir / f"pnl_total_curve_{suffix}.csv")

        with Path(path).open("w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(["timestamp", "delta_realized_pnl_usdc", "cumulative_realized_pnl_usdc"])
            for p in report.total_curve:
                writer.writerow([p.timestamp, p.delta_realized_pnl_usdc, p.cumulative_realized_pnl_usdc])
        return path

    def save_market_curve_csv(self, report: AnalysisReport, path: str | None = None) -> str:
        output_dir = Path(report.request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not path:
            suffix = "partial" if report.is_partial else "final"
            path = str(output_dir / f"pnl_market_curve_{suffix}.csv")

        with Path(path).open("w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(_MARKET_TABLE_CSV_COLUMNS)
            for market in report.markets:
                avg_entry_price = _market_avg_entry_price(market)
                writer.writerow(
                    [
                        market.market_slug,
                        _market_trade_time(market.market_slug),
                        market.realized_pnl_usdc,
                        market.taker_fee_usdc,
                        market.maker_reward_usdc,
                        _market_entry_side(market),
                        _market_entry_amount(market),
                        "" if avg_entry_price is None else avg_entry_price,
                    ]
                )
        return path

    def save_curve_csv(self, report: AnalysisReport, path: str | None = None) -> str:
        return self.save_total_curve_csv(report, path)



def _cumulative_at(by_ts: dict[int, float], ts: int) -> float:
    cumulative = 0.0
    for key in sorted(by_ts.keys()):
        if key > ts:
            break
        cumulative += by_ts[key]
    return cumulative


def _market_order_key(slug: str) -> tuple[int, str]:
    try:
        return int(str(slug).rsplit("-", 1)[-1]), slug
    except Exception:  # noqa: BLE001
        return 10**18, slug


def _has_market_trade_activity(market_report: MarketReport) -> bool:
    return any(token.trade_count > 0 for token in market_report.tokens)


_MARKET_TABLE_CSV_COLUMNS = [
    "Market",
    "Trade Time",
    "Realized PnL",
    "Taker Fee",
    "Maker Reward",
    "Entry Side",
    "Entry Amt",
    "Avg Entry",
]


def _market_ts(market_slug: str) -> int | None:
    try:
        ts = int(str(market_slug or "").split("-")[-1])
    except ValueError:
        return None
    if ts <= 0:
        return None
    return ts


def _market_trade_time(market_slug: str) -> str:
    ts = _market_ts(market_slug)
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _market_entry_side(market_report: MarketReport) -> str:
    sides = []
    for token in market_report.tokens:
        if token.buy_qty > 0 or token.entry_amount_usdc > 0:
            sides.append(token.outcome)

    unique_sides = list(dict.fromkeys(side for side in sides if side))
    if not unique_sides:
        return "-"
    if len(unique_sides) == 1:
        return unique_sides[0]
    return "Both"


def _market_entry_amount(market_report: MarketReport) -> float:
    return round(sum(token.entry_amount_usdc for token in market_report.tokens), 10)


def _market_avg_entry_price(market_report: MarketReport) -> float | None:
    total_buy_qty = sum(token.buy_qty for token in market_report.tokens)
    if total_buy_qty <= 1e-12:
        return None
    return round(_market_entry_amount(market_report) / total_buy_qty, 10)


def _chunk_specs_by_timestamp(
    specs: list[MarketSlugSpec], timestamps_per_chunk: int
) -> list[list[MarketSlugSpec]]:
    if not specs:
        return []
    chunk_size = max(1, int(timestamps_per_chunk))

    ts_to_specs: dict[int, list[MarketSlugSpec]] = defaultdict(list)
    ts_order: list[int] = []
    for spec in specs:
        if spec.timestamp not in ts_to_specs:
            ts_order.append(spec.timestamp)
        ts_to_specs[spec.timestamp].append(spec)

    chunks: list[list[MarketSlugSpec]] = []
    for start in range(0, len(ts_order), chunk_size):
        ts_slice = ts_order[start : start + chunk_size]
        chunk_specs: list[MarketSlugSpec] = []
        for ts in ts_slice:
            chunk_specs.extend(ts_to_specs[ts])
        chunks.append(chunk_specs)
    return chunks


def _result_to_cache_payload(result: _MarketProcessResult) -> dict:
    return {
        "market_slug": result.market_slug,
        "market_report": result.market_report.model_dump(),
        "market_report_no_fee": result.market_report_no_fee.model_dump(),
        "deltas": [_delta_to_dict(d) for d in result.deltas],
        "deltas_no_fee": [_delta_to_dict(d) for d in result.deltas_no_fee],
        "warnings": [w.model_dump() for w in result.warnings],
    }


def _result_from_cache_payload(slug: str, payload: dict) -> _MarketProcessResult | None:
    try:
        if not _cache_payload_has_entry_fields(payload.get("market_report")):
            return None
        if not _cache_payload_has_entry_fields(payload.get("market_report_no_fee")):
            return None

        market_report = MarketReport.model_validate(payload["market_report"])
        market_report_no_fee = MarketReport.model_validate(payload["market_report_no_fee"])
        deltas = [_delta_from_dict(d) for d in payload.get("deltas", [])]
        deltas_no_fee = [_delta_from_dict(d) for d in payload.get("deltas_no_fee", [])]
        warnings = [WarningItem.model_validate(w) for w in payload.get("warnings", [])]
        return _MarketProcessResult(
            market_slug=slug,
            market_report=market_report,
            market_report_no_fee=market_report_no_fee,
            deltas=deltas,
            deltas_no_fee=deltas_no_fee,
            warnings=warnings,
        )
    except Exception:  # noqa: BLE001
        return None


def _cache_payload_has_entry_fields(report_payload: dict | None) -> bool:
    if not isinstance(report_payload, dict):
        return False

    if abs(float(report_payload.get("maker_reward_usdc", 0) or 0)) > 1e-12:
        return False

    tokens = report_payload.get("tokens", [])
    if not isinstance(tokens, list):
        return False

    for token in tokens:
        if not isinstance(token, dict):
            return False
        if abs(float(token.get("maker_reward_usdc", 0) or 0)) > 1e-12:
            return False
        buy_qty = float(token.get("buy_qty", 0) or 0)
        if buy_qty <= 0:
            continue
        if "entry_amount_usdc" not in token:
            return False
        if "avg_entry_price" not in token:
            return False
    return True


def _delta_to_dict(delta: PnlDelta) -> dict:
    return {
        "timestamp": int(delta.timestamp),
        "market_slug": str(delta.market_slug),
        "token_id": str(delta.token_id),
        "delta_pnl_usdc": float(delta.delta_pnl_usdc),
    }


def _delta_from_dict(payload: dict) -> PnlDelta:
    return PnlDelta(
        timestamp=int(payload["timestamp"]),
        market_slug=str(payload["market_slug"]),
        token_id=str(payload["token_id"]),
        delta_pnl_usdc=float(payload["delta_pnl_usdc"]),
    )


def _is_market_result_cache_eligible(slug: str, now_ts: int, recent_window_sec: int) -> bool:
    market_ts = _market_order_key(slug)[0]
    if market_ts >= 10**18:
        return False
    return (now_ts - market_ts) > recent_window_sec


def _filter_discovered_markets(
    discovered_markets: list[DiscoveredMarket],
    keywords: list[str],
) -> list[DiscoveredMarket]:
    return [item for item in discovered_markets if _slug_matches_filters(item.slug, keywords=keywords)]


def _slug_matches_filters(slug: str, keywords: list[str]) -> bool:
    normalized_slug = str(slug or "").strip().lower()
    if not normalized_slug:
        return False

    if keywords and not any(keyword in normalized_slug for keyword in keywords):
        return False

    return True
