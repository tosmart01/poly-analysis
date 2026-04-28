from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from loguru import logger

from .models import WarningItem

DISCOVERY_ACTIVITY_TYPES = ("TRADE", "SPLIT", "REDEEM")
DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX = 500
DISCOVERY_ACTIVITY_OFFSET_MAX = 10000
DISCOVERY_ACTIVITY_WINDOW_SEC = 2 * 60 * 60
DAY_WINDOW_SEC = 24 * 60 * 60
WEEK_WINDOW_SEC = 7 * DAY_WINDOW_SEC


@dataclass
class DiscoveredMarket:
    slug: str
    condition_id: str
    first_activity_ts: int
    last_activity_ts: int


async def collect_user_activity(
    client,
    address: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    warnings: list[WarningItem],
    activity_types: list[str] | tuple[str, ...] | None = None,
    log_detail: bool = True,
    allow_range_cache: bool = True,
) -> list:
    started = time.perf_counter()
    normalized_activity_types = tuple(activity_types or DISCOVERY_ACTIVITY_TYPES)
    cache = getattr(client, "_activity_page_cache", None)
    now_ts = int(time.time())
    use_range_cache = (
        allow_range_cache
        and cache is not None
        and start_ts is not None
        and end_ts is not None
        and cache.is_cache_eligible(end_ts=end_ts, now_ts=now_ts)
    )

    if use_range_cache:
        cached_records, missing_segments = cache.load_range(
            user=address,
            activity_types=normalized_activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            sort_direction="ASC",
        )
        if not missing_segments:
            deduped = dedupe_activity_records(cached_records)
            if log_detail:
                logger.info(
                    "activity range cache hit address={} types={} range=[{}, {}] cached_records={} elapsed_sec={:.3f}",
                    address,
                    ",".join(normalized_activity_types),
                    start_ts,
                    end_ts,
                    len(deduped),
                    time.perf_counter() - started,
                )
            return deduped

        if cached_records and log_detail:
            logger.info(
                "activity range cache partial hit address={} types={} range=[{}, {}] cached_records={} missing_segments={} elapsed_sec={:.3f}",
                address,
                ",".join(normalized_activity_types),
                start_ts,
                end_ts,
                len(cached_records),
                len(missing_segments),
                time.perf_counter() - started,
            )

        records = list(cached_records)
        for missing_start, missing_end in missing_segments:
            records.extend(
                await collect_user_activity(
                    client=client,
                    address=address,
                    start_ts=missing_start,
                    end_ts=missing_end,
                    page_limit=page_limit,
                    warnings=warnings,
                    activity_types=normalized_activity_types,
                    log_detail=False,
                    allow_range_cache=False,
                )
            )
        deduped = dedupe_activity_records(records)
        cache.save_range(
            user=address,
            activity_types=normalized_activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            sort_direction="ASC",
            records=deduped,
        )
        if log_detail:
            logger.info(
                "activity collect done address={} types={} range=[{}, {}] cache_mode=range records={} deduped={} elapsed_sec={:.3f}",
                address,
                ",".join(normalized_activity_types),
                start_ts,
                end_ts,
                len(records),
                len(deduped),
                time.perf_counter() - started,
            )
        return deduped

    records: list = []
    offset = 0
    reached_offset_cap = False
    request_count = 0

    while True:
        request_count += 1
        page = await client.get_user_activity_page(
            user=address,
            activity_types=list(normalized_activity_types),
            start_ts=start_ts,
            end_ts=end_ts,
            limit=page_limit,
            offset=offset,
            sort_direction="ASC",
        )
        if not page:
            break
        records.extend(page)
        if len(page) < page_limit:
            break
        offset += len(page)
        if offset > DISCOVERY_ACTIVITY_OFFSET_MAX:
            reached_offset_cap = True
            break

    if not reached_offset_cap:
        deduped = dedupe_activity_records(records)
        if log_detail:
            logger.info(
                "activity collect done address={} types={} range=[{}, {}] requests={} records={} deduped={} elapsed_sec={:.3f}",
                address,
                ",".join(normalized_activity_types),
                start_ts,
                end_ts,
                request_count,
                len(records),
                len(deduped),
                time.perf_counter() - started,
            )
        if cache is not None and start_ts is not None and end_ts is not None and cache.is_cache_eligible(end_ts=end_ts, now_ts=now_ts):
            cache.save_range(
                user=address,
                activity_types=normalized_activity_types,
                start_ts=start_ts,
                end_ts=end_ts,
                sort_direction="ASC",
                records=deduped,
            )
        return deduped

    if end_ts - start_ts <= 1:
        warnings.append(
            WarningItem(
                timestamp=start_ts,
                code="DISCOVERY_WINDOW_TRUNCATED",
                message="user activity window is too dense to split further; discovery may be incomplete",
            )
        )
        deduped = dedupe_activity_records(records)
        logger.warning(
            "activity collect truncated address={} types={} range=[{}, {}] requests={} records={} deduped={} elapsed_sec={:.3f}",
            address,
            ",".join(normalized_activity_types),
            start_ts,
            end_ts,
            request_count,
            len(records),
            len(deduped),
            time.perf_counter() - started,
        )
        return deduped

    split_ts = start_ts + ((end_ts - start_ts) // 2)
    logger.info(
        "activity collect split address={} types={} range=[{}, {}] requests={} records={} split_ts={} elapsed_sec={:.3f}",
        address,
        ",".join(normalized_activity_types),
        start_ts,
        end_ts,
        request_count,
        len(records),
        split_ts,
        time.perf_counter() - started,
    )
    left_records, right_records = await asyncio.gather(
        collect_user_activity(
            client=client,
            address=address,
            start_ts=start_ts,
            end_ts=split_ts,
            page_limit=page_limit,
            warnings=warnings,
            activity_types=activity_types,
            log_detail=log_detail,
            allow_range_cache=False,
        ),
        collect_user_activity(
            client=client,
            address=address,
            start_ts=split_ts + 1,
            end_ts=end_ts,
            page_limit=page_limit,
            warnings=warnings,
            activity_types=activity_types,
            log_detail=log_detail,
            allow_range_cache=False,
        ),
    )
    deduped = dedupe_activity_records([*left_records, *right_records])
    if cache is not None and start_ts is not None and end_ts is not None and cache.is_cache_eligible(end_ts=end_ts, now_ts=now_ts):
        cache.save_range(
            user=address,
            activity_types=normalized_activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            sort_direction="ASC",
            records=deduped,
        )
    return deduped


async def collect_user_activity_for_windows(
    client,
    address: str,
    windows: list[tuple[int, int]],
    page_limit: int,
    warnings: list[WarningItem],
    activity_types: list[str] | tuple[str, ...],
    label: str,
) -> list:
    if not windows:
        return []

    started = time.perf_counter()
    normalized_activity_types = tuple(activity_types or DISCOVERY_ACTIVITY_TYPES)
    overall_start = min(window_start for window_start, _ in windows)
    overall_end = max(window_end for _, window_end in windows)
    cache = getattr(client, "_activity_page_cache", None)
    now_ts = int(time.time())

    records = []
    missing_window_segments = list(windows)
    if cache is not None:
        cached_records, missing_segments = cache.load_range(
            user=address,
            activity_types=normalized_activity_types,
            start_ts=overall_start,
            end_ts=overall_end,
            sort_direction="ASC",
        )
        records.extend(cached_records)
        missing_window_segments = _intersect_segments_with_windows(missing_segments, windows)
        logger.info(
            "activity window group cache check address={} label={} types={} cached_records={} missing_segments={} missing_window_segments={} elapsed_sec={:.3f}",
            address,
            label,
            ",".join(normalized_activity_types),
            len(cached_records),
            len(missing_segments),
            len(missing_window_segments),
            time.perf_counter() - started,
        )

    for window_start, window_end in missing_window_segments:
        records.extend(
            await collect_user_activity(
                client=client,
                address=address,
                start_ts=window_start,
                end_ts=window_end,
                page_limit=page_limit,
                warnings=warnings,
                activity_types=normalized_activity_types,
                log_detail=False,
                allow_range_cache=False,
            )
        )

    deduped = dedupe_activity_records(records)
    if (
        cache is not None
        and cache.is_cache_eligible(end_ts=overall_end, now_ts=now_ts)
    ):
        cache.save_range(
            user=address,
            activity_types=normalized_activity_types,
            start_ts=overall_start,
            end_ts=overall_end,
            sort_direction="ASC",
            records=deduped,
        )
    logger.info(
        "activity window group done address={} label={} types={} windows={} fetched_segments={} records={} deduped={} elapsed_sec={:.3f}",
        address,
        label,
        ",".join(normalized_activity_types),
        len(windows),
        len(missing_window_segments),
        len(records),
        len(deduped),
        time.perf_counter() - started,
    )
    return deduped


async def discover_user_markets_in_range(
    client,
    address: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    warnings: list[WarningItem],
) -> list[DiscoveredMarket]:
    records = await collect_discovery_activity_by_policy(
        client=client,
        address=address,
        start_ts=start_ts,
        end_ts=end_ts,
        page_limit=page_limit,
        warnings=warnings,
    )
    return summarize_discovered_markets(records, warnings)


async def discover_user_markets_by_day(
    client,
    address: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    warnings: list[WarningItem],
) -> list[DiscoveredMarket]:
    market_by_key: dict[str, DiscoveredMarket] = {}
    records = await collect_discovery_activity_by_policy(
        client=client,
        address=address,
        start_ts=start_ts,
        end_ts=end_ts,
        page_limit=page_limit,
        warnings=warnings,
    )
    for discovered in summarize_discovered_markets(records, warnings):
        market_key = discovered.condition_id or discovered.slug
        existing = market_by_key.get(market_key)
        if existing is None:
            market_by_key[market_key] = discovered
            continue
        existing.first_activity_ts = min(existing.first_activity_ts, discovered.first_activity_ts)
        existing.last_activity_ts = max(existing.last_activity_ts, discovered.last_activity_ts)
        if not existing.slug and discovered.slug:
            existing.slug = discovered.slug

    return sorted(
        market_by_key.values(),
        key=lambda item: (item.first_activity_ts, item.last_activity_ts, item.slug),
    )


async def collect_discovery_activity_by_policy(
    client,
    address: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    warnings: list[WarningItem],
) -> list:
    capped_page_limit = min(max(1, page_limit), DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX)
    trade_records = await collect_user_activity_for_windows(
        client=client,
        address=address,
        windows=iter_day_windows(start_ts, end_ts),
        page_limit=capped_page_limit,
        warnings=warnings,
        activity_types=("TRADE",),
        label="trade_2h",
    )
    split_redeem_records = await collect_user_activity_for_windows(
        client=client,
        address=address,
        windows=iter_calendar_day_windows(start_ts, end_ts),
        page_limit=capped_page_limit,
        warnings=warnings,
        activity_types=("SPLIT", "REDEEM"),
        label="split_redeem_1d",
    )
    return dedupe_activity_records([*trade_records, *split_redeem_records])


def summarize_discovered_markets(records: list, warnings: list[WarningItem]) -> list[DiscoveredMarket]:
    market_by_key: dict[str, DiscoveredMarket] = {}
    warned_missing_slug: set[str] = set()

    for record in records:
        if _is_zero_value_redeem(record):
            continue

        slug = str(record.slug or "").strip()
        condition_id = str(record.condition_id or "").strip()
        if not slug:
            dedupe_key = f"{condition_id}:{record.type}"
            if dedupe_key not in warned_missing_slug:
                warnings.append(
                    WarningItem(
                        timestamp=record.timestamp,
                        code="DISCOVERY_SKIP_MISSING_SLUG",
                        message="skip user activity record without market slug",
                    )
                )
                warned_missing_slug.add(dedupe_key)
            continue

        market_key = condition_id or slug
        existing = market_by_key.get(market_key)
        if existing is None:
            market_by_key[market_key] = DiscoveredMarket(
                slug=slug,
                condition_id=condition_id,
                first_activity_ts=record.timestamp,
                last_activity_ts=record.timestamp,
            )
            continue

        existing.first_activity_ts = min(existing.first_activity_ts, record.timestamp)
        existing.last_activity_ts = max(existing.last_activity_ts, record.timestamp)
        if not existing.slug and slug:
            existing.slug = slug

    return sorted(
        market_by_key.values(),
        key=lambda item: (item.first_activity_ts, item.last_activity_ts, item.slug),
    )


def _is_zero_value_redeem(record) -> bool:
    if str(getattr(record, "type", "")).upper() != "REDEEM":
        return False
    size = float(getattr(record, "size", 0) or 0)
    usdc_size = float(getattr(record, "usdc_size", 0) or 0)
    return size <= 0 and usdc_size <= 0


def dedupe_activity_records(records: list) -> list:
    deduped: dict[str, object] = {}
    for record in records:
        deduped[activity_key(record)] = record
    return sorted(
        deduped.values(),
        key=lambda item: (
            int(getattr(item, "timestamp", 0)),
            str(getattr(item, "transaction_hash", "")),
            str(getattr(item, "type", "")),
        ),
    )


def _intersect_segments_with_windows(
    segments: list[tuple[int, int]],
    windows: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    intersections: list[tuple[int, int]] = []
    for seg_start, seg_end in segments:
        if seg_start > seg_end:
            continue
        for window_start, window_end in windows:
            if window_end < seg_start or window_start > seg_end:
                continue
            overlap_start = max(seg_start, window_start)
            overlap_end = min(seg_end, window_end)
            if overlap_start <= overlap_end:
                intersections.append((overlap_start, overlap_end))
    return intersections


def activity_key(record) -> str:
    return "|".join(
        [
            str(getattr(record, "transaction_hash", "")),
            str(getattr(record, "condition_id", "")),
            str(getattr(record, "type", "")),
        ]
    )


def iter_day_windows(start_ts: int, end_ts: int) -> list[tuple[int, int]]:
    return _iter_aligned_windows(start_ts, end_ts, DISCOVERY_ACTIVITY_WINDOW_SEC)


def iter_calendar_day_windows(start_ts: int, end_ts: int) -> list[tuple[int, int]]:
    return _iter_aligned_windows(start_ts, end_ts, DAY_WINDOW_SEC)


def iter_week_windows(start_ts: int, end_ts: int) -> list[tuple[int, int]]:
    return _iter_range_windows(start_ts, end_ts, WEEK_WINDOW_SEC)


def _iter_aligned_windows(start_ts: int, end_ts: int, window_sec: int) -> list[tuple[int, int]]:
    if start_ts > end_ts:
        return []

    windows: list[tuple[int, int]] = []
    current = start_ts
    while current <= end_ts:
        window_start = (current // window_sec) * window_sec
        window_end = min(window_start + window_sec - 1, end_ts)
        windows.append((current, window_end))
        current = window_end + 1
    return windows


def _iter_range_windows(start_ts: int, end_ts: int, window_sec: int) -> list[tuple[int, int]]:
    if start_ts > end_ts:
        return []

    windows: list[tuple[int, int]] = []
    current = start_ts
    while current <= end_ts:
        window_end = min(current + window_sec, end_ts)
        windows.append((current, window_end))
        current = window_end + 1
    return windows
