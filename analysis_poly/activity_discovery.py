from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .models import WarningItem

DISCOVERY_ACTIVITY_TYPES = ("TRADE", "SPLIT", "REDEEM")
DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX = 500
DISCOVERY_ACTIVITY_OFFSET_MAX = 10000
DISCOVERY_ACTIVITY_WINDOW_SEC = 2 * 60 * 60


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
) -> list:
    records: list = []
    offset = 0
    reached_offset_cap = False

    while True:
        page = await client.get_user_activity_page(
            user=address,
            activity_types=list(activity_types or DISCOVERY_ACTIVITY_TYPES),
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
        return dedupe_activity_records(records)

    if end_ts - start_ts <= 1:
        warnings.append(
            WarningItem(
                timestamp=start_ts,
                code="DISCOVERY_WINDOW_TRUNCATED",
                message="user activity window is too dense to split further; discovery may be incomplete",
            )
        )
        return dedupe_activity_records(records)

    split_ts = start_ts + ((end_ts - start_ts) // 2)
    left_records, right_records = await asyncio.gather(
        collect_user_activity(
            client=client,
            address=address,
            start_ts=start_ts,
            end_ts=split_ts,
            page_limit=page_limit,
            warnings=warnings,
            activity_types=activity_types,
        ),
        collect_user_activity(
            client=client,
            address=address,
            start_ts=split_ts + 1,
            end_ts=end_ts,
            page_limit=page_limit,
            warnings=warnings,
            activity_types=activity_types,
        ),
    )
    return dedupe_activity_records([*left_records, *right_records])


async def discover_user_markets_in_range(
    client,
    address: str,
    start_ts: int,
    end_ts: int,
    page_limit: int,
    warnings: list[WarningItem],
) -> list[DiscoveredMarket]:
    records = await collect_user_activity(
        client=client,
        address=address,
        start_ts=start_ts,
        end_ts=end_ts,
        page_limit=min(max(1, page_limit), DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX),
        warnings=warnings,
        activity_types=DISCOVERY_ACTIVITY_TYPES,
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
    for window_start, window_end in iter_day_windows(start_ts, end_ts):
        day_records = await collect_user_activity(
            client=client,
            address=address,
            start_ts=window_start,
            end_ts=window_end,
            page_limit=min(max(1, page_limit), DISCOVERY_ACTIVITY_PAGE_LIMIT_MAX),
            warnings=warnings,
            activity_types=DISCOVERY_ACTIVITY_TYPES,
        )
        for discovered in summarize_discovered_markets(day_records, warnings):
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


def activity_key(record) -> str:
    return "|".join(
        [
            str(getattr(record, "transaction_hash", "")),
            str(getattr(record, "condition_id", "")),
            str(getattr(record, "type", "")),
            str(getattr(record, "timestamp", 0)),
            str(getattr(record, "slug", "")),
            str(getattr(record, "size", 0)),
            str(getattr(record, "usdc_size", 0)),
        ]
    )


def iter_day_windows(start_ts: int, end_ts: int) -> list[tuple[int, int]]:
    if start_ts > end_ts:
        return []

    windows: list[tuple[int, int]] = []
    current = start_ts
    while current <= end_ts:
        window_start = (current // DISCOVERY_ACTIVITY_WINDOW_SEC) * DISCOVERY_ACTIVITY_WINDOW_SEC
        window_end = min(window_start + DISCOVERY_ACTIVITY_WINDOW_SEC - 1, end_ts)
        windows.append((current, window_end))
        current = window_end + 1
    return windows
