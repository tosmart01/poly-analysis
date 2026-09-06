# Changelog

## 0.1.9 - 2026-09-06

- Fix sports markets being excluded when calendar-date slug suffixes were interpreted as Unix timestamps.
- Use consistent timestamp parsing for market filtering, ordering, caching, and settlement.
- Show per-outcome market rows and actual trade times in the UI and CSV exports.
- Include token fees and maker rewards in streamed results.
- Add one-day time-window navigation and remove the default keyword filter.
- Exclude generated analysis outputs from source control and release packages.

## 2026-04-28

### Fixes
- Reuse activity range cache for stale portions of windowed requests even when the requested end time is inside the recent protection window.
- Prevent recent short-range requests from forcing a later long-range request to refetch every activity window.

### Diagnostics
- Always log window-group cache checks with cached record count and missing segment counts.

## 2026-03-20

### Fixes
- Settle closed-market residual positions by resolved `outcomePrices` when no explicit `redeem` or manual close is detected.
- Count closed-market winners as realized profit and losers as realized loss even when the activity feed has no `redeem` record.
- Persist the market `closed` flag from Polymarket market metadata for settlement decisions.

### Tests
- Add regression coverage for closed-market settlement with winning, losing, and unresolved `outcomePrices` branches.

## 2026-03-02

### Performance
- Optimize market metadata cache lookup by adding in-memory symbol payload and market object caches.
- Avoid repeated full JSON deserialization for each slug lookup in `MarketMetadataCache.get`.
- Reuse symbol payload for writes and skip disk writes when market payload is unchanged.
- Save address market result cache only when cache content changed during a run.
- Use compact JSON serialization for cache writes to reduce serialization and IO overhead.

### Tests
- Add regression test to verify market cache can serve from in-memory payload after initial load.
