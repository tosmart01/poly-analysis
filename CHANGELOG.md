# Changelog

## 2026-03-02

### Performance
- Optimize market metadata cache lookup by adding in-memory symbol payload and market object caches.
- Avoid repeated full JSON deserialization for each slug lookup in `MarketMetadataCache.get`.
- Reuse symbol payload for writes and skip disk writes when market payload is unchanged.
- Save address market result cache only when cache content changed during a run.
- Use compact JSON serialization for cache writes to reduce serialization and IO overhead.

### Tests
- Add regression test to verify market cache can serve from in-memory payload after initial load.
