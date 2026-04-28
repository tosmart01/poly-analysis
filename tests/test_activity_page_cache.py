import asyncio

from analysis_poly.activity_page_cache import UserActivityPageCache
from analysis_poly.activity_discovery import collect_user_activity, collect_user_activity_for_windows
from analysis_poly.models import ActivityRecord
from analysis_poly.polymarket_client import PolymarketApiClient


def test_user_activity_page_cache_roundtrip(tmp_path):
    cache = UserActivityPageCache(cache_dir=tmp_path / "activity_cache", recent_window_sec=1800)
    records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x1",
                "timestamp": 100,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-100",
                "size": 1,
                "usdcSize": 0.5,
            }
        )
    ]

    cache.save(
        user="0xabc",
        activity_types=["TRADE", "REDEEM"],
        start_ts=10,
        end_ts=20,
        limit=500,
        offset=0,
        sort_direction="ASC",
        records=records,
    )
    loaded = cache.load(
        user="0xabc",
        activity_types=["TRADE", "REDEEM"],
        start_ts=10,
        end_ts=20,
        limit=500,
        offset=0,
        sort_direction="ASC",
    )

    assert loaded is not None
    assert len(loaded) == 1
    assert loaded[0].transaction_hash == "0x1"


def test_user_activity_page_cache_eligibility():
    cache = UserActivityPageCache(cache_dir=".cache/test_activity_page_cache", recent_window_sec=1800)

    assert cache.is_cache_eligible(end_ts=1000, now_ts=4000)
    assert not cache.is_cache_eligible(end_ts=3500, now_ts=4000)
    assert not cache.is_cache_eligible(end_ts=None, now_ts=4000)


def test_user_activity_range_cache_loads_partial_coverage(tmp_path):
    cache = UserActivityPageCache(cache_dir=tmp_path / "activity_cache", recent_window_sec=1800)
    records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x1",
                "timestamp": 12000,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-12000",
                "size": 1,
                "usdcSize": 0.5,
            }
        ),
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x2",
                "timestamp": 20000,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-20000",
                "size": 1,
                "usdcSize": 0.5,
            }
        ),
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x3",
                "timestamp": 28000,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-28000",
                "size": 1,
                "usdcSize": 0.5,
            }
        ),
    ]

    cache.save_range(
        user="0xabc",
        activity_types=["TRADE"],
        start_ts=10000,
        end_ts=30000,
        sort_direction="ASC",
        records=records,
    )

    cached_records, missing = cache.load_range(
        user="0xabc",
        activity_types=["TRADE"],
        start_ts=8000,
        end_ts=32000,
        sort_direction="ASC",
    )

    assert [record.transaction_hash for record in cached_records] == ["0x1", "0x2", "0x3"]
    assert missing == [(8000, 10599), (29401, 32000)]


def test_user_activity_range_cache_merges_adjacent_entries_before_slack(tmp_path):
    cache = UserActivityPageCache(cache_dir=tmp_path / "activity_cache", recent_window_sec=1800)

    first_records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x1",
                "timestamp": 12000,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-12000",
                "size": 1,
                "usdcSize": 0.5,
            }
        )
    ]
    second_records = [
        ActivityRecord.model_validate(
            {
                "transactionHash": "0x2",
                "timestamp": 22000,
                "type": "TRADE",
                "conditionId": "cond",
                "slug": "btc-updown-5m-22000",
                "size": 1,
                "usdcSize": 0.5,
            }
        )
    ]

    cache.save_range(
        user="0xabc",
        activity_types=["TRADE"],
        start_ts=10000,
        end_ts=19999,
        sort_direction="ASC",
        records=first_records,
    )
    cache.save_range(
        user="0xabc",
        activity_types=["TRADE"],
        start_ts=20000,
        end_ts=30000,
        sort_direction="ASC",
        records=second_records,
    )

    cached_records, missing = cache.load_range(
        user="0xabc",
        activity_types=["TRADE"],
        start_ts=8000,
        end_ts=32000,
        sort_direction="ASC",
    )

    assert [record.transaction_hash for record in cached_records] == ["0x1", "0x2"]
    assert missing == [(8000, 10599), (29401, 32000)]


def test_polymarket_client_get_user_activity_page_hits_cache_for_stale_window(monkeypatch, tmp_path):
    async def runner():
        client = PolymarketApiClient(timeout_sec=20)
        client._activity_page_cache = UserActivityPageCache(
            cache_dir=tmp_path / "activity_cache",
            recent_window_sec=1800,
        )

        call_count = 0

        async def fake_request_json(method, url, params=None):
            nonlocal call_count
            call_count += 1
            return [
                {
                    "transactionHash": "0x1",
                    "timestamp": 100,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-100",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ]

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                from datetime import datetime, timezone

                return datetime.fromtimestamp(4000, timezone.utc)

        monkeypatch.setattr("analysis_poly.polymarket_client.datetime", FakeDateTime)
        monkeypatch.setattr(client, "_request_json", fake_request_json)

        first = await client.get_user_activity_page(
            user="0xabc",
            activity_types=["TRADE"],
            start_ts=10,
            end_ts=1000,
            limit=500,
            offset=0,
            sort_direction="ASC",
        )
        second = await client.get_user_activity_page(
            user="0xabc",
            activity_types=["TRADE"],
            start_ts=10,
            end_ts=1000,
            limit=500,
            offset=0,
            sort_direction="ASC",
        )

        assert call_count == 1
        assert len(first) == 1
        assert len(second) == 1
        assert second[0].transaction_hash == "0x1"

    asyncio.run(runner())


def test_collect_user_activity_reuses_cached_range_and_fetches_only_missing_segments(tmp_path):
    async def runner():
        master_records = [
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xhead",
                    "timestamp": 9000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-9000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid1",
                    "timestamp": 12000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-12000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid2",
                    "timestamp": 20000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-20000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid3",
                    "timestamp": 28000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-28000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xtail",
                    "timestamp": 30000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-30000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
        ]

        class FakeClient:
            def __init__(self):
                self._activity_page_cache = UserActivityPageCache(
                    cache_dir=tmp_path / "activity_cache",
                    recent_window_sec=1800,
                )
                self.calls = []

            async def get_user_activity_page(
                self,
                user,
                activity_types=None,
                start_ts=None,
                end_ts=None,
                limit=500,
                offset=0,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, offset))
                if offset != 0:
                    return []
                return [
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ]

        client = FakeClient()
        warnings = []

        first = await collect_user_activity(
            client=client,
            address="0xabc",
            start_ts=10000,
            end_ts=30000,
            page_limit=500,
            warnings=warnings,
            activity_types=("TRADE",),
        )
        second = await collect_user_activity(
            client=client,
            address="0xabc",
            start_ts=8000,
            end_ts=32000,
            page_limit=500,
            warnings=warnings,
            activity_types=("TRADE",),
        )

        assert [record.transaction_hash for record in first] == ["0xmid1", "0xmid2", "0xmid3", "0xtail"]
        assert [record.transaction_hash for record in second] == ["0xhead", "0xmid1", "0xmid2", "0xmid3", "0xtail"]
        assert client.calls == [
            (("TRADE",), 10000, 30000, 0),
            (("TRADE",), 8000, 10599, 0),
            (("TRADE",), 29401, 32000, 0),
        ]

    asyncio.run(runner())


def test_collect_user_activity_for_windows_fetches_only_missing_window_segments(tmp_path):
    async def runner():
        master_records = [
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xhead",
                    "timestamp": 9500,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-9500",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid1",
                    "timestamp": 12000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-12000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid2",
                    "timestamp": 22000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-22000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xtail",
                    "timestamp": 30500,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-30500",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
        ]

        class FakeClient:
            def __init__(self):
                self._activity_page_cache = UserActivityPageCache(
                    cache_dir=tmp_path / "activity_cache",
                    recent_window_sec=1800,
                )
                self.calls = []

            async def get_user_activity_page(
                self,
                user,
                activity_types=None,
                start_ts=None,
                end_ts=None,
                limit=500,
                offset=0,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, offset))
                if offset != 0:
                    return []
                return [
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ]

        client = FakeClient()
        client._activity_page_cache.save_range(
            user="0xabc",
            activity_types=["TRADE"],
            start_ts=10000,
            end_ts=30000,
            sort_direction="ASC",
            records=[record for record in master_records if 10000 <= int(record.timestamp) <= 30000],
        )

        records = await collect_user_activity_for_windows(
            client=client,
            address="0xabc",
            windows=[(8000, 15999), (16000, 23999), (24000, 32000)],
            page_limit=500,
            warnings=[],
            activity_types=("TRADE",),
            label="trade_2h",
        )

        assert [record.transaction_hash for record in records] == ["0xhead", "0xmid1", "0xmid2", "0xtail"]
        assert client.calls == [
            (("TRADE",), 8000, 10599, 0),
            (("TRADE",), 29401, 32000, 0),
        ]

    asyncio.run(runner())


def test_collect_user_activity_for_windows_reuses_stale_cache_when_overall_end_is_recent(monkeypatch, tmp_path):
    async def runner():
        master_records = [
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xhead",
                    "timestamp": 9500,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-9500",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xmid",
                    "timestamp": 22000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-22000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
            ActivityRecord.model_validate(
                {
                    "transactionHash": "0xtail",
                    "timestamp": 39000,
                    "type": "TRADE",
                    "conditionId": "cond",
                    "slug": "btc-updown-5m-39000",
                    "size": 1,
                    "usdcSize": 0.5,
                }
            ),
        ]

        class FakeClient:
            def __init__(self):
                self._activity_page_cache = UserActivityPageCache(
                    cache_dir=tmp_path / "activity_cache",
                    recent_window_sec=1800,
                )
                self.calls = []

            async def get_user_activity_page(
                self,
                user,
                activity_types=None,
                start_ts=None,
                end_ts=None,
                limit=500,
                offset=0,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, offset))
                if offset != 0:
                    return []
                return [
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ]

        client = FakeClient()
        client._activity_page_cache.save_range(
            user="0xabc",
            activity_types=["TRADE"],
            start_ts=10000,
            end_ts=30000,
            sort_direction="ASC",
            records=[record for record in master_records if 10000 <= int(record.timestamp) <= 30000],
        )

        monkeypatch.setattr("analysis_poly.activity_discovery.time.time", lambda: 40000)
        records = await collect_user_activity_for_windows(
            client=client,
            address="0xabc",
            windows=[(8000, 15999), (16000, 23999), (24000, 39500)],
            page_limit=500,
            warnings=[],
            activity_types=("TRADE",),
            label="trade_2h",
        )

        assert [record.transaction_hash for record in records] == ["0xhead", "0xmid", "0xtail"]
        assert client.calls == [
            (("TRADE",), 8000, 10599, 0),
            (("TRADE",), 29401, 39500, 0),
        ]

    asyncio.run(runner())
