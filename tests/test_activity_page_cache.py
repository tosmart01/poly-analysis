from analysis_poly.models import DataApiPage, DataApiPagination
import asyncio

from analysis_poly.activity_page_cache import UserActivityPageCache
from analysis_poly.activity_discovery import collect_user_activity, collect_user_activity_for_windows
from analysis_poly.models import ActivityRecord


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
                cursor=None,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, cursor))
                if cursor is not None:
                    return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
                return DataApiPage(data=[
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ], pagination=DataApiPagination(next_cursor=None))

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
            (("TRADE",), 10000, 30000, None),
            (("TRADE",), 8000, 10599, None),
            (("TRADE",), 29401, 32000, None),
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
                cursor=None,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, cursor))
                if cursor is not None:
                    return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
                return DataApiPage(data=[
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ], pagination=DataApiPagination(next_cursor=None))

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
            (("TRADE",), 8000, 10599, None),
            (("TRADE",), 29401, 32000, None),
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
                cursor=None,
                sort_direction="ASC",
            ):
                self.calls.append((tuple(activity_types or []), start_ts, end_ts, cursor))
                if cursor is not None:
                    return DataApiPage(data=[], pagination=DataApiPagination(next_cursor=None))
                return DataApiPage(data=[
                    record
                    for record in master_records
                    if start_ts <= int(record.timestamp) <= end_ts
                ], pagination=DataApiPagination(next_cursor=None))

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
            (("TRADE",), 8000, 10599, None),
            (("TRADE",), 29401, 39500, None),
        ]

    asyncio.run(runner())
