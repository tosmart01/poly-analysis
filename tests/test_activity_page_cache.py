import asyncio

from analysis_poly.activity_page_cache import UserActivityPageCache
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
