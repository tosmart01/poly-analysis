import asyncio

import httpx
import pytest
from pydantic import ValidationError

from analysis_poly.activity_discovery import collect_user_activity
from analysis_poly.activity_page_cache import UserActivityPageCache
from analysis_poly.models import ActivityRecord, TradeRecord
from analysis_poly.polymarket_client import PolymarketApiClient


def row(index=1):
    return {
        "transaction_hash": f"0x{index}", "timestamp": 100,
        "type": "TRADE", "condition_id": "condition", "token_id": "token",
        "side": "BUY", "size": 2, "price": 0.3, "usdc_size": 0.6,
        "slug": "btc-updown-5m-100",
    }


def envelope(rows, cursor=None):
    return {"data": rows, "pagination": {"next_cursor": cursor}}


@pytest.mark.parametrize("kind", ["trades", "activity", "discovery"])
def test_cursor_walk_continues_after_short_and_empty_pages(kind, tmp_path):
    async def run():
        calls = []
        responses = [envelope([row()], "opaque-A"), envelope([], "opaque-B"), envelope([row(2)])]

        def handle(request):
            calls.append(request)
            return httpx.Response(200, json=responses[len(calls) - 1])

        client = PolymarketApiClient()
        await client._client.aclose()
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        client._activity_page_cache = UserActivityPageCache(cache_dir=tmp_path)
        try:
            if kind == "trades":
                records = await client.get_trades("user", "condition", False, 1000)
            elif kind == "activity":
                records = await client.get_activity("user", "condition", "REDEEM", 1000)
            else:
                records = await collect_user_activity(client, "user", 10, 200, 1000, [], ("TRADE",))
            assert [r.transaction_hash for r in records] == ["0x1", "0x2"]
            assert len(calls) == 3
            first = dict(calls[0].url.params)
            assert "offset" not in first and "market" not in first
            for request, cursor in zip(calls, [None, "opaque-A", "opaque-B"]):
                params = dict(request.url.params)
                assert params.pop("cursor", None) == cursor
                assert params == first  # All filters survive every cursor hop.
                assert request.url.path == ("/v2/trades" if kind == "trades" else "/v2/activity")
            if kind == "trades":
                assert first["taker_only"] == "false"
                assert records[0].asset == "token"
            if kind != "discovery":
                assert first["condition"] == "condition" and first["start"] == "1"
            else:
                assert first["start"] == "10" and first["end"] == "200"
                assert first["sort_direction"] == "ASC"
        finally:
            await client.aclose()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["trades", "activity", "discovery"])
def test_repeated_cursor_fails_instead_of_caching_partial_results(kind, tmp_path):
    async def run():
        client = PolymarketApiClient()
        client._activity_page_cache = UserActivityPageCache(cache_dir=tmp_path)

        async def request(*args, **kwargs):
            return envelope([row()], "repeated")

        client._request_json = request
        try:
            with pytest.raises(RuntimeError, match="repeated cursor"):
                if kind == "discovery":
                    await collect_user_activity(client, "user", 10, 200, 1000, [])
                elif kind == "trades":
                    await client.get_trades("user", "condition", True)
                else:
                    await client.get_activity("user", "condition", "SPLIT")
            assert not list(tmp_path.glob("*.json"))
        finally:
            await client.aclose()
    asyncio.run(run())


def test_discovery_reads_beyond_old_10000_row_cap(tmp_path):
    async def run():
        client = PolymarketApiClient()
        client._activity_page_cache = UserActivityPageCache(cache_dir=tmp_path)
        calls = 0

        async def request(*args, **kwargs):
            nonlocal calls
            calls += 1
            return envelope([row(i) for i in range((calls - 1) * 1000, calls * 1000)], str(calls) if calls < 12 else None)

        client._request_json = request
        try:
            records = await collect_user_activity(client, "user", 100, 101, 1000, [])
            assert len(records) == 12000 and calls == 12
        finally:
            await client.aclose()
    asyncio.run(run())


def test_models_accept_v2_and_legacy_reports_without_losing_values():
    activity = ActivityRecord.model_validate(row())
    assert activity.condition_id == "condition" and activity.usdc_size == 0.6
    assert ActivityRecord.model_validate(activity.model_dump(by_alias=True)) == activity
    trade = TradeRecord.model_validate(row())
    assert trade.asset == "token" and trade.transaction_hash == "0x1"
    assert TradeRecord.model_validate(trade.model_dump(by_alias=True)) == trade


@pytest.mark.parametrize("payload", [None, [], {"data": []}, {"data": [], "pagination": {}}])
def test_invalid_envelope_is_not_a_successful_empty_result(payload, tmp_path):
    async def run():
        client = PolymarketApiClient()
        client._activity_page_cache = UserActivityPageCache(cache_dir=tmp_path)

        async def request(*args, **kwargs):
            return payload

        client._request_json = request
        try:
            with pytest.raises(ValidationError):
                await collect_user_activity(client, "user", 10, 200, 500, [])
            assert not list(tmp_path.glob("*.json"))
        finally:
            await client.aclose()
    asyncio.run(run())
