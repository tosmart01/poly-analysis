import pytest

from analysis_poly.activity_discovery import DiscoveredMarket
from analysis_poly.analyzer import _filter_discovered_markets, _is_market_result_cache_eligible
from analysis_poly.market_cache import MarketMetadataCache
from analysis_poly.slugs import market_timestamp_from_slug


@pytest.mark.parametrize('slug', [
    'lol-ig1-we-2026-09-06',
    'cs2-ts7-fal2-2026-09-05',
    'cs2-fal2-g2-2026-09-04-game2',
    'some-market-2026',
])
def test_sports_market_is_not_filtered_or_cached_as_epoch(slug, tmp_path):
    assert market_timestamp_from_slug(slug) is None
    market = DiscoveredMarket(slug, 'condition', 1788665769, 1788665769)
    assert _filter_discovered_markets([market], 1788624000, 1788710340, []) == [market]
    assert not _is_market_result_cache_eligible(slug, 1788690000, 1800)
    assert not MarketMetadataCache(tmp_path).is_cache_eligible(slug, 1788690000)


def test_timestamp_markets_keep_time_range_filter():
    slug = 'btc-updown-5m-1788665700'
    assert market_timestamp_from_slug(slug) == 1788665700
    assert market_timestamp_from_slug('btc-updown-5m-1000') == 1000
    market = DiscoveredMarket(slug, 'condition', 1788665769, 1788665769)
    assert _filter_discovered_markets([market], 1788624000, 1788710340, []) == [market]
    assert _filter_discovered_markets([market], 1788665800, 1788710340, []) == []
