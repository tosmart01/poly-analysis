from __future__ import annotations

import json
import re
from pathlib import Path

from .models import PolymarketMarket


class MarketMetadataCache:
    def __init__(self, cache_dir: str | Path = ".cache/market_by_slug", recent_window_sec: int = 30 * 60):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._recent_window_sec = recent_window_sec

    def is_cache_eligible(self, slug: str, now_ts: int) -> bool:
        market_ts = _market_ts_from_slug(slug)
        if market_ts is None:
            return False
        return (now_ts - market_ts) > self._recent_window_sec

    def get(self, slug: str) -> PolymarketMarket | None:
        symbol = _symbol_from_slug(slug)
        if symbol is None:
            return None

        grouped_path = self._path_for_symbol(symbol)
        if grouped_path.exists():
            try:
                payload = json.loads(grouped_path.read_text(encoding="utf-8"))
                markets = payload.get("markets", {})
                market_payload = markets.get(slug)
                if market_payload is not None:
                    return PolymarketMarket.model_validate(market_payload)
            except Exception:  # noqa: BLE001
                return None

        # Backward compatibility for legacy per-slug cache files.
        legacy_path = self._legacy_path_for_slug(slug)
        if not legacy_path.exists():
            return None
        try:
            market = PolymarketMarket.model_validate_json(legacy_path.read_text(encoding="utf-8"))
            self.set(slug, market)
            return market
        except Exception:  # noqa: BLE001
            return None

    def set(self, slug: str, market: PolymarketMarket) -> None:
        symbol = _symbol_from_slug(slug)
        if symbol is None:
            return

        path = self._path_for_symbol(symbol)
        payload: dict = {"symbol": symbol, "markets": {}}
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    payload = {"symbol": symbol, "markets": {}}
                if not isinstance(payload.get("markets"), dict):
                    payload["markets"] = {}
            except Exception:  # noqa: BLE001
                payload = {"symbol": symbol, "markets": {}}

        payload["symbol"] = symbol
        payload["markets"][slug] = market.model_dump()
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(path)
        except Exception:  # noqa: BLE001
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:  # noqa: BLE001
                pass
            return

    def _path_for_symbol(self, symbol: str) -> Path:
        safe_symbol = re.sub(r"[^a-zA-Z0-9_.-]", "_", symbol.lower())
        return self._cache_dir / f"{safe_symbol}.json"

    def _legacy_path_for_slug(self, slug: str) -> Path:
        safe_slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
        return self._cache_dir / f"{safe_slug}.json"



def _market_ts_from_slug(slug: str) -> int | None:
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except Exception:  # noqa: BLE001
        return None


def _symbol_from_slug(slug: str) -> str | None:
    parts = str(slug).split("-")
    if not parts:
        return None
    symbol = parts[0].strip().lower()
    return symbol or None
