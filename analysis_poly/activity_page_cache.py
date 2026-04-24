from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .models import ActivityRecord


class UserActivityPageCache:
    def __init__(
        self,
        cache_dir: str | Path = ".cache/user_activity_pages",
        recent_window_sec: int = 30 * 60,
    ):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._recent_window_sec = recent_window_sec

    def is_cache_eligible(self, end_ts: int | None, now_ts: int) -> bool:
        if end_ts is None or end_ts <= 0:
            return False
        return (now_ts - end_ts) > self._recent_window_sec

    def load(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int | None,
        end_ts: int | None,
        limit: int,
        offset: int,
        sort_direction: str,
    ) -> list[ActivityRecord] | None:
        path = self._path_for_page(
            user=user,
            activity_types=activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
            offset=offset,
            sort_direction=sort_direction,
        )
        if not path.exists():
            return None

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            records = payload.get("records", [])
            if not isinstance(records, list):
                return None
            return [ActivityRecord.model_validate(item) for item in records]
        except Exception:  # noqa: BLE001
            return None

    def save(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int | None,
        end_ts: int | None,
        limit: int,
        offset: int,
        sort_direction: str,
        records: list[ActivityRecord],
    ) -> None:
        path = self._path_for_page(
            user=user,
            activity_types=activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
            offset=offset,
            sort_direction=sort_direction,
        )
        payload = {
            "user": str(user).lower().strip(),
            "activity_types": list(activity_types or []),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": int(limit),
            "offset": int(offset),
            "sort_direction": str(sort_direction or "ASC").upper(),
            "records": [record.model_dump(by_alias=True) for record in records],
        }

        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            tmp_path.replace(path)
        except Exception:  # noqa: BLE001
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:  # noqa: BLE001
                pass

    def _path_for_page(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int | None,
        end_ts: int | None,
        limit: int,
        offset: int,
        sort_direction: str,
    ) -> Path:
        safe_user = re.sub(r"[^a-zA-Z0-9_.-]", "_", str(user).lower().strip())
        key_payload = {
            "user": safe_user,
            "activity_types": [str(item).strip().upper() for item in (activity_types or []) if str(item).strip()],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": int(limit),
            "offset": int(offset),
            "sort_direction": str(sort_direction or "ASC").upper(),
        }
        digest = hashlib.sha256(
            json.dumps(key_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return self._cache_dir / f"{safe_user}_{digest}.json"
