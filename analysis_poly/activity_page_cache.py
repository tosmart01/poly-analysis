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
        self._range_boundary_slack_sec = 10 * 60

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

    def load_range(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int,
        end_ts: int,
        sort_direction: str,
    ) -> tuple[list[ActivityRecord], list[tuple[int, int]]]:
        entries = self._load_matching_range_entries(
            user=user,
            activity_types=activity_types,
            sort_direction=sort_direction,
        )
        if not entries:
            return [], [(start_ts, end_ts)]

        cached_records: list[ActivityRecord] = []
        overlapped_entry_segments: list[tuple[int, int]] = []
        for entry in entries:
            overlap_start = max(start_ts, entry["start_ts"])
            overlap_end = min(end_ts, entry["end_ts"])
            if overlap_start > overlap_end:
                continue

            cached_records.extend(
                [
                    record
                    for record in entry["records"]
                    if overlap_start <= int(record.timestamp) <= overlap_end
                ]
            )
            overlapped_entry_segments.append((overlap_start, overlap_end))

        merged_entry_segments = self._merge_segments(overlapped_entry_segments)
        covered_segments = [
            (
                max(start_ts, seg_start + self._range_boundary_slack_sec),
                min(end_ts, seg_end - self._range_boundary_slack_sec),
            )
            for seg_start, seg_end in merged_entry_segments
        ]
        covered_segments = [
            (seg_start, seg_end)
            for seg_start, seg_end in covered_segments
            if seg_start <= seg_end
        ]
        missing = self._compute_missing_segments(start_ts, end_ts, covered_segments)
        return cached_records, missing

    def save_range(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int,
        end_ts: int,
        sort_direction: str,
        records: list[ActivityRecord],
    ) -> None:
        path = self._path_for_range(
            user=user,
            activity_types=activity_types,
            start_ts=start_ts,
            end_ts=end_ts,
            sort_direction=sort_direction,
        )
        payload = {
            "user": str(user).lower().strip(),
            "activity_types": self._normalize_activity_types(activity_types),
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
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

    def _path_for_range(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        start_ts: int,
        end_ts: int,
        sort_direction: str,
    ) -> Path:
        safe_user = re.sub(r"[^a-zA-Z0-9_.-]", "_", str(user).lower().strip())
        key_payload = {
            "user": safe_user,
            "activity_types": self._normalize_activity_types(activity_types),
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
            "sort_direction": str(sort_direction or "ASC").upper(),
            "kind": "range",
        }
        digest = hashlib.sha256(
            json.dumps(key_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return self._cache_dir / f"{safe_user}_range_{digest}.json"

    def _load_matching_range_entries(
        self,
        *,
        user: str,
        activity_types: list[str] | tuple[str, ...] | None,
        sort_direction: str,
    ) -> list[dict]:
        safe_user = re.sub(r"[^a-zA-Z0-9_.-]", "_", str(user).lower().strip())
        normalized_types = self._normalize_activity_types(activity_types)
        normalized_direction = str(sort_direction or "ASC").upper()
        entries: list[dict] = []
        for path in self._cache_dir.glob(f"{safe_user}_range_*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("activity_types") != normalized_types:
                    continue
                if str(payload.get("sort_direction") or "ASC").upper() != normalized_direction:
                    continue
                start_ts = int(payload["start_ts"])
                end_ts = int(payload["end_ts"])
                records = payload.get("records", [])
                if not isinstance(records, list):
                    continue
                entries.append(
                    {
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "records": [ActivityRecord.model_validate(item) for item in records],
                    }
                )
            except Exception:  # noqa: BLE001
                continue

        entries.sort(key=lambda item: (item["start_ts"], item["end_ts"]))
        return entries

    @staticmethod
    def _normalize_activity_types(activity_types: list[str] | tuple[str, ...] | None) -> list[str]:
        return [str(item).strip().upper() for item in (activity_types or []) if str(item).strip()]

    @staticmethod
    def _merge_segments(segments: list[tuple[int, int]]) -> list[tuple[int, int]]:
        if not segments:
            return []

        merged: list[list[int]] = []
        for seg_start, seg_end in sorted(segments):
            if seg_start > seg_end:
                continue
            if not merged or seg_start > merged[-1][1] + 1:
                merged.append([seg_start, seg_end])
                continue
            merged[-1][1] = max(merged[-1][1], seg_end)
        return [(seg_start, seg_end) for seg_start, seg_end in merged]

    @staticmethod
    def _compute_missing_segments(
        start_ts: int,
        end_ts: int,
        covered_segments: list[tuple[int, int]],
    ) -> list[tuple[int, int]]:
        if start_ts > end_ts:
            return []
        if not covered_segments:
            return [(start_ts, end_ts)]

        missing: list[tuple[int, int]] = []
        cursor = start_ts
        for seg_start, seg_end in UserActivityPageCache._merge_segments(covered_segments):
            if cursor < seg_start:
                missing.append((cursor, seg_start - 1))
            cursor = max(cursor, seg_end + 1)
            if cursor > end_ts:
                break
        if cursor <= end_ts:
            missing.append((cursor, end_ts))
        return missing
