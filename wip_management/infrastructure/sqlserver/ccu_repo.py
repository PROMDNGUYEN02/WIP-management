# wip_management/infrastructure/sqlserver/ccu_repo.py
from __future__ import annotations

import asyncio
import atexit
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TrayId, TraySignal, Watermark
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Fast JSON parsing - try orjson first
# ─────────────────────────────────────────────────────────────────────────────
try:
    import orjson

    def _fast_json_loads(s: str | bytes) -> dict:
        if isinstance(s, str):
            s = s.encode("utf-8")
        return orjson.loads(s)

    _JSON_BACKEND = "orjson"
except ImportError:
    def _fast_json_loads(s: str | bytes) -> dict:
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        return json.loads(s)

    _JSON_BACKEND = "stdlib"

log.debug("JSON backend: %s", _JSON_BACKEND)


# ─────────────────────────────────────────────────────────────────────────────
# Thread Pool Management
# ─────────────────────────────────────────────────────────────────────────────
_PARSE_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_LOCK = threading.Lock()


def _get_parse_executor() -> ThreadPoolExecutor:
    global _PARSE_EXECUTOR
    if _PARSE_EXECUTOR is None:
        with _EXECUTOR_LOCK:
            if _PARSE_EXECUTOR is None:
                _PARSE_EXECUTOR = ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix="ccu-parse",
                )
                atexit.register(_shutdown_executor)
    return _PARSE_EXECUTOR


def _shutdown_executor() -> None:
    global _PARSE_EXECUTOR
    if _PARSE_EXECUTOR is not None:
        _PARSE_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        _PARSE_EXECUTOR = None


# ─────────────────────────────────────────────────────────────────────────────
# LRU Cache with TTL
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class _CacheEntry:
    data: Any
    created_at: float
    access_count: int = 0
    last_access: float = field(default_factory=time.monotonic)


class TTLCache:
    """Thread-safe LRU cache with TTL expiration."""

    def __init__(
        self,
        maxsize: int = 1000,
        ttl_seconds: float = 600.0,
        *,
        name: str = "cache",
    ) -> None:
        self._maxsize = max(1, maxsize)
        self._ttl = ttl_seconds
        self._name = name
        self._cache: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            now = time.monotonic()
            if self._ttl > 0 and (now - entry.created_at) > self._ttl:
                del self._cache[key]
                self._misses += 1
                return None
            self._cache.move_to_end(key)
            entry.access_count += 1
            entry.last_access = now
            self._hits += 1
            return entry.data

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            now = time.monotonic()
            self._evict_expired(now)
            while len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)
            self._cache[key] = _CacheEntry(data=value, created_at=now)

    def _evict_expired(self, now: float) -> None:
        if self._ttl <= 0:
            return
        expired = [
            k for k, v in self._cache.items()
            if (now - v.created_at) > self._ttl
        ]
        for k in expired:
            del self._cache[k]

    def invalidate(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def invalidate_prefix(self, prefix: str) -> int:
        with self._lock:
            to_remove = [k for k in self._cache if k.startswith(prefix)]
            for k in to_remove:
                del self._cache[k]
            return len(to_remove)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            return {
                "name": self._name,
                "size": len(self._cache),
                "maxsize": self._maxsize,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": f"{hit_rate:.1f}%",
            }


# ─────────────────────────────────────────────────────────────────────────────
# Disk Cache
# ─────────────────────────────────────────────────────────────────────────────
class DiskCache:
    """Simple disk-based cache for large results that survive restarts."""

    def __init__(
        self,
        cache_dir: str | Path | None = None,
        max_entries: int = 500,
        ttl_hours: float = 24.0,
    ) -> None:
        if cache_dir is None:
            cache_dir = Path(tempfile.gettempdir()) / "wip_cache"
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._max_entries = max_entries
        self._ttl_seconds = ttl_hours * 3600
        self._index_file = self._cache_dir / "_index.json"
        self._index: dict[str, dict] = self._load_index()
        self._lock = threading.Lock()

    def _load_index(self) -> dict[str, dict]:
        try:
            if self._index_file.exists():
                with open(self._index_file, "r") as f:
                    return json.load(f)
        except Exception:
            log.debug("Disk cache index load failed, starting fresh")
        return {}

    def _save_index(self) -> None:
        tmp_path = self._index_file.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._index, f, ensure_ascii=False)
            os.replace(tmp_path, self._index_file)
        except Exception:
            log.warning("Disk cache index save failed")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _key_to_filename(self, key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()[:32] + ".json"

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._index.get(key)
            if entry is None:
                return None
            created_at = entry.get("created_at", 0)
            if time.time() - created_at > self._ttl_seconds:
                self._remove_entry(key)
                return None
            filename = entry.get("filename", "")
            filepath = self._cache_dir / filename
            try:
                if filepath.exists():
                    with open(filepath, "r", encoding="utf-8") as f:
                        return json.load(f)
            except Exception:
                log.debug("Disk cache read failed key=%s", key)
                self._remove_entry(key)
            return None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._evict_if_needed()
            filename = self._key_to_filename(key)
            filepath = self._cache_dir / filename
            tmp_path = filepath.with_suffix(".tmp")
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(value, f, default=str, ensure_ascii=False)
                os.replace(tmp_path, filepath)
                self._index[key] = {
                    "filename": filename,
                    "created_at": time.time(),
                    "size": filepath.stat().st_size,
                }
                self._save_index()
            except Exception:
                log.warning("Disk cache write failed key=%s", key)
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass

    def _remove_entry(self, key: str) -> None:
        entry = self._index.pop(key, None)
        if entry:
            filepath = self._cache_dir / entry.get("filename", "")
            try:
                if filepath.exists():
                    filepath.unlink()
            except Exception:
                pass
            self._save_index()

    def _evict_if_needed(self) -> None:
        now = time.time()
        expired = [
            k for k, v in self._index.items()
            if now - v.get("created_at", 0) > self._ttl_seconds
        ]
        for k in expired:
            self._remove_entry(k)
        while len(self._index) >= self._max_entries:
            oldest_key = min(
                self._index.keys(),
                key=lambda k: self._index[k].get("created_at", 0),
            )
            self._remove_entry(oldest_key)

    def clear(self) -> None:
        with self._lock:
            for entry in self._index.values():
                filepath = self._cache_dir / entry.get("filename", "")
                try:
                    if filepath.exists():
                        filepath.unlink()
                except Exception:
                    pass
            self._index.clear()
            self._save_index()


# ─────────────────────────────────────────────────────────────────────────────
# JSON helpers
# ─────────────────────────────────────────────────────────────────────────────
_BAD_PREFIXES = frozenset((
    "FIELDNAME", "PARAMNAME", "MAXVALUE", "WIPID",
    "RECORDTIME", "CREATEUSER", "CREATEDATE", "STDVALUE",
))


def _parse_record_json(raw: str) -> dict[str, str]:
    """
    Parse RECORD_JSON with optimized fast path.
    Structure: {"TRAYID": {"paramValue": "VB050000435", ...}, ...}
    """
    if not raw or len(raw) < 10:
        return {}
    try:
        outer = _fast_json_loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(outer, dict):
        return {}
    result: dict[str, str] = {}
    for field_name, field_obj in outer.items():
        if isinstance(field_obj, dict):
            v = field_obj.get("paramValue")
            if v is not None:
                result[field_name] = str(v)
        elif isinstance(field_obj, (str, int, float)):
            result[field_name] = str(field_obj)
    return result


def _is_valid_tray_id(value: str) -> bool:
    if not value:
        return False
    length = len(value)
    if length < 6 or length > 40:
        return False
    has_digit = False
    for c in value:
        if c.isdigit():
            has_digit = True
            break
    if not has_digit:
        return False
    upper_value = value.upper()
    for prefix in _BAD_PREFIXES:
        if upper_value.startswith(prefix):
            return False
    return True


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    text = str(value).strip()
    if not text or len(text) < 10:
        return None
    try:
        normalized = text.replace("Z", "").replace("T", " ")
        if "." in normalized:
            normalized = normalized[:26]
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:26], fmt)
        except ValueError:
            continue
    return None


def _chunked(values: list, chunk_size: int) -> list[list]:
    if chunk_size <= 0:
        return [values]
    return [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]


# ─────────────────────────────────────────────────────────────────────────────
# Core Parse (runs in thread pool)
# ─────────────────────────────────────────────────────────────────────────────
def _parse_ccu_rows_sync(
    rows: list[dict[str, Any]],
    tray_key: str,
    pos_key: str,
    keyword: str,
) -> list[TraySignal]:
    per_tray: dict[str, dict[str, Any]] = {}
    skipped = parsed = 0
    keyword_upper = keyword.upper() if keyword else ""

    for row in rows:
        start_time = _coerce_datetime(row.get("start_time"))
        end_time = _coerce_datetime(row.get("end_time"))
        collected = end_time or start_time
        if collected is None:
            skipped += 1
            continue

        raw_json = row.get("record_json")
        if not raw_json:
            skipped += 1
            continue

        raw_text = str(raw_json)
        if keyword_upper and keyword_upper not in raw_text.upper():
            skipped += 1
            continue

        fields = _parse_record_json(raw_text)
        if not fields:
            skipped += 1
            continue

        raw_tray = fields.get(tray_key, "").strip()
        if not raw_tray:
            raw_tray = fields.get(tray_key.upper(), "").strip()
        tray_id = raw_tray.upper()
        if not _is_valid_tray_id(tray_id):
            skipped += 1
            continue

        raw_pos = fields.get(pos_key, "") or fields.get(pos_key.upper(), "")
        pos_value: int | None = None
        if raw_pos:
            try:
                pos_value = int(float(raw_pos))
            except (ValueError, TypeError):
                pass

        lot_no = (
            fields.get("LOT_NO", "").strip()
            or str(row.get("lot_no") or "").strip()
        )

        if tray_id not in per_tray:
            per_tray[tray_id] = {
                "seen_positions": set(),
                "lot_nos": set(),
                "latest_lot": None,
                "min_pos": None,
                "max_pos": None,
                "first_start": None,
                "last_end": None,
                "latest_time": collected,
            }

        stat = per_tray[tray_id]

        if pos_value is not None:
            stat["seen_positions"].add(pos_value)
            if stat["min_pos"] is None or pos_value < stat["min_pos"]:
                stat["min_pos"] = pos_value
            if stat["max_pos"] is None or pos_value > stat["max_pos"]:
                stat["max_pos"] = pos_value

        if lot_no:
            stat["lot_nos"].add(lot_no)
            stat["latest_lot"] = lot_no

        if start_time and (
            stat["first_start"] is None or start_time < stat["first_start"]
        ):
            stat["first_start"] = start_time
        if end_time and (
            stat["last_end"] is None or end_time > stat["last_end"]
        ):
            stat["last_end"] = end_time
        if collected > stat["latest_time"]:
            stat["latest_time"] = collected

        parsed += 1

    out: list[TraySignal] = []
    for tray_id, stat in per_tray.items():
        quantity = (
            len(stat["seen_positions"])
            if stat["seen_positions"]
            else len(stat["lot_nos"])
        )
        payload: dict[str, Any] = {
            "lot_no": stat["latest_lot"],
            "quantity": quantity,
            "lot_count": len(stat["lot_nos"]),
            "start_time": (
                stat["first_start"].isoformat() if stat["first_start"] else None
            ),
            "end_time": (
                stat["last_end"].isoformat() if stat["last_end"] else None
            ),
            "min_position": stat["min_pos"],
            "max_position": stat["max_pos"],
        }
        out.append(TraySignal(
            source=SignalSource.CCU,
            tray_id=TrayId(tray_id),
            collected_time=stat["latest_time"],
            payload=payload,
        ))

    out.sort(key=lambda s: (s.collected_time, str(s.tray_id)))

    if rows and skipped == len(rows):
        log.error(
            "CCU parse: ALL %s rows skipped! tray_key='%s' pos_key='%s'",
            len(rows), tray_key, pos_key,
        )

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Lookup Strategy Enum
# ─────────────────────────────────────────────────────────────────────────────
class TrayLookupStrategy:
    """Determines how to look up tray IDs in the CCU table."""
    JSON_VALUE = "json_value"  # Use JSON_VALUE() — precise, SQL Server 2016+
    LIKE_SCAN = "like_scan"    # Use RECORD_JSON LIKE '%tray_id%' — slow fallback
    INDEXED_COLUMN = "indexed_column"  # Use pre-computed indexed column


# ─────────────────────────────────────────────────────────────────────────────
# Repository
# ─────────────────────────────────────────────────────────────────────────────
class CcuRepo:
    """
    Optimized CCU Repository:
    - L1: In-memory TTL cache (hot data)
    - L2: Disk cache (persistence across restarts)
    - JSON_VALUE() extraction for tray ID lookups (SQL Server 2016+)
    - Falls back to LIKE scan only if explicitly configured

    IMPORTANT: MANUFACTURE_CODE contains machine codes (e.g. 'RMP1.06092ASY01'),
    NOT tray IDs. Tray IDs are stored in RECORD_JSON under $.TRAYID.paramValue.
    """

    def __init__(self, connection: SQLServerConnection) -> None:
        self._conn = connection
        self._table = SQLServerConnection.table_name(
            settings.sql_schema, settings.ccu_table
        )
        self._lot_col = SQLServerConnection.column_name(settings.ccu_lot_column)
        self._start_col = SQLServerConnection.column_name(settings.ccu_start_column)
        self._end_col = SQLServerConnection.column_name(settings.ccu_end_column)
        self._update_col = SQLServerConnection.column_name(settings.update_time_column)
        self._record_col = SQLServerConnection.column_name(
            settings.ccu_record_json_column
        )
        self._tray_key = settings.ccu_json_tray_id_key
        self._pos_key = settings.ccu_json_pos_key
        self._keyword = settings.record_filter_keyword.strip().upper()

        # JSON path for SQL Server JSON_VALUE()
        self._json_tray_path = getattr(
            settings, "ccu_json_tray_id_path", "$.TRAYID.paramValue"
        )

        # Resolve lookup strategy
        self._lookup_strategy: str
        self._tray_id_col_quoted: str | None = None

        _raw_col = getattr(settings, "ccu_tray_id_db_column", "").strip()

        if not _raw_col:
            # Default: Use JSON_VALUE() for precise extraction
            self._lookup_strategy = TrayLookupStrategy.JSON_VALUE
            log.info(
                "CCU repo: using JSON_VALUE('%s') for tray ID extraction "
                "(MANUFACTURE_CODE contains machine codes, not tray IDs)",
                self._json_tray_path,
            )
        elif _raw_col.upper() == "LIKE":
            # Explicit LIKE fallback
            self._lookup_strategy = TrayLookupStrategy.LIKE_SCAN
            log.info(
                "CCU repo: using RECORD_JSON LIKE scan for tray ID lookup "
                "(slow, consider JSON_VALUE or computed column)"
            )
        else:
            # Assume it's an indexed column name (computed column)
            try:
                self._tray_id_col_quoted = SQLServerConnection.column_name(_raw_col)
                self._lookup_strategy = TrayLookupStrategy.INDEXED_COLUMN
                log.info(
                    "CCU repo: using indexed column %s for tray ID lookup",
                    _raw_col,
                )
            except ValueError:
                log.warning(
                    "CCU repo: invalid ccu_tray_id_db_column='%s', "
                    "falling back to JSON_VALUE",
                    _raw_col,
                )
                self._lookup_strategy = TrayLookupStrategy.JSON_VALUE

        # L1: Memory cache
        self._tray_cells_cache = TTLCache(
            maxsize=int(getattr(settings, "tray_detail_cache_max_entries", 500)),
            ttl_seconds=float(
                getattr(settings, "tray_detail_cache_ttl_seconds", 300)
            ),
            name="tray_cells",
        )

        # L2: Disk cache
        cache_dir = Path(getattr(settings, "local_cache_dir", "")) or None
        self._disk_cache = DiskCache(
            cache_dir=cache_dir,
            max_entries=2000,
            ttl_hours=24.0,
        )

        # Cell owner cache
        self._cell_owner_cache = TTLCache(
            maxsize=1000,
            ttl_seconds=600.0,
            name="cell_owner",
        )

        # Known tray data from initial/delta loads
        self._known_tray_data: dict[str, dict[str, Any]] = {}
        self._known_tray_lock = threading.Lock()

    async def start(self) -> None:
        await self._conn.start()

    async def close(self) -> None:
        await self._conn.close()
        self._tray_cells_cache.clear()
        self._cell_owner_cache.clear()
        log.info(
            "CCU repo closed cache_stats=%s",
            self._tray_cells_cache.stats(),
        )

    def register_known_tray(self, tray_id: str, data: dict[str, Any]) -> None:
        with self._known_tray_lock:
            self._known_tray_data[tray_id.upper()] = data

    def get_known_tray(self, tray_id: str) -> dict[str, Any] | None:
        with self._known_tray_lock:
            return self._known_tray_data.get(tray_id.upper())

    async def fetch_initial(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[TraySignal]:
        log.info(
            "CCU initial fetch start start_time=%s end_time=%s batch_size=%s",
            start_time.isoformat(),
            end_time.isoformat(),
            settings.max_fetch_batch,
        )
        t0 = time.perf_counter()
        rows = await self._fetch_range(start_time, end_time)
        signals = await self._parse_rows_parallel(rows)
        for signal in signals:
            self.register_known_tray(str(signal.tray_id), signal.payload)
        log.info(
            "CCU initial fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            int((time.perf_counter() - t0) * 1000),
            len(rows),
            len(signals),
        )
        return signals

    async def fetch_delta(
        self,
        watermark: Watermark,
        end_time: datetime,
    ) -> list[TraySignal]:
        overlap_start = watermark.collected_time - timedelta(
            seconds=settings.delta_overlap_seconds
        )
        t0 = time.perf_counter()
        rows = await self._fetch_range(overlap_start, end_time)
        signals = await self._parse_rows_parallel(rows)
        for signal in signals:
            self.register_known_tray(str(signal.tray_id), signal.payload)
        log.debug(
            "CCU delta fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            int((time.perf_counter() - t0) * 1000),
            len(rows),
            len(signals),
        )
        return signals

    async def fetch_by_tray_ids(
        self,
        tray_ids: Iterable[str],
        end_time: datetime,
        *,
        allow_historical_scan: bool = True,
        allow_targeted_lookup: bool = True,
    ) -> list[TraySignal]:
        wanted = {t.strip().upper() for t in tray_ids if t and t.strip()}
        if not wanted:
            return []

        log.info("CCU backfill fetch start tray_count=%s", len(wanted))
        t0 = time.perf_counter()
        lookback_start = end_time - timedelta(
            hours=max(1.0, float(settings.ccu_backfill_lookback_hours))
        )

        all_rows: list[dict[str, Any]] = []
        for batch in _chunked(sorted(wanted), 50):
            all_rows.extend(
                await self._fetch_by_tray_ids_batch(batch, lookback_start, end_time)
            )

        signals = await self._parse_rows_parallel(all_rows)
        result = [s for s in signals if str(s.tray_id).upper() in wanted]

        for signal in result:
            self.register_known_tray(str(signal.tray_id), signal.payload)

        log.info(
            "CCU backfill fetch done elapsed_ms=%s requested=%s matched=%s strategy=%s",
            int((time.perf_counter() - t0) * 1000),
            len(wanted),
            len(result),
            self._lookup_strategy,
        )
        return result

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        query = f"""
        SELECT TOP 1 COALESCE({self._end_col}, {self._start_col}) AS latest_time
        FROM {self._table} WITH (NOLOCK)
        WHERE {self._update_col} <= ?
          AND ({self._end_col} IS NOT NULL OR {self._start_col} IS NOT NULL)
        ORDER BY {self._update_col} DESC
        """
        rows = await self._conn.query_rows(
            query, [end_time], query_name="ccu.peek_latest"
        )
        return _coerce_datetime(rows[0].get("latest_time")) if rows else None

    async def fetch_tray_cells(
        self,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, str | None]]:
        """
        Fetch tray cells with multi-tier caching.
        Uses JSON_VALUE() to extract tray ID from RECORD_JSON (correct approach),
        NOT MANUFACTURE_CODE which contains machine codes.
        """
        wanted = tray_id.strip().upper()
        if not wanted:
            return []

        date_key = end_time.strftime("%Y%m%d")
        memory_cache_key = f"{wanted}:{date_key}"
        disk_cache_key = f"tray_cells:{wanted}:{date_key}"

        # L1: Memory cache
        cached = self._tray_cells_cache.get(memory_cache_key)
        if cached is not None:
            log.debug("Tray cells L1 cache hit tray_id=%s", wanted)
            return list(cached)

        # L2: Disk cache
        disk_cached = self._disk_cache.get(disk_cache_key)
        if disk_cached is not None:
            log.debug("Tray cells L2 cache hit tray_id=%s", wanted)
            self._tray_cells_cache.set(memory_cache_key, disk_cached)
            return list(disk_cached)

        # L3: Tiered DB query
        t0 = time.perf_counter()
        result = await self._fetch_tray_cells_tiered(wanted, start_time, end_time)
        elapsed_ms = int((time.perf_counter() - t0) * 1000)

        log.info(
            "Tray cells fetched tray_id=%s rows=%s elapsed_ms=%s strategy=%s",
            wanted,
            len(result),
            elapsed_ms,
            self._lookup_strategy,
        )

        if result:
            self._tray_cells_cache.set(memory_cache_key, result)
            self._disk_cache.set(disk_cache_key, result)

        return result

    async def _fetch_tray_cells_tiered(
        self,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, str | None]]:
        """
        Tiered time window strategy:
        Tier 1: last 2h  (fastest, most likely to contain today's data)
        Tier 2: last 8h  (wider window if tier 1 has <10 results)
        Tier 3: full window (fallback)
        """
        narrow_start_1 = end_time - timedelta(hours=2)
        if narrow_start_1 >= start_time:
            result = await self._fetch_tray_cells_query(
                tray_id, narrow_start_1, end_time, limit=200
            )
            if len(result) >= 10:
                return result

        narrow_start_2 = end_time - timedelta(hours=8)
        if narrow_start_2 >= start_time:
            result = await self._fetch_tray_cells_query(
                tray_id, narrow_start_2, end_time, limit=500
            )
            if result:
                return result

        return await self._fetch_tray_cells_query(
            tray_id, start_time, end_time, limit=1000
        )

    async def _fetch_tray_cells_query(
        self,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 500,
    ) -> list[dict[str, str | None]]:
        """
        Execute tray-cells query using the configured lookup strategy.

        JSON_VALUE (default, recommended):
          WHERE JSON_VALUE(RECORD_JSON, '$.TRAYID.paramValue') = 'VB050001008'
          Precise extraction, works on SQL Server 2016+.

        INDEXED_COLUMN (if computed column exists):
          WHERE TRAY_ID = 'VB050001008'
          Fastest, requires DBA to add computed column.

        LIKE_SCAN (legacy fallback):
          WHERE RECORD_JSON LIKE '%VB050001008%'
          Slow (~8-24s), may match false positives.
        """
        if self._lookup_strategy == TrayLookupStrategy.JSON_VALUE:
            # ── JSON_VALUE extraction (precise, no computed column needed) ────
            query = f"""
            SELECT TOP {limit}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND JSON_VALUE({self._record_col}, '{self._json_tray_path}') = ?
            ORDER BY {self._end_col} DESC
            """
            params: list[Any] = [start_time, end_time, tray_id]

        elif self._lookup_strategy == TrayLookupStrategy.INDEXED_COLUMN:
            # ── Indexed computed column (fastest) ─────────────────────────────
            query = f"""
            SELECT TOP {limit}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND {self._tray_id_col_quoted} = ?
            ORDER BY {self._end_col} DESC
            """
            params = [start_time, end_time, tray_id]

        else:
            # ── LIKE scan fallback (slow) ─────────────────────────────────────
            query = f"""
            SELECT TOP {limit}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND {self._record_col} LIKE ?
            ORDER BY {self._end_col} DESC
            """
            params = [start_time, end_time, f"%{tray_id}%"]

        rows = await self._conn.query_rows(
            query, params, query_name="ccu.fetch_tray_cells"
        )
        return self._parse_tray_cells_rows(rows, tray_id)

    def _parse_tray_cells_rows(
        self,
        rows: list[dict[str, Any]],
        target_tray_id: str,
    ) -> list[dict[str, str | None]]:
        out: list[dict[str, str | None]] = []
        seen: set[str] = set()
        target_upper = target_tray_id.upper()

        for row in rows:
            fields = _parse_record_json(str(row.get("record_json") or ""))

            # Verify this row belongs to our target tray
            tray_in_record = fields.get(self._tray_key, "").strip().upper()
            if tray_in_record != target_upper:
                continue

            lot = (
                fields.get("LOT_NO", "").strip()
                or str(row.get("lot_no") or "").strip()
            )
            pos_raw = fields.get(self._pos_key, "")
            pos: int | None = None
            if pos_raw:
                try:
                    pos = int(float(pos_raw))
                except (ValueError, TypeError):
                    pass

            key = f"{lot}|{pos}"
            if key in seen:
                continue
            seen.add(key)

            start_dt = _coerce_datetime(row.get("start_time"))
            end_dt = _coerce_datetime(row.get("end_time"))

            out.append({
                "cell_id": lot,
                "position": str(pos) if pos is not None else None,
                "start_time": (
                    start_dt.isoformat(sep=" ", timespec="seconds")
                    if start_dt else None
                ),
                "end_time": (
                    end_dt.isoformat(sep=" ", timespec="seconds")
                    if end_dt else None
                ),
            })

        return out

    async def fetch_cell_owner(
        self,
        cell_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, str | None] | None:
        wanted = cell_id.strip().upper()
        if not wanted:
            return None

        cache_key = f"{wanted}:{end_time.strftime('%Y%m%d')}"
        cached = self._cell_owner_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        query = f"""
        SELECT TOP 1
            {self._lot_col}    AS lot_no,
            {self._start_col}  AS start_time,
            {self._end_col}    AS end_time,
            {self._record_col} AS record_json
        FROM {self._table} WITH (NOLOCK)
        WHERE {self._update_col} >= ?
          AND {self._update_col} <= ?
          AND UPPER({self._lot_col}) = ?
        ORDER BY COALESCE({self._end_col}, {self._start_col}) DESC
        """
        rows = await self._conn.query_rows(
            query,
            [start_time, end_time, wanted],
            query_name="ccu.fetch_cell_owner",
        )
        if not rows:
            return None

        row = rows[0]
        fields = _parse_record_json(str(row.get("record_json") or ""))
        tray_raw = fields.get(self._tray_key, "").strip()
        tray_id = tray_raw.upper()
        if not tray_raw or not _is_valid_tray_id(tray_id):
            return None

        start_dt = _coerce_datetime(row.get("start_time"))
        end_dt = _coerce_datetime(row.get("end_time"))

        result = {
            "cell_id": wanted,
            "tray_id": tray_id,
            "start_time": (
                start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else None
            ),
            "end_time": (
                end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else None
            ),
        }
        self._cell_owner_cache.set(cache_key, result)
        return result

    async def _fetch_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """Keyset-paginated range fetch for initial/delta loads."""
        batch_size = max(1, int(settings.max_fetch_batch))
        pages = 0
        all_rows: list[dict[str, Any]] = []

        # Keyset cursor over (COLLECTED_TIME, PK_ID) for O(n) paging.
        last_collected_time: datetime = start_time
        last_pk_id: str = ""

        keyset_query = f"""
        SELECT TOP {batch_size}
            {self._lot_col}    AS lot_no,
            {self._start_col}  AS start_time,
            {self._end_col}    AS end_time,
            {self._record_col} AS record_json,
            {self._update_col} AS update_time,
            [PK_ID]            AS pk_id
        FROM {self._table} WITH (NOLOCK)
        WHERE (
            {self._update_col} > ?
            OR ({self._update_col} = ? AND [PK_ID] > ?)
        )
          AND {self._update_col} <= ?
          AND {self._record_col} IS NOT NULL
          AND LEN({self._record_col}) > 10
        ORDER BY {self._update_col} ASC, [PK_ID] ASC
        """

        while True:
            chunk = await self._conn.query_rows(
                keyset_query,
                [
                    last_collected_time,
                    last_collected_time,
                    last_pk_id,
                    end_time,
                ],
                query_name="ccu.fetch_range",
            )

            if not chunk:
                break

            all_rows.extend(chunk)
            pages += 1

            last_row = chunk[-1]
            next_collected_time = _coerce_datetime(last_row.get("update_time"))
            if next_collected_time is None:
                log.warning(
                    "CCU fetch range keyset cursor missing update_time; "
                    "stopping pagination page=%s",
                    pages,
                )
                break
            last_collected_time = next_collected_time
            last_pk_id = str(last_row.get("pk_id") or "")

            if len(chunk) < batch_size:
                break

        if pages > 1:
            log.info(
                "CCU fetch range paged pages=%s raw_rows=%s batch_size=%s",
                pages,
                len(all_rows),
                batch_size,
            )

        for row in all_rows:
            row.pop("pk_id", None)
        return all_rows

    async def _fetch_by_tray_ids_batch(
        self,
        tray_ids: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """
        Fetch rows for a batch of tray IDs using the configured strategy.
        """
        if not tray_ids:
            return []

        if self._lookup_strategy == TrayLookupStrategy.JSON_VALUE:
            return await self._fetch_by_tray_json_value(
                tray_ids, start_time, end_time
            )
        elif self._lookup_strategy == TrayLookupStrategy.INDEXED_COLUMN:
            return await self._fetch_by_tray_column(
                tray_ids, start_time, end_time
            )
        else:
            return await self._fetch_by_tray_like(
                tray_ids, start_time, end_time
            )

    async def _fetch_by_tray_json_value(
        self,
        tray_ids: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """
        Backfill lookup using JSON_VALUE() extraction.
        Precise and efficient for small batches.
        """
        all_rows: list[dict[str, Any]] = []
        backfill_batch = min(5000, settings.max_fetch_batch)

        for chunk in _chunked(tray_ids, 20):
            # Build OR conditions for JSON_VALUE matches
            json_conditions = " OR ".join(
                f"JSON_VALUE({self._record_col}, '{self._json_tray_path}') = ?"
                for _ in chunk
            )
            query = f"""
            SELECT TOP {backfill_batch}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json,
                {self._update_col} AS update_time
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND ({json_conditions})
            ORDER BY {self._update_col} DESC
            """
            try:
                rows = await self._conn.query_rows(
                    query,
                    [start_time, end_time, *chunk],
                    query_name="ccu.backfill_json_value",
                )
                all_rows.extend(rows)
                log.debug(
                    "CCU backfill JSON_VALUE rows=%s chunk_size=%s",
                    len(rows), len(chunk),
                )
            except Exception as exc:
                log.warning(
                    "CCU backfill JSON_VALUE failed chunk=%s error=%r, "
                    "falling back to LIKE for this chunk",
                    chunk, exc,
                )
                # Per-chunk fallback to LIKE
                fallback_rows = await self._fetch_by_tray_like(
                    chunk, start_time, end_time
                )
                all_rows.extend(fallback_rows)

        return all_rows

    async def _fetch_by_tray_column(
        self,
        tray_ids: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """
        Fast backfill lookup using indexed computed column.
        Only used if DBA has created a computed column like:
          ALTER TABLE TT_WO_RPARAM_RECORD_CCU
          ADD TRAY_ID AS JSON_VALUE(RECORD_JSON, '$.TRAYID.paramValue');
        """
        all_rows: list[dict[str, Any]] = []
        backfill_batch = min(5000, settings.max_fetch_batch)

        for chunk in _chunked(tray_ids, 50):
            placeholders = ", ".join("?" for _ in chunk)
            query = f"""
            SELECT TOP {backfill_batch}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json,
                {self._update_col} AS update_time
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND {self._tray_id_col_quoted} IN ({placeholders})
            ORDER BY {self._update_col} DESC
            """
            try:
                rows = await self._conn.query_rows(
                    query,
                    [start_time, end_time, *chunk],
                    query_name="ccu.backfill_indexed",
                )
                all_rows.extend(rows)
            except Exception as exc:
                log.warning(
                    "CCU backfill indexed-column failed chunk=%s error=%r",
                    chunk, exc,
                )

        return all_rows

    async def _fetch_by_tray_like(
        self,
        tray_ids: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """
        Slow fallback: RECORD_JSON LIKE '%tray_id%' full-table scan.
        Only used when other strategies fail or are explicitly configured.
        """
        all_rows: list[dict[str, Any]] = []
        backfill_batch = min(5000, settings.max_fetch_batch)

        for mini_batch in _chunked(tray_ids, 10):
            like_clause = " OR ".join(
                f"{self._record_col} LIKE ?" for _ in mini_batch
            )
            like_params = [f"%{t}%" for t in mini_batch]
            query = f"""
            SELECT TOP {backfill_batch}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json,
                {self._update_col} AS update_time
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND ({like_clause})
            ORDER BY {self._update_col} DESC
            """
            try:
                rows = await self._conn.query_rows(
                    query,
                    [start_time, end_time, *like_params],
                    query_name="ccu.backfill_like",
                )
                all_rows.extend(rows)
            except Exception:
                log.warning(
                    "CCU backfill LIKE scan failed tray_ids=%s, skipping",
                    mini_batch,
                )

        return all_rows

    async def _parse_rows_parallel(
        self,
        rows: list[dict[str, Any]],
    ) -> list[TraySignal]:
        if not rows:
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            _get_parse_executor(),
            _parse_ccu_rows_sync,
            rows,
            self._tray_key,
            self._pos_key,
            self._keyword,
        )

    def cache_stats(self) -> dict[str, Any]:
        return {
            "tray_cells": self._tray_cells_cache.stats(),
            "cell_owner": self._cell_owner_cache.stats(),
            "known_trays": len(self._known_tray_data),
            "lookup_strategy": self._lookup_strategy,
            "json_tray_path": self._json_tray_path,
        }
