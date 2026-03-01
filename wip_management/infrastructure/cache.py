# wip_management/infrastructure/cache.py
"""
Shared caching utilities for the WIP Management system.
"""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generic, TypeVar

from wip_management.config import settings

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(slots=True)
class CacheEntry(Generic[T]):
    """Cache entry with metadata."""
    data: T
    created_at: float
    access_count: int = 0
    last_access: float = field(default_factory=time.monotonic)
    size_bytes: int = 0


class TTLCache(Generic[T]):
    """
    Thread-safe LRU cache with TTL expiration.
    
    Features:
    - O(1) get/set operations
    - Automatic TTL expiration
    - LRU eviction when at capacity
    - Hit/miss statistics
    """

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
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> T | None:
        """Get value from cache, returns None if not found or expired."""
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

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            entry.access_count += 1
            entry.last_access = now
            self._hits += 1
            return entry.data

    def set(self, key: str, value: T, *, size_bytes: int = 0) -> None:
        """Set value in cache with optional size tracking."""
        with self._lock:
            now = time.monotonic()
            self._evict_expired(now)

            while len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)

            self._cache[key] = CacheEntry(
                data=value,
                created_at=now,
                size_bytes=size_bytes,
            )

    def invalidate(self, key: str) -> bool:
        """Remove specific key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def invalidate_prefix(self, prefix: str) -> int:
        """Remove all keys starting with prefix."""
        with self._lock:
            to_remove = [k for k in self._cache if k.startswith(prefix)]
            for k in to_remove:
                del self._cache[k]
            return len(to_remove)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()

    def _evict_expired(self, now: float) -> None:
        """Remove expired entries."""
        if self._ttl <= 0:
            return
        expired = [
            k for k, v in self._cache.items()
            if (now - v.created_at) > self._ttl
        ]
        for k in expired:
            del self._cache[k]

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0.0
            total_size = sum(e.size_bytes for e in self._cache.values())
            return {
                "name": self._name,
                "size": len(self._cache),
                "maxsize": self._maxsize,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": round(hit_rate, 1),
                "total_size_mb": round(total_size / 1024 / 1024, 2),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)


class DiskCache:
    """
    Persistent disk-based cache for large objects.
    
    Features:
    - Survives application restarts
    - Automatic expiration
    - Size-limited eviction
    """

    def __init__(
        self,
        cache_dir: Path | None = None,
        max_entries: int = 5000,
        ttl_hours: float = 24.0,
    ) -> None:
        if cache_dir is None:
            cache_dir = settings.get_local_cache_path()
        
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._max_entries = max_entries
        self._ttl_seconds = ttl_hours * 3600
        self._index_file = self._cache_dir / "_index.json"
        self._lock = threading.Lock()
        self._index: dict[str, dict[str, Any]] = self._load_index()
        
        # Clean expired on startup
        self._cleanup_expired()

    def _load_index(self) -> dict[str, dict[str, Any]]:
        """Load index from disk."""
        try:
            if self._index_file.exists():
                with open(self._index_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            log.debug("Disk cache index load failed: %s", e)
        return {}

    def _save_index(self) -> None:
        """Save index to disk."""
        try:
            with open(self._index_file, "w", encoding="utf-8") as f:
                json.dump(self._index, f)
        except Exception as e:
            log.warning("Disk cache index save failed: %s", e)

    def _key_to_filename(self, key: str) -> str:
        """Convert key to safe filename."""
        return hashlib.sha256(key.encode()).hexdigest()[:32] + ".pkl"

    def get(self, key: str) -> Any | None:
        """Get value from disk cache."""
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
                    with open(filepath, "rb") as f:
                        return pickle.load(f)
            except Exception as e:
                log.debug("Disk cache read failed for %s: %s", key, e)
                self._remove_entry(key)

            return None

    def set(self, key: str, value: Any) -> None:
        """Store value in disk cache."""
        with self._lock:
            self._evict_if_needed()

            filename = self._key_to_filename(key)
            filepath = self._cache_dir / filename

            try:
                with open(filepath, "wb") as f:
                    pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)

                self._index[key] = {
                    "filename": filename,
                    "created_at": time.time(),
                    "size": filepath.stat().st_size,
                }
                self._save_index()
            except Exception as e:
                log.warning("Disk cache write failed for %s: %s", key, e)

    def _remove_entry(self, key: str) -> None:
        """Remove entry from cache."""
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
        """Evict old entries if over limit."""
        self._cleanup_expired()

        while len(self._index) >= self._max_entries:
            oldest_key = min(
                self._index.keys(),
                key=lambda k: self._index[k].get("created_at", 0),
            )
            self._remove_entry(oldest_key)

    def _cleanup_expired(self) -> None:
        """Remove all expired entries."""
        now = time.time()
        expired = [
            k for k, v in self._index.items()
            if now - v.get("created_at", 0) > self._ttl_seconds
        ]
        for k in expired:
            self._remove_entry(k)

    def clear(self) -> None:
        """Clear all cached data."""
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

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_size = sum(e.get("size", 0) for e in self._index.values())
            return {
                "entries": len(self._index),
                "max_entries": self._max_entries,
                "total_size_mb": round(total_size / 1024 / 1024, 2),
                "cache_dir": str(self._cache_dir),
            }


# ═══════════════════════════════════════════════════════════════════════════
# Shared cache instances
# ═══════════════════════════════════════════════════════════════════════════

_tray_detail_cache: TTLCache[list[dict]] | None = None
_disk_cache: DiskCache | None = None


def get_tray_detail_cache() -> TTLCache[list[dict]]:
    """Get shared tray detail memory cache."""
    global _tray_detail_cache
    if _tray_detail_cache is None:
        _tray_detail_cache = TTLCache(
            maxsize=settings.tray_detail_cache_max_entries,
            ttl_seconds=settings.tray_detail_cache_ttl_seconds,
            name="tray_detail",
        )
    return _tray_detail_cache


def get_disk_cache() -> DiskCache:
    """Get shared disk cache."""
    global _disk_cache
    if _disk_cache is None:
        _disk_cache = DiskCache(
            max_entries=settings.local_cache_max_entries,
            ttl_hours=settings.local_cache_ttl_hours,
        )
    return _disk_cache


def clear_all_caches() -> None:
    """Clear all caches (useful for testing or reset)."""
    global _tray_detail_cache, _disk_cache
    
    if _tray_detail_cache is not None:
        _tray_detail_cache.clear()
    
    if _disk_cache is not None:
        _disk_cache.clear()


def get_cache_stats() -> dict[str, Any]:
    """Get statistics for all caches."""
    stats = {}
    
    if _tray_detail_cache is not None:
        stats["memory"] = _tray_detail_cache.stats()
    
    if _disk_cache is not None:
        stats["disk"] = _disk_cache.stats()
    
    return stats