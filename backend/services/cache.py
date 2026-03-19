"""
Simple in-memory cache with optional Redis backend.

Used to cache fetched odds so we don't hammer The Odds API
on every request. TTL defaults to 30 seconds.
"""
import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# In-memory fallback
_store: dict = {}


def _now() -> float:
    return time.monotonic()


def get(key: str) -> Optional[Any]:
    """Get a cached value. Returns None if missing or expired."""
    entry = _store.get(key)
    if not entry:
        return None
    value, expires_at = entry
    if _now() > expires_at:
        del _store[key]
        return None
    return value


def set(key: str, value: Any, ttl: int = 30) -> None:
    """Cache a value with a TTL (seconds)."""
    _store[key] = (value, _now() + ttl)


def delete(key: str) -> None:
    _store.pop(key, None)


def clear() -> None:
    _store.clear()


# ---- Optional Redis backend ----

_redis_client = None


def init_redis(redis_url: str) -> None:
    global _redis_client
    try:
        import redis
        _redis_client = redis.from_url(redis_url, decode_responses=True)
        _redis_client.ping()
        logger.info(f"Redis connected at {redis_url}")
    except Exception as e:
        logger.warning(f"Redis unavailable ({e}), using in-memory cache")
        _redis_client = None


def redis_get(key: str) -> Optional[Any]:
    if not _redis_client:
        return get(key)
    try:
        raw = _redis_client.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return get(key)


def redis_set(key: str, value: Any, ttl: int = 30) -> None:
    if not _redis_client:
        set(key, value, ttl)
        return
    try:
        _redis_client.setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        set(key, value, ttl)
