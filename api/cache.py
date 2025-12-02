import os, json, functools, redis
from typing import Callable

TTL = int(os.getenv("CACHE_TTL", 300))
redis_url = os.getenv("REDIS_URL")
try:
    r = redis.from_url(redis_url, decode_responses=True) if (redis_url and redis_url.strip()) else None
except (ValueError, Exception):
    r = None  # Redis not configured or invalid URL

def get_redis():
    """Get Redis client instance. Returns None if Redis is not configured."""
    return r
def cache_key(func: Callable, *args, **kw):
    return f"{func.__name__}:{hash(str(args)+str(sorted(kw.items())))}"

def cache_ttl(ttl: int = TTL):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            if r is None:  # Redis not configured
                return func(*args, **kw)
            key = cache_key(func, *args, **kw)
            hit = r.get(key)
            if hit:
                return json.loads(hit)
            val = func(*args, **kw)
            r.setex(key, ttl, json.dumps(val))
            return val
        return wrapper
    return decorator
