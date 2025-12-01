import os, json, functools, redis
from typing import Callable

TTL = int(os.getenv("CACHE_TTL", 300))
r = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)

def cache_key(func: Callable, *args, **kw):
    return f"{func.__name__}:{hash(str(args)+str(sorted(kw.items())))}"

def cache_ttl(ttl: int = TTL):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            key = cache_key(func, *args, **kw)
            hit = r.get(key)
            if hit:
                return json.loads(hit)
            val = func(*args, **kw)
            r.setex(key, ttl, json.dumps(val))
            return val
        return wrapper
    return decorator
