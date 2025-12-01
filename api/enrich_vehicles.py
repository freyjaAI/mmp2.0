"""Vehicle Registration Enrichment Module

Nationwide vehicle registration via OpenDataNation API.
Free tier: Unlimited; $0.001/lookup for premium features.

Cost: Free basic, $0.001/lookup premium
Rate limit: 100 req/sec
Coverage: Nationwide (all 50 states)
"""

import asyncio
import os
import logging
from typing import Optional
import aiohttp
import redis.asyncio as redis
from datetime import datetime

logger = logging.getLogger(__name__)

ODN_API_BASE = "https://api.opendatanation.com/vehicle"
CACHE_TTL = 86400  # 24 hours
RATE_LIMIT_DELAY = 0.01  # 100 req/sec

_rate_limiter: Optional[asyncio.Semaphore] = None


async def get_rate_limiter():
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = asyncio.Semaphore(50)  # 50 concurrent
    return _rate_limiter


async def _get_redis_client():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        client = await redis.from_url(redis_url, decode_responses=True)
        await client.ping()
        return client
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        return None


async def _fetch_vehicles_odn(person_data: dict) -> Optional[list]:
    """Fetch vehicle registration from OpenDataNation."""
    limiter = await get_rate_limiter()
    
    # Extract search parameters
    first_name = person_data.get("first_name", "")
    last_name = person_data.get("last_name", "")
    
    if not (first_name and last_name):
        return None
    
    async with limiter:
        headers = {"User-Agent": "MMP-Risk-Analytics/1.0"}
        api_key = os.getenv("ODN_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{ODN_API_BASE}/search"
                params = {"first_name": first_name, "last_name": last_name}
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("vehicles", []) if isinstance(data, dict) else []
                    elif resp.status == 404:
                        return []
                    else:
                        logger.warning(f"ODN API returned {resp.status}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"ODN API timeout")
            return None
        except Exception as e:
            logger.error(f"ODN API error: {e}")
            return None
        finally:
            await asyncio.sleep(RATE_LIMIT_DELAY)


async def enrich_vehicles(person_data: dict) -> dict:
    """
    Enrichment function: Fetch vehicle registrations for person.
    
    Returns:
        {"vehicles": [...]} or {}
    """
    if not person_data:
        return {}
    
    # Try cached result
    redis_client = await _get_redis_client()
    cache_key = f"vehicles:{person_data.get('id', '')}"
    
    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                import json
                return {"vehicles": json.loads(cached)}
        except Exception:
            pass
    
    # Fetch vehicles
    vehicles = await _fetch_vehicles_odn(person_data)
    
    if vehicles is None:
        return {}
    
    # Format output
    formatted = []
    for vehicle in vehicles:
        try:
            formatted.append({
                "year": vehicle.get("year"),
                "make": vehicle.get("make"),
                "model": vehicle.get("model"),
                "vin": vehicle.get("vin"),
                "license_plate": vehicle.get("license_plate"),
                "registration_state": vehicle.get("state"),
                "owner_name": vehicle.get("owner_name"),
            })
        except Exception:
            continue
    
    # Cache result
    if redis_client and formatted:
        try:
            import json
            await redis_client.setex(cache_key, CACHE_TTL, json.dumps(formatted))
        except Exception:
            pass
    
    logger.info(f"Found {len(formatted)} vehicles for person")
    return {"vehicles": formatted} if formatted else {}
