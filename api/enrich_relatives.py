"""Relatives & Associates Deep Graph Enrichment Module

Nationwide relatives/associates via A-Leads family tree API.
Uses existing free token (60K/month quota).

Cost: $0.00/lookup (uses existing A-Leads quota)
Rate limit: 10 req/sec
Coverage: Nationwide (all 50 states)
"""

import asyncio
import os
import logging
from typing import Optional, List, Dict
import aiohttp
import redis.asyncio as redis
from datetime import datetime

logger = logging.getLogger(__name__)

A_LEADS_FAMILY = "https://app.a-leads.co/api/v2/family"
A_LEADS_KEY = os.getenv("A_LEADS_API_KEY", "")
CACHE_TTL = 86400  # 24 hours
RATE_LIMIT_DELAY = 0.1  # 10 req/sec
FREE_TIER_MONTHLY_LIMIT = 60000

# Global state for free tier quota
_monthly_usage = {"count": 0, "month": None}
_rate_limiter: Optional[asyncio.Semaphore] = None


async def get_rate_limiter():
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = asyncio.Semaphore(10)  # 10 concurrent
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


def _is_free_tier_available() -> bool:
    """Check if free tier monthly quota is available."""
    current_month = datetime.utcnow().strftime("%Y-%m")
    
    if _monthly_usage["month"] != current_month:
        _monthly_usage["count"] = 0
        _monthly_usage["month"] = current_month
    
    return _monthly_usage["count"] < FREE_TIER_MONTHLY_LIMIT


def _increment_monthly_usage():
    _monthly_usage["count"] += 1


async def enrich_relatives_deep(person_name: str) -> Optional[List[Dict]]:
    """Fetch deep relatives & associates graph from A-Leads."""
    
    if not A_LEADS_KEY:
        logger.warning("A_LEADS_API_KEY not configured")
        return None
    
    if not _is_free_tier_available():
        logger.warning("A-Leads monthly quota exceeded")
        return None
    
    limiter = await get_rate_limiter()
    async with limiter:
        try:
            # Parse name
            parts = person_name.split(",") if "," in person_name else person_name.split()
            last = parts[0].strip() if "," in person_name else parts[-1].strip()
            first = parts[1].strip() if "," in person_name and len(parts) > 1 else parts[0].strip() if len(parts) > 0 else ""
            
            async with aiohttp.ClientSession() as session:
                payload = {"first_name": first, "last_name": last}
                headers = {"X-API-Key": A_LEADS_KEY}
                
                async with session.post(A_LEADS_FAMILY, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        _increment_monthly_usage()
                        
                        # Parse results
                        results = data.get("results", [])
                        if not results:
                            return None
                        
                        family = results[0].get("family", [])
                        associates = results[0].get("associates", [])
                        
                        cleaned = []
                        
                        # Parse family
                        for rel in family:
                            cleaned.append({
                                "relationship": rel.get("relationship", "relative"),
                                "name": rel.get("name", ""),
                                "age": rel.get("age"),
                                "address": rel.get("address", ""),
                                "phone": rel.get("phone", ""),
                                "email": rel.get("email", ""),
                                "source": "a_leads_family"
                            })
                        
                        # Parse associates
                        for assoc in associates:
                            cleaned.append({
                                "relationship": assoc.get("relationship", "associate"),
                                "name": assoc.get("name", ""),
                                "age": assoc.get("age"),
                                "address": assoc.get("address", ""),
                                "phone": assoc.get("phone", ""),
                                "email": assoc.get("email", ""),
                                "source": "a_leads_associates"
                            })
                        
                        return cleaned
                    elif resp.status == 404:
                        return []
                    else:
                        logger.warning(f"A-Leads family API returned {resp.status}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"A-Leads family API timeout")
            return None
        except Exception as e:
            logger.error(f"A-Leads family API error: {e}")
            return None
        finally:
            await asyncio.sleep(RATE_LIMIT_DELAY)


async def enrich_relatives(person_data: dict) -> dict:
    """
    Enrichment function: Fetch relatives & associates deep graph.
    
    Returns:
        {"relatives_deep": [...]} or {}
    """
    if not person_data:
        return {}
    
    # Extract person name
    person_name = person_data.get("name", person_data.get("best_name", ""))
    if not person_name:
        return {}
    
    # Try cached result
    redis_client = await _get_redis_client()
    cache_key = f"relatives_deep:{person_name.lower().strip()}"
    
    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                import json
                return {"relatives_deep": json.loads(cached)}
        except Exception:
            pass
    
    # Fetch relatives
    relatives = await enrich_relatives_deep(person_name)
    
    if relatives is None:
        return {}
    
    # Cache result
    if redis_client and relatives:
        try:
            import json
            await redis_client.setex(cache_key, CACHE_TTL, json.dumps(relatives))
        except Exception:
            pass
    
    logger.info(f"Found {len(relatives)} relatives/associates for {person_name}")
    return {"relatives_deep": relatives} if relatives else {}
