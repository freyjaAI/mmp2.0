"""HaveIBeenPwned Breach Enrichment Module

Nationwide data breach history enrichment via HaveIBeenPwned (HIBP) API.
Free tier: 500 lookups/month; Paid API: $0.001/lookup.
Lazy-triggered enrichment with cost guards.

Cost: $0.001/lookup (paid API) or free tier
Rate limit: 1.5 req/sec (HIBP policy)
Coverage: Nationwide (all 50 states)
"""

import asyncio
import os
import logging
from typing import Optional
import aiohttp
import redis.asyncio as redis
from functools import lru_cache
from datetime import datetime

logger = logging.getLogger(__name__)

HIBP_API_URL = "https://haveibeenpwned.com/api/v3/breachedaccount/{email}"
HIBP_PASTE_URL = "https://haveibeenpwned.com/api/v3/pasteaccount/{email}"
CACHE_TTL = 86400  # 24 hours (breach data doesn't change frequently)
RATE_LIMIT_DELAY = 0.7  # 1.5 req/sec maximum
FREE_TIER_MONTHLY_LIMIT = 500
COST_PER_LOOKUP = 0.001

# Global state for free tier quota
_monthly_usage = {"count": 0, "month": None}
_rate_limiter: Optional[asyncio.Semaphore] = None


async def get_rate_limiter():
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = asyncio.Semaphore(1)  # 1 concurrent request for HIBP
    return _rate_limiter


async def _get_redis_client():
    """Get or create Redis client for caching."""
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
    
    # Reset monthly counter if month changed
    if _monthly_usage["month"] != current_month:
        _monthly_usage["count"] = 0
        _monthly_usage["month"] = current_month
    
    return _monthly_usage["count"] < FREE_TIER_MONTHLY_LIMIT


def _increment_monthly_usage():
    """Increment monthly usage counter."""
    _monthly_usage["count"] += 1


async def _fetch_breaches_hibp(email: str, use_paid_api: bool = False) -> Optional[list]:
    """Fetch breach data from HaveIBeenPwned API."""
    limiter = await get_rate_limiter()
    
    async with limiter:
        headers = {
            "User-Agent": "MMP-Risk-Analytics/1.0",
        }
        
        if use_paid_api:
            api_key = os.getenv("HIBP_API_KEY")
            if api_key:
                headers["hibp-api-key"] = api_key
            else:
                logger.warning("HIBP_API_KEY not configured; using free tier")
                use_paid_api = False
        
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch breached account history
                url = HIBP_API_URL.format(email=email)
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        breaches = await resp.json()
                        return breaches if isinstance(breaches, list) else []
                    elif resp.status == 404:
                        return []  # No breaches found
                    else:
                        logger.warning(f"HIBP API returned {resp.status} for {email}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"HIBP API timeout for {email}")
            return None
        except Exception as e:
            logger.error(f"HIBP API error: {e}")
            return None
        finally:
            await asyncio.sleep(RATE_LIMIT_DELAY)


def _extract_email_from_person(person_data: dict) -> Optional[str]:
    """Extract email from person data."""
    # Try common email fields
    for field in ["email", "primary_email", "emails"]:
        if field in person_data:
            email = person_data[field]
            if isinstance(email, list) and email:
                return email[0]
            elif isinstance(email, str):
                return email
    return None


async def enrich_breach_history(person_data: dict) -> dict:
    """
    Enrichment function: Fetch data breach history for person.
    
    Returns:
        {"breaches": [...]} or {} if no breaches found or error
    
    Idempotent: Safe to re-run (no state changes)
    Async: Non-blocking, executes in background
    Cost guard: Uses free tier first, respects monthly quota
    """

    if not person_data:
        return {}

    # Extract email
    email = _extract_email_from_person(person_data)
    if not email or "@" not in email:
        return {}

    # Try to get cached result
    redis_client = await _get_redis_client()
    cache_key = f"breaches:{email.lower().strip()}"

    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                logger.info(f"Breach cache hit for {email}")
                import json
                return {"breaches": json.loads(cached)}
        except Exception as e:
            logger.warning(f"Redis cache miss: {e}")

    # Determine if we should use paid API
    use_paid = not _is_free_tier_available()
    
    # Fetch breach data
    breaches = await _fetch_breaches_hibp(email, use_paid_api=use_paid)

    if breaches is None:
        return {}

    # Increment usage if successful
    if breaches is not None:
        _increment_monthly_usage()

    # Format breach data
    formatted_breaches = []
    for breach in breaches:
        try:
            formatted_breaches.append({
                "name": breach.get("Name", ""),
                "title": breach.get("Title", ""),
                "domain": breach.get("Domain", ""),
                "date": breach.get("BreachDate", ""),
                "data_classes": breach.get("DataClasses", []),
                "affected_count": breach.get("PwnCount", 0),
                "is_verified": breach.get("IsVerified", False),
                "is_sensitive": breach.get("IsSensitive", False),
            })
        except Exception as e:
            logger.debug(f"Error parsing breach: {e}")
            continue

    # Cache result
    if redis_client and formatted_breaches:
        try:
            import json
            await redis_client.setex(
                cache_key,
                CACHE_TTL,
                json.dumps(formatted_breaches),
            )
        except Exception as e:
            logger.warning(f"Redis cache set failed: {e}")

    logger.info(f"Found {len(formatted_breaches)} breaches for {email}")
    return {"breaches": formatted_breaches} if formatted_breaches else {}


# Entry point for async enrichment orchestrator
async def enrich_breach(person_data: dict) -> dict:
    """Wrapper for enrichment orchestrator integration."""
    return await enrich_breach_history(person_data)
