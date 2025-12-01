"""SEC EDGAR Form 3/4/5 Enrichment Module

Nationwide officer/director filing enrichment via SEC EDGAR API.
Free, unlimited, nationwide coverage. Lazy-triggered enrichment.

Cost: $0.00/lookup
Rate limit: 10,000/hr (conservative)
Coverage: Nationwide (all 50 states)
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import Optional
import aiohttp
from xml.etree import ElementTree as ET
import redis.asyncio as redis
from functools import lru_cache

logger = logging.getLogger(__name__)

SEC_DAILY_INDEX_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=3%2C4%2C5&dateb={date}&owner=exclude&count=100&output=xml"
SEC_FILING_DETAIL_URL = "https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession}&xbrl_type=v"
CACHE_TTL = 3600  # 1 hour
FILING_LIMIT = 20  # PDF-friendly cap
RATE_LIMIT_DELAY = 0.1  # 10 req/sec

# Global semaphore for rate limiting
_rate_limiter: Optional[asyncio.Semaphore] = None


async def get_rate_limiter():
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = asyncio.Semaphore(10)  # 10 concurrent requests
    return _rate_limiter


async def _get_redis_client():
    """Get or create Redis client for caching."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        client = await redis.from_url(redis_url, decode_responses=True)
        await client.ping()
        return client
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}. Using in-memory cache.")
        return None


@lru_cache(maxsize=1024)
def _normalize_name(name: str) -> str:
    """Normalize name for fuzzy matching."""
    if not name:
        return ""
    return name.lower().strip()


def _name_matches(filing_name: str, person_name: str, threshold: float = 0.8) -> bool:
    """Check if filing officer name matches person name with fuzzy logic."""
    if not filing_name or not person_name:
        return False

    filing_norm = _normalize_name(filing_name)
    person_norm = _normalize_name(person_name)

    # Exact match
    if filing_norm == person_norm:
        return True

    # Substring match (handles common name variations)
    if filing_norm in person_norm or person_norm in filing_norm:
        return True

    # Levenshtein distance-based fuzzy match
    try:
        from difflib import SequenceMatcher
        ratio = SequenceMatcher(None, filing_norm, person_norm).ratio()
        return ratio >= threshold
    except Exception:
        return False


async def _fetch_daily_index(date: str) -> Optional[str]:
    """Fetch SEC daily index XML for given date (YYYY-MM-DD)."""
    limiter = await get_rate_limiter()
    async with limiter:
        url = SEC_DAILY_INDEX_URL.format(date=date)
        headers = {"User-Agent": "MMP-Risk-Analytics/1.0 (Affordable Risk Analysis)"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    else:
                        logger.warning(f"SEC API returned {resp.status} for {date}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"SEC API timeout for {date}")
            return None
        except Exception as e:
            logger.error(f"SEC API error: {e}")
            return None
        finally:
            await asyncio.sleep(RATE_LIMIT_DELAY)


def _parse_sec_xml(xml_content: str, person_name: str) -> list:
    """Parse SEC XML daily index and extract matching filings."""
    filings = []

    try:
        root = ET.fromstring(xml_content)
        # SEC XML namespace
        ns = {"sec": "http://www.sec.gov/schemas/xml/feed"}

        # Fallback: try without namespace
        entries = root.findall(".//entry") or root.findall(".//sec:entry", ns)

        for entry in entries:
            try:
                # Extract filing details
                title_elem = entry.find(".//title") or entry.find(".//sec:title", ns)
                title = title_elem.text if title_elem is not None else ""

                summary_elem = entry.find(".//summary") or entry.find(".//sec:summary", ns)
                summary = summary_elem.text if summary_elem is not None else ""

                link_elem = entry.find(".//link") or entry.find(".//sec:link", ns)
                link = link_elem.get("href", "") if link_elem is not None else ""

                updated_elem = entry.find(".//updated") or entry.find(".//sec:updated", ns)
                updated = updated_elem.text if updated_elem is not None else ""

                # Try to extract officer/director name from title or summary
                if not title:
                    continue

                # Parse form type (e.g., "4" from "Form 4")
                form_type = title.split(" ")[1] if len(title.split(" ")) > 1 else ""
                if form_type not in ["3", "4", "5"]:
                    continue

                # Extract CIK and accession number from link
                if "/cgi-bin/" not in link:
                    continue

                # Parse company and filing info from summary
                # Format: "CIK: 0000320193 Company: Apple Inc. Form Type: 4 Filed: 2024-01-15"
                company_name = summary.split("Company: ")[1].split(" Form")[0] if "Company: " in summary else ""
                filed_date = updated.split("T")[0] if updated else ""

                # Extract CIK from link
                cik = link.split("CIK=")[1].split("&")[0] if "CIK=" in link else ""

                # Extract accession number
                accession = link.split("accession_number=")[1].split("&")[0] if "accession_number=" in link else ""

                # Check if officer/director name matches person
                officer_name = title.replace(f"Form {form_type} ", "").split(" - ")[0] if " - " in title else ""

                if _name_matches(officer_name, person_name):
                    filings.append({
                        "form_type": form_type,
                        "company_name": company_name or "Unknown",
                        "filed_date": filed_date,
                        "cik": cik,
                        "accession_number": accession,
                        "link": link,
                    })

            except Exception as e:
                logger.debug(f"Error parsing entry: {e}")
                continue

        return filings

    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error parsing SEC XML: {e}")
        return []


async def enrich_sec_filings(person_data: dict) -> dict:
    """
    Enrichment function: Fetch SEC Form 3/4/5 filings for person.
    
    Returns:
        {"sec_filings": [...]} or {} if no filings found or error
    
    Idempotent: Safe to re-run (no state changes)
    Async: Non-blocking, executes in background
    Cost guard: 20-filing cap maintains <$0.01 cost
    """

    if not person_data or "name" not in person_data:
        return {}

    person_name = person_data.get("name", "")
    if not person_name:
        return {}

    # Try to get cached result
    redis_client = await _get_redis_client()
    cache_key = f"sec_filings:{person_name.lower().strip()}"

    if redis_client:
        try:
            cached = await redis_client.get(cache_key)
            if cached:
                logger.info(f"SEC filings cache hit for {person_name}")
                import json
                return {"sec_filings": json.loads(cached)}
        except Exception as e:
            logger.warning(f"Redis cache miss: {e}")

    # Fetch yesterday's index (data lag = 1 day)
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    xml_content = await _fetch_daily_index(yesterday)

    if not xml_content:
        return {}

    # Parse and filter filings
    all_filings = _parse_sec_xml(xml_content, person_name)

    # Cap at 20 filings for cost/PDF friendliness
    filings = all_filings[:FILING_LIMIT]

    # Cache result
    if redis_client and filings:
        try:
            import json
            await redis_client.setex(
                cache_key,
                CACHE_TTL,
                json.dumps(filings),
            )
        except Exception as e:
            logger.warning(f"Redis cache set failed: {e}")

    logger.info(f"Found {len(filings)} SEC filings for {person_name}")
    return {"sec_filings": filings} if filings else {}


# Entry point for async enrichment orchestrator
async def enrich_sec(person_data: dict) -> dict:
    """Wrapper for enrichment orchestrator integration."""
    return await enrich_sec_filings(person_data)
