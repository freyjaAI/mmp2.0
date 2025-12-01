import aiohttp, os, datetime
from typing import Optional

EVICTION_URL = "https://data.harriscountytx.gov/resource/3bgt-xf3c.json"  # justice courts eviction cases

async def enrich_evictions(person_name: str) -> Optional[dict]:
    """
    Returns {eviction_count, eviction_dates[]} for Harris County only.
    """
    last, first = person_name.split(", ") if ", " in person_name else (person_name, "")
    params = {
        "$select": "case_number, filed_date, defendant",
        "$where": f"lower(defendant) LIKE lower('%{last}%') AND lower(defendant) LIKE lower('%{first}%')",
        "$order": "filed_date DESC",
        "$limit": 100
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(EVICTION_URL, params=params, timeout=15) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if not data:
                return None
            
            return {
                "eviction_count": len(data),
                "eviction_dates": [d["filed_date"][:10] for d in data]
            }
