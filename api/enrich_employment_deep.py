#!/usr/bin/env python3
"""
Nationwide employment history – job titles, employers, start/end dates, industry.
Source: Data Axle employment bulk (free, uses existing token).
"""
import aiohttp, os
from typing import Optional, List

DATA_AXLE_EMP = "https://api.data-axle.com/v2/employment/search"  # uses existing free token

async def enrich_employment_deep(person_name: str) -> Optional[List[dict]]:
    """
    Returns [{job_title, employer, start_date, end_date, industry, source}]
    """
    last, first = person_name.split(", ") if ", " in person_name else (person_name, "")
    payload = {
        "names": [f"{first} {last}"],
        "select": "job_title,employer_name,start_date,end_date,industry",
        "limit": 20
    }
    headers = {"Authorization": f"Bearer {os.getenv('DATA_AXLE_API_KEY')}"}
    async with aiohttp.ClientSession() as session:
        async with session.post(DATA_AXLE_EMP, json=payload, headers=headers, timeout=15) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    jobs = data.get("results", [])
    cleaned = []
    for j in jobs:
        cleaned.append({
            "job_title": j.get("job_title"),
            "employer": j.get("employer_name"),
            "start_date": j.get("start_date"),
            "end_date": j.get("end_date"),
            "industry": j.get("industry"),
            "source": "data_axle_employment"
        })
    return cleaned[:20]  # cap at 20 jobs
