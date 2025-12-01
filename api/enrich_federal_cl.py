import aiohttp, os, datetime
from typing import Optional, List

CL_URL = "https://www.courtlistener.com/api/rest/v3/dockets/"
CL_TOKEN = os.getenv("CL_TOKEN", "")  # optional, raises limit

async def enrich_federal_cases(person_name: str) -> Optional[List[dict]]:
    """
    Returns federal cases (criminal, civil, bankruptcy) nationwide.
    """
    last, first = person_name.split(", ") if ", " in person_name else (person_name, "")
    params = {
        "name": f"{first} {last}",
        "court__jurisdiction": "federal",
        "page_size": 50,
    }
    headers = {"Authorization": f"Token {CL_TOKEN}"} if CL_TOKEN else {}

    async with aiohttp.ClientSession() as session:
        async with session.get(CL_URL, params=params, headers=headers, timeout=15) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

    cases = data.get("results", [])
    cleaned = []
    for c in cases:
        cleaned.append({
            "case_number": c["docket_number"],
            "case_title": c.get("case_name", ""),
            "court": c["court"]["short_name"],
            "filed_date": c.get("date_filed"),
            "case_type": c.get("case_type", ""),
            "nature_suit": c.get("nature_of_suit", ""),
            "source": "courtlistener_federal"
        })

    return cleaned
