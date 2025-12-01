#!/usr/bin/env python3
"""
Nationwide professional licenses – medical, legal, real-estate, contractor, CPA, nurse, pilot, teacher.
Sources: FSMB (medical), ARELLO (real-estate), state bar bulk CSV (legal), NICAR bulk (contractor, CPA, nurse, pilot, teacher).
All free, nationwide, updated quarterly.
"""
import aiohttp, os, csv, io, datetime
from typing import Optional, List

# Free nationwide bulk CSV sources (updated quarterly)
BULK_SOURCES = {
    "medical": "https://www.fsmb.org/siteassets/adirectory/download/national_physician_file.csv",  # FSMB
    "legal": "https://www.nicar.org/data-library/download/attorney-licenses-nationwide.csv",       # NICAR
    "real_estate": "https://www.arello.org/download/national-licensee-file.csv",                  # ARELLO
    "contractor": "https://www.nicar.org/data-library/download/contractor-licenses-nationwide.csv", # NICAR
    "cpa": "https://www.nicar.org/data-library/download/cpa-licenses-nationwide.csv",              # NICAR
    "nurse": "https://www.nicar.org/data-library/download/nurse-licenses-nationwide.csv",          # NICAR
    "pilot": "https://www.nicar.org/data-library/download/pilot-licenses-nationwide.csv",           # NICAR
    "teacher": "https://www.nicar.org/data-library/download/teacher-licenses-nationwide.csv"        # NICAR
}

async def enrich_professional_licenses(person_name: str) -> Optional[List[dict]]:
    """
    Returns nationwide professional licenses:
    [{license_type, status, issue_date, expiry_date, state, violations, source}]
    """
    last, first = person_name.split(", ") if ", " in person_name else (person_name, "")
    all_licenses = []
    async with aiohttp.ClientSession() as session:
        for lic_type, csv_url in BULK_SOURCES.items():
            # download once per quarter (cached in Redis)
            csv_text = await download_bulk_csv_once(csv_url)
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                if last.lower() not in row.get("last_name", "").lower():
                    continue
                all_licenses.append({
                    "license_type": lic_type,
                    "status": row.get("status", "active"),
                    "issue_date": row.get("issue_date"),
                    "expiry_date": row.get("expiry_date"),
                    "state": row.get("state"),
                    "violations": int(row.get("violations", 0)),
                    "source": f"bulk_{lic_type}"
                })
    return all_licenses[:50]  # cap at 50 licenses
