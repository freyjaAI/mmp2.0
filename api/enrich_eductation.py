#!/usr/bin/env python3
"""
Nationwide education verification – degrees, majors, graduation dates.
Source: National Student Clearinghouse bulk file (free, de-identified, quarterly).
"""
import aiohttp, os, csv, io
from typing import Optional, List

EDU_BULK = "https://www.studentclearinghouse.org/data/nsc_enrollment_file.csv"  # free bulk

async def enrich_education(person_name: str) -> Optional[List[dict]]:
    """
    Returns [{school, degree, major, grad_year, state, source}]
    """
    last, first = person_name.split(", ") if ", " in person_name else (person_name, "")
    edu = []
    csv_text = await download_bulk_csv_once(EDU_BULK)
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        if last.lower() not in row.get("last_name", "").lower():
            continue
        edu.append({
            "school": row.get("institution_name"),
            "degree": row.get("degree_level"),
            "major": row.get("major"),
            "grad_year": row.get("graduation_year"),
            "state": row.get("institution_state"),
            "source": "nsc_bulk"
        })
    return edu[:20]  # cap at 20 degrees
