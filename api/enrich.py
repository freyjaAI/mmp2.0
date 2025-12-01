#!/usr/bin/env python3
"""
Nationwide lazy enrichment orchestrator – triggers external APIs only when fields missing.
Caches results in Redis for 1 hour.
"""
import asyncio, os, json, redis, psycopg2
from typing import Optional, List, Dict
from api.enrich_bankruptcy import enrich_bankruptcy
from api.enrich_federal_cl import enrich_federal_cases
from api.enrich_sec import enrich_sec_officer
from api.enrich_breach import enrich_breach
from api.enrich_domain import enrich_domain
from api.enrich_vehicles import enrich_vehicles
from api.enrich_boat import enrich_boat
from api.enrich_aircraft import enrich_aircraft
from api.enrich_eviction import enrich_eviction
from api.enrich_relatives_deep import enrich_relatives_deep
from api.enrich_professional_licenses import enrich_professional_licenses
from api.enrich_education import enrich_education
from api.enrich_employment_deep import enrich_employment_deep
from api.enrich_social_deep import enrich_social_deep

REDIS_URL = os.getenv("REDIS_URL")
ENRICH_TTL = 3600  # 1 hour cache

async def enrich_person(person_canon_id: str, base: dict) -> dict:
    """
    Returns enriched dict (phone, email, bankruptcy, etc.)
    If missing → triggers background API call → store → cache.
    """
    r = redis.from_url(REDIS_URL, decode_responses=True)
    key = f"enrich:person:{person_canon_id}"
    cached = await r.get(key)
    if cached:
        base.update(json.loads(cached))
        return base

    # decide what to fetch
    missing = []
    if not base.get("phone"): missing.append("phone")
    if not base.get("email"): missing.append("email")
    if not base.get("bankruptcy"): missing.append("bankruptcy")
    if not base.get("federal_cases"): missing.append("federal_cases")
    if not base.get("sec_filings"): missing.append("sec_filings")
    if not base.get("breach_count"): missing.append("breach_count")
    if not base.get("domains"): missing.append("domains")
    if not base.get("vehicles"): missing.append("vehicles")
    if not base.get("boat"): missing.append("boat")
    if not base.get("aircraft"): missing.append("aircraft")
    if not base.get("eviction_count"): missing.append("eviction_count")
    if not base.get("relatives_deep"): missing.append("relatives_deep")
    if not base.get("professional_licenses"): missing.append("professional_licenses")
    if not base.get("education"): missing.append("education")
    if not base.get("employment_deep"): missing.append("employment_deep")
    if not base.get("social_deep"): missing.append("social_deep")

    # background fetch (fire-and-forget)
    if missing:
        asyncio.create_task(_background_fetch(person_canon_id, base, missing))

    # return base + flag
    base["_enriching"] = missing
    return base

async def _background_fetch(person_canon_id: str, base: dict, missing: List[str]):
    """
    Async fetch + store + cache.
    """
    try:
        # fetch base data from DB
        with psycopg2.connect(os.getenv("DB_DSN")) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT best_name, email_local
                    FROM person_canon
                    WHERE person_canon_id = %s
                """, (person_canon_id,))
                row = cur.fetchone()
                if not row:
                    return
                entity_data = {"best_name": row[0], "email_local": row[1] or ""}

        # ---- bankruptcy ----
        if "bankruptcy" in missing:
            tasks.append(enrich_bankruptcy(person_canon_id, entity_data.get("best_name", "")))
        # ---- federal cases ----
        if "federal_cases" in missing:
            tasks.append(enrich_federal_cases(client, entity_data.get("best_name", "")))
        # ---- SEC filings ----
        if "sec_filings" in missing:
            tasks.append(enrich_sec_officer(client, entity_data.get("best_name", "")))
        # ---- breach history ----
        if "breach_count" in missing:
            tasks.append(enrich_breach(client, entity_data.get("email_local", "")))
        # ---- domain ownership ----
        if "domains" in missing:
            tasks.append(enrich_domain(client, entity_data.get("email_local", "")))
        # ---- vehicles ----
        if "vehicles" in missing:
            tasks.append(enrich_vehicles(client, entity_data.get("best_name", "")))
        # ---- boats ----
        if "boat" in missing:
            tasks.append(enrich_boat(client, entity_data.get("best_name", "")))
        # ---- aircraft ----
        if "aircraft" in missing:
            tasks.append(enrich_aircraft(client, entity_data.get("best_name", "")))
        # ---- eviction records ----
        if "eviction_count" in missing:
            tasks.append(enrich_eviction(client, entity_data.get("best_name", "")))

        # ---- relatives deep ----
        if "relatives_deep" in missing:
            tasks.append(enrich_relatives_deep(entity_data.get("best_name", "")))
        # ---- professional licenses ----
        if "professional_licenses" in missing:
            tasks.append(enrich_professional_licenses(entity_data.get("best_name", "")))
        # ---- education deep ----
        if "education" in missing:
            tasks.append(enrich_education(entity_data.get("best_name", "")))
        # ---- employment deep ----
        if "employment_deep" in missing:
            tasks.append(enrich_employment_deep(entity_data.get("best_name", "")))
        # ---- aircraft ----
        if "aircraft" in missing:
            tasks.append(enrich_aircraft(entity_data.get("best_name", "")))
        # ---- boat ----
        if "boat" in missing:
            tasks.append(enrich_boat(entity_data.get("best_name", "")))
        # ---- social deep ----
        if "social_deep" in missing:
            tasks.append(enrich_social_deep(entity_data.get("best_name", "")))

        # run all async fetches
        results = await asyncio.gather(*tasks, return_exceptions=True)
        merged = {}
        for r in results:
            if isinstance(r, dict):
                merged.update(r)

        # store DB + Redis
        await _store_enriched(person_canon_id, merged)

    except Exception as e:
        print(f"enrich bg error: {e}")

async def _store_enriched(person_canon_id: str, data: dict):
    """
    Store to DB + Redis cache.
    """
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.setex(f"enrich:person:{person_canon_id}", ENRICH_TTL, json.dumps(data))
    # optional DB store (if you want persistent copy)
    with psycopg2.connect(os.getenv("DB_DSN")) as conn:
        with conn.cursor() as cur:
            if data.get("phone") or data.get("email"):
                cur.execute("""
                    INSERT INTO person_contact (person_canon_id, src_name, src_row_id, phone10, email_local, first_reported)
                    VALUES (%s, 'lazy_enrich', %s, %s, %s, CURRENT_DATE)
                    ON CONFLICT DO NOTHING
                """, (person_canon_id, "lazy", data.get("phone"), data.get("email")))
            if data.get("bankruptcy"):
                cur.execute("""
                    INSERT INTO person_risk_signal (person_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
                    VALUES (%s, 'bankruptcy', %s, 8, 'courtlistener_lazy', %s, %s)
                    ON CONFLICT DO NOTHING
                """, (person_canon_id, data["bankruptcy_filed"], "lazy", json.dumps({"chapter": data["bankruptcy_chapter"]})))
