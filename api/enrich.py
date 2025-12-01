#!/usr/bin/env python3
"""
Lazy enrichment module - only fetch external data when requested.
Consumes free API tokens (Data Axle 6K/mo, A-Leads 60K/mo) on-demand.
"""
import os, asyncio, httpx, psycopg2, json
from typing import Optional
from datetime import datetime

DSN = os.getenv("DB_DSN")
DATA_AXLE_KEY = os.getenv("DATA_AXLE_API_KEY", "")
A_LEADS_KEY = os.getenv("A_LEADS_API_KEY", "")

# Free quota limits
FREE_LIMITS = {"data_axle": 6000, "a_leads": 60000}

def get_monthly_usage(source: str) -> int:
    """Check how many free tokens used this month"""
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(SUM(lookups),0) 
                    FROM api_cost_log 
                    WHERE source=%s AND log_date >= date_trunc('month', CURRENT_DATE)
                """, (source,))
                return cur.fetchone()[0]
    except Exception as e:
        print(f"Error checking usage for {source}: {e}")
        return 999999  # Fail safe - assume quota exceeded

def can_enrich(source: str) -> bool:
    """Check if we have free quota remaining"""
    used = get_monthly_usage(source)
    limit = FREE_LIMITS.get(source, 0)
    return used < limit

async def enrich_person_contact(person_canon_id: str, best_name: str):
    """Fetch phone/email from A-Leads if not already in DB"""
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM person_contact 
                    WHERE person_canon_id=%s AND src_name='a_leads'
                """, (person_canon_id,))
                if cur.fetchone()[0] > 0:
                    return  # already have it
    except Exception as e:
        print(f"DB check error: {e}")
        return
    
    if not can_enrich("a_leads"):
        print(f"A-Leads quota exceeded")
        return
    
    if not A_LEADS_KEY:
        print("A_LEADS_API_KEY not configured")
        return
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                "https://app.a-leads.co/api/v2/search",
                json={"names": [best_name], "fields": ["phone", "email"], "limit": 1},
                headers={"X-API-Key": A_LEADS_KEY}
            )
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            if not results:
                return
            
            contact = results[0]
            phone = contact.get("phone", "")[-10:] if contact.get("phone") else None
            email_full = contact.get("email", "")
            email = email_full.split("@")[0].lower() if email_full and "@" in email_full else None
            
            with psycopg2.connect(DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO person_contact 
                        (person_canon_id, src_name, src_row_id, phone10, email_local, first_reported)
                        VALUES (%s, 'a_leads', %s, %s, %s, CURRENT_DATE)
                        ON CONFLICT (person_canon_id, src_name, src_row_id) DO NOTHING
                    """, (person_canon_id, contact.get("id", "unknown"), phone, email))
                    
                    cur.execute("""
                        INSERT INTO api_cost_log (source, lookups, cost_cents)
                        VALUES ('a_leads', 1, 0)
                    """)
                    conn.commit()
            
            print(f"✔ A-Leads enriched: {best_name}")
            
        except Exception as e:
            print(f"A-Leads error for {best_name}: {e}")

async def enrich_bankruptcy(person_canon_id: str, best_name: str):
    """Check CourtListener for bankruptcy if not already in DB"""
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM person_risk_signal 
                    WHERE person_canon_id=%s AND signal_type='bankruptcy'
                """, (person_canon_id,))
                if cur.fetchone()[0] > 0:
                    return
    except Exception as e:
        print(f"DB check error: {e}")
        return
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            last = best_name.split(",")[0].strip() if "," in best_name else best_name
            
            resp = await client.get(
                "https://www.courtlistener.com/api/rest/v3/dockets/",
                params={
                    "q": f'debtor:"{last}"',
                    "type": "bk",
                    "order_by": "dateFiled desc",
                    "page_size": 3
                }
            )
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            for case in results:
                try:
                    with psycopg2.connect(DSN) as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO person_risk_signal 
                                (person_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
                                VALUES (%s, 'bankruptcy', %s, 8, 'courtlistener_bk', %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                person_canon_id,
                                case.get("date_filed"),
                                case.get("docket_number", "unknown"),
                                json.dumps({
                                    "case_name": case.get("case_name", ""),
                                    "court": case.get("court", "")
                                })
                            ))
                            conn.commit()
                    print(f"✔ CourtListener found bankruptcy for {best_name}")
                except Exception as e:
                    print(f"DB insert error: {e}")
            
        except Exception as e:
            print(f"CourtListener error for {best_name}: {e}")

async def enrich_business_firmographics(business_canon_id: str, legal_name: str):
    """Fetch firmographics from Data Axle if not already in DB"""
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM business_risk_signal 
                    WHERE business_canon_id=%s AND signal_type='firmographics'
                """, (business_canon_id,))
                if cur.fetchone()[0] > 0:
                    return
    except Exception as e:
        print(f"DB check error: {e}")
        return
    
    if not can_enrich("data_axle"):
        print("Data Axle quota exceeded")
        return
    
    if not DATA_AXLE_KEY:
        print("DATA_AXLE_API_KEY not configured")
        return
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                "https://api.data-axle.com/v2/businesses/search",
                json={
                    "name": [legal_name],
                    "select": "name,employees,sales_volume,sic_code",
                    "limit": 1
                },
                headers={"Authorization": f"Bearer {DATA_AXLE_KEY}"}
            )
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            if not results:
                return
            
            biz = results[0]
            
            with psycopg2.connect(DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO business_risk_signal 
                        (business_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
                        VALUES (%s, 'firmographics', CURRENT_DATE, 3, 'data_axle', %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        business_canon_id,
                        biz.get("id", "unknown"),
                        json.dumps({
                            "employees": biz.get("employees", 0),
                            "sales": biz.get("sales_volume", 0),
                            "sic": biz.get("sic_code", "")
                        })
                    ))
                    
                    cur.execute("""
                        INSERT INTO api_cost_log (source, lookups, cost_cents)
                        VALUES ('data_axle', 1, 0)
                    """)
                    conn.commit()
            
            print(f"✔ Data Axle enriched: {legal_name}")
            
        except Exception as e:
            print(f"Data Axle error for {legal_name}: {e}")

def trigger_enrichments_async(entity_type: str, entity_id: str, entity_data: dict):
    """Non-blocking enrichment trigger"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        if entity_type == "person":
            tasks = [
                enrich_person_contact(entity_id, entity_data.get("best_name", "")),
                enrich_bankruptcy(entity_id, entity_data.get("best_name", ""))
            ]
        elif entity_type == "business":
            tasks = [
                enrich_business_firmographics(entity_id, entity_data.get("legal_name", ""))
            ]
        else:
            return
        
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.close()
    except Exception as e:
        print(f"Enrichment error: {e}")
