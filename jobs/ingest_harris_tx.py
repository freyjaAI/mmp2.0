#!/usr/bin/env python3
"""
Harris County TX Criminal Records Ingestion - Daily
Free Socrata API, no key required. ~1000/day limit.
"""
import os, requests, datetime, psycopg2, sys
from psycopg2.extras import execute_batch

URL = "https://data.harriscountytx.gov/resource/qqjv-iqi7.json"
DSN = os.getenv("DB_DSN")
BATCH = 1000
DATE_FMT = "%Y-%m-%dT%H:%M:%S.%f"

if not DSN:
    sys.exit("DB_DSN not set")

def main():
    print("Fetching Harris County arrests...")
    params = {"$limit": BATCH, "$order": "booking_date DESC"}
    
    resp = requests.get(URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    
    records = []
    for row in data:
        try:
            dob = datetime.datetime.strptime(row.get("date_of_birth", ""), "%Y%m%d").date() if row.get("date_of_birth") else None
            arrest = datetime.datetime.strptime(row["booking_date"], DATE_FMT).date() if row.get("booking_date") else None
            
            records.append({
                "booking": row.get("booking_number", ""),
                "last": row.get("last_name", "").upper(),
                "first": row.get("first_name", "").upper(),
                "dob": dob,
                "arrest": arrest,
                "charge": (row.get("charge_description", "") or "")[:80],
                "court": (row.get("court", "") or "")[:20]
            })
        except Exception as e:
            print(f"Parse error: {e}")
            continue
    
    if not records:
        print("No new arrests")
        return
    
    print(f"Parsed {len(records)} Harris County arrests")
    
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            # Upsert person_raw
            execute_batch(cur, """
                INSERT INTO person_raw 
                (src_name, src_row_id, last_name_std, first_name_std, dob, hash_blob)
                VALUES ('harris_tx', %(booking)s, %(last)s, %(first)s, %(dob)s, %(booking)s)
                ON CONFLICT (src_name, src_row_id) DO NOTHING
            """, records)
            
            # Get mapping
            cur.execute("""
                SELECT person_raw_id, src_row_id 
                FROM person_raw 
                WHERE src_name = 'harris_tx'
            """)
            mapping = {row[1]: row[0] for row in cur.fetchall()}
            
            # Create canon entries
            for raw_id in mapping.values():
                cur.execute("""
                    INSERT INTO person_raw_canon (person_raw_id, person_canon_id, match_score)
                    SELECT %s, gen_random_uuid(), 0.95
                    WHERE NOT EXISTS (
                        SELECT 1 FROM person_raw_canon WHERE person_raw_id = %s
                    )
                """, (raw_id, raw_id))
            
            # Get canon IDs
            cur.execute("""
                SELECT prc.person_canon_id, pr.src_row_id
                FROM person_raw_canon prc
                JOIN person_raw pr ON pr.person_raw_id = prc.person_raw_id
                WHERE pr.src_name = 'harris_tx'
            """)
            canon_map = {row[1]: row[0] for row in cur.fetchall()}
            
            # Insert risk signals
            signals = []
            for rec in records:
                canon_id = canon_map.get(rec["booking"])
                if canon_id and rec["arrest"]:
                    signals.append({
                        "canon": str(canon_id),
                        "arrest": rec["arrest"],
                        "booking": rec["booking"],
                        "charge": rec["charge"],
                        "court": rec["court"]
                    })
            
            execute_batch(cur, """
                INSERT INTO person_risk_signal 
                (person_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
                VALUES (%(canon)s, 'criminal', %(arrest)s, 7, 'harris_tx', %(booking)s,
                        jsonb_build_object('charge', %(charge)s, 'court', %(court)s))
                ON CONFLICT DO NOTHING
            """, signals)
            
            conn.commit()
    
    print(f"✔ Harris TX: {len(records)} arrests processed")

if __name__ == "__main__":
    main()
