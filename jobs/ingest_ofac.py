#!/usr/bin/env python3
"""
OFAC Sanctions List Ingestion - Daily
Downloads Treasury OFAC SDN XML and creates person_risk_signal entries.
Free, no API key required. Updated daily by Treasury.
"""
import os, requests, xml.etree.ElementTree as ET, psycopg2, sys
from datetime import datetime
from psycopg2.extras import execute_batch

URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
DSN = os.getenv("DB_DSN")

if not DSN:
    sys.exit("DB_DSN not set")

def parse_date(d):
    """Parse OFAC date format MM/DD/YYYY"""
    try:
        return datetime.strptime(d, "%m/%d/%Y").date()
    except:
        return None

def main():
    print("Fetching OFAC SDN list...")
    resp = requests.get(URL, timeout=60)
    resp.raise_for_status()
    
    root = ET.fromstring(resp.content)
    records = []
    
    # Get publish date
    publish_dt = None
    for pub in root.findall(".//publishInformation"):
        date_str = pub.find("Publish_Date").text
        publish_dt = parse_date(date_str)
        break
    
    # Parse SDN entries
    for sdn in root.findall(".//sdnEntry"):
        uid = sdn.find("uid").text
        last = (sdn.find("lastName").text or "").upper()
        first = (sdn.find("firstName").text or "").upper()
        full_name = f"{last}, {first}".strip(", ")
        
        sdn_type = sdn.find("sdnType").text
        records.append({
            "uid": uid,
            "name": full_name,
            "last": last,
            "first": first,
            "type": sdn_type,
            "date": publish_dt
        })
    
    print(f"Parsed {len(records)} OFAC entries")
    
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            # Upsert person_raw
            execute_batch(cur, """
                INSERT INTO person_raw 
                (src_name, src_row_id, last_name_std, first_name_std, hash_blob)
                VALUES ('treasury_ofac', %(uid)s, %(last)s, %(first)s, %(uid)s)
                ON CONFLICT (src_name, src_row_id) DO NOTHING
            """, records)
            
            # Get raw_id -> canon_id mapping
            cur.execute("""
                SELECT person_raw_id, src_row_id 
                FROM person_raw 
                WHERE src_name = 'treasury_ofac'
            """)
            mapping = {row[1]: row[0] for row in cur.fetchall()}
            
            # Create canon entries if needed
            for raw_id in mapping.values():
                cur.execute("""
                    INSERT INTO person_raw_canon (person_raw_id, person_canon_id, match_score)
                    SELECT %s, gen_random_uuid(), 0.99
                    WHERE NOT EXISTS (
                        SELECT 1 FROM person_raw_canon WHERE person_raw_id = %s
                    )
                """, (raw_id, raw_id))
            
            # Get canon IDs
            cur.execute("""
                SELECT prc.person_canon_id, pr.src_row_id
                FROM person_raw_canon prc
                JOIN person_raw pr ON pr.person_raw_id = prc.person_raw_id
                WHERE pr.src_name = 'treasury_ofac'
            """)
            canon_map = {row[1]: row[0] for row in cur.fetchall()}
            
            # Insert risk signals
            signals = []
            for rec in records:
                canon_id = canon_map.get(rec["uid"])
                if canon_id:
                    signals.append({
                        "canon": str(canon_id),
                        "date": rec["date"],
                        "uid": rec["uid"],
                        "name": rec["name"],
                        "type": rec["type"]
                    })
            
            execute_batch(cur, """
                INSERT INTO person_risk_signal 
                (person_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
                VALUES (%(canon)s, 'ofac', %(date)s, 10, 'treasury_ofac', %(uid)s, 
                        jsonb_build_object('name', %(name)s, 'type', %(type)s))
                ON CONFLICT DO NOTHING
            """, signals)
            
            conn.commit()
    
    print(f"✔ OFAC: {len(records)} entries processed")

if __name__ == "__main__":
    main()
