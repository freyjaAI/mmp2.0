#!/usr/bin/env python3

import psycopg2, os, pathlib, sys

DSN = os.getenv("DB_DSN")

if not DSN:
    sys.exit("DB_DSN not set")

def init():
    sql_files = sorted(pathlib.Path("ddl").glob("*.sql"))
    
    with psycopg2.connect(DSN) as conn:
        for f in sql_files:
            with conn.cursor() as cur:
                cur.execute(f.read_text())
            print(f"✔ {f.name}")
    
    # seed
    seed_file = pathlib.Path("seed/01_seed.sql")
    if seed_file.exists():
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(seed_file.read_text())
        print("✔ seeded")
    
    print("DB ready")

if __name__ == "__main__":
    init()
