import os, asyncpg
from typing import List

dsn = os.getenv("DB_DSN")
pool = None

async def get_pool():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(dsn, min_size=10, max_size=25)
    return pool

async def fetch_many(person_ids: List[str]):
    pool = await get_pool()
    sql = """
        SELECT person_canon_id, best_name, best_dob, confidence_score, flags
        FROM   person_canon
        WHERE  person_canon_id = ANY($1)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, person_ids)
    return [dict(r) for r in rows]
