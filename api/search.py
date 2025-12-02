"""Search API for finding people and businesses by name"""

from fastapi import APIRouter, Query
from typing import List, Dict, Any
import psycopg2
from api.db import get_pool

router = APIRouter(prefix="/api", tags=["search"])

@router.get("/search")
async def search_entities(
    query: str = Query(..., description="Name to search for"),
    type: str = Query("person", description="Entity type: person or business")
) -> List[Dict[str, Any]]:
    """
    Search for people or businesses by name
    """
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        if type == "person":
            # Search in person table by best_name
            results = await conn.fetch(
                """
                SELECT person_canon_id, best_name
                FROM person
                WHERE best_name ILIKE $1
                LIMIT 10
                """,
                f"%{query}%"
            )
            return [{"person_canon_id": r["person_canon_id"], "best_name": r["best_name"]} for r in results]
        else:
            # Search in business table by business_name
            results = await conn.fetch(
                """
                SELECT business_canon_id, business_name
                FROM business
                WHERE business_name ILIKE $1
                LIMIT 10
                """,
                f"%{query}%"
            )
            return [{"business_canon_id": r["business_canon_id"], "business_name": r["business_name"]} for r in results]

__all__ = ["router"]
