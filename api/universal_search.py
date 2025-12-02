"""Universal search - finds anyone by name using external APIs"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import os
from uuid import uuid4
from api.db import get_pool
from api.enrich import trigger_enrichments_async

router = APIRouter(prefix="/api", tags=["universal-search"])

class UniversalSearchRequest(BaseModel):
    name: str
    entity_type: str = "person"  # person or business

class UniversalSearchResponse(BaseModel):
    canon_id: str
    name: str
    entity_type: str
    status: str
    message: str

@router.post("/universal-search")
async def universal_search(request: UniversalSearchRequest):
    """
    Universal search: finds anyone by name, creates record if needed, enriches
    """
    # Step 1: Search Data Axle API
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    if not data_axle_key:
        raise HTTPException(500, "Data Axle API key not configured")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if request.entity_type == "person":
                # Search Data Axle for person
                response = await client.get(
                    "https://api.data-axle.com/v2/people/search",
                    headers={"Authorization": f"Bearer {data_axle_key}"},
                    params={"name": request.name, "limit": 1}
                )
                
                if response.status_code != 200:
                    raise HTTPException(500, f"Data Axle API error: {response.text}")
                
                results = response.json().get("results", [])
                if not results:
                    raise HTTPException(404, f"No person found with name '{request.name}'")
                
                person_data = results[0]
                
                # Step 2: Check if person exists in database
                pool = await get_pool()
                async with pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT person_canon_id FROM person WHERE best_name ILIKE $1",
                        person_data.get("name", request.name)
                    )
                    
                    if existing:
                        canon_id = existing["person_canon_id"]
                    else:
                        # Step 3: Create new person record
                        canon_id = f"0{uuid4().hex[:7]}-1234-1234-1234-{uuid4().hex[:12]}"
                        await conn.execute(
                            """
                            INSERT INTO person (person_canon_id, best_name)
                            VALUES ($1, $2)
                            """,
                            canon_id,
                            person_data.get("name", request.name)
                        )
                
                # Step 4: Trigger enrichment pipeline
                await trigger_enrichments_async("person", canon_id, person_data)
                
                return UniversalSearchResponse(
                    canon_id=canon_id,
                    name=person_data.get("name", request.name),
                    entity_type="person",
                    status="enriching",
                    message="Person found and enrichment started"
                )
            
            else:  # business
                # Similar logic for business
                response = await client.get(
                    "https://api.data-axle.com/v2/businesses/search",
                    headers={"Authorization": f"Bearer {data_axle_key}"},
                    params={"name": request.name, "limit": 1}
                )
                
                if response.status_code != 200:
                    raise HTTPException(500, f"Data Axle API error: {response.text}")
                
                results = response.json().get("results", [])
                if not results:
                    raise HTTPException(404, f"No business found with name '{request.name}'")
                
                business_data = results[0]
                
                pool = await get_pool()
                async with pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT business_canon_id FROM business WHERE business_name ILIKE $1",
                        business_data.get("name", request.name)
                    )
                    
                    if existing:
                        canon_id = existing["business_canon_id"]
                    else:
                        canon_id = f"0{uuid4().hex[:7]}-1234-1234-1234-{uuid4().hex[:12]}"
                        await conn.execute(
                            """
                            INSERT INTO business (business_canon_id, business_name)
                            VALUES ($1, $2)
                            """,
                            canon_id,
                            business_data.get("name", request.name)
                        )
                
                await trigger_enrichments_async("business", canon_id, business_data)
                
                return UniversalSearchResponse(
                    canon_id=canon_id,
                    name=business_data.get("name", request.name),
                    entity_type="business",
                    status="enriching",
                    message="Business found and enrichment started"
                )
    
    except httpx.TimeoutException:
        raise HTTPException(504, "External API timeout - please try again")
    except Exception as e:
        raise HTTPException(500, f"Search error: {str(e)}")

__all__ = ["router"]
