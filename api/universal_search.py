"""Universal search - finds anyone by name using external APIs"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
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
    email: str = None
    address: str = None
    ein: str = None  # Employer Identification Number for business search

class UniversalSearchResponse(BaseModel):
    canon_id: str
    name: str
    entity_type: str
    status: str
    message: str

async def search_data_axle_person(name: str, email: str = None) -> dict:
    """Search Data Axle for a person, with fallback to name-only search"""
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    if not data_axle_key:
        raise HTTPException(500, "Data Axle API key not configured")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Try with email first if provided
        if email:
            try:
                response = await client.post(
                    "https://platform.data-axle.com/v2/people/search",
                    headers={"Authorization": f"Bearer {data_axle_key}"},
                    json={"name": name, "email": email, "limit": 1}
                )
                if response.status_code == 200:
                    results = response.json().get("results", [])
                    if results:
                        return results[0]
            except:
                pass  # Fall through to name-only search
        
        # Fallback: name-only search
        try:
            response = await client.post(
                "https://platform.data-axle.com/v2/people/search",
                headers={"Authorization": f"Bearer {data_axle_key}"},
                json={"name": name, "limit": 1}
            )
            
            # Handle HTML 404 error page from Data Axle
            if response.status_code == 404 or "Error 404" in response.text:
                return None
            
            if response.status_code != 200:
                return None
            
            results = response.json().get("results", [])
            return results[0] if results else None
        
        except Exception:
            return None

async def search_data_axle_business(name: str, address: str = None, ein: str = None) -> dict:
    """Search Data Axle for a business, with fallback to name-only search"""
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    if not data_axle_key:
        raise HTTPException(500, "Data Axle API key not configured")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Try with EIN first if provided
        if ein:
            try:
                response = await client.request(
                    "GET",
                    "https://api.data-axle.com/v1/places/search",
                    headers={"X-AUTH-TOKEN": data_axle_key},
                    json={"filter": {"relation": "in", "attribute": "eins", "value": [ein]}, "limit": 1}
                )
                if response.status_code == 200:
                    data = response.json()
                    documents = data.get("documents", [])
                    if documents:
                        return documents[0]
            except:
                pass  # Fall through to address search
        
        # Try with address first if provided
        if address:
            try:
                response = await client.request(
                    "GET",
                    "https://api.data-axle.com/v1/places/search",
                    headers={"X-AUTH-TOKEN": data_axle_key},
                    json={"query": f"{name} {address}", "limit": 1}
                )
                if response.status_code == 200:
                    data = response.json()
                    documents = data.get("documents", [])
                    if documents:
                        return documents[0]
            except:
                pass  # Fall through to name-only search
        
        # Fallback: name-only search
        try:
            response = await client.request(
                "GET",
                "https://api.data-axle.com/v1/places/search",
                headers={"X-AUTH-TOKEN": data_axle_key},
                json={"query": name, "limit": 1}
            )
            
            # Handle HTML 404 error page from Data Axle
            if response.status_code == 404 or "Error 404" in response.text:
                return None
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            documents = data.get("documents", [])
            return documents[0] if documents else None
        
        except Exception:
            return None

@router.post("/universal-search")
async def universal_search(request: UniversalSearchRequest, background_tasks: BackgroundTasks):
    """
    Universal search: finds anyone by name, creates record if needed, enriches
    """
    try:
        if request.entity_type == "person":
            # Search Data Axle for person
            person_data = await search_data_axle_person(request.name, request.email)
            
            if not person_data:
                raise HTTPException(404, f"No person found with name '{request.name}'")
            
            # Check if person exists in database
            pool = await get_pool()
            async with pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT person_canon_id FROM person WHERE best_name ILIKE $1",
                    person_data.get("name", request.name)
                )
                
                if existing:
                    canon_id = existing["person_canon_id"]
                else:
                    # Create new person record
                    canon_id = f"0{uuid4().hex[:7]}-1234-1234-1234-{uuid4().hex[:12]}"
                    await conn.execute(
                        """
                        INSERT INTO person (person_canon_id, best_name)
                        VALUES ($1, $2)
                        """,
                        canon_id,
                        person_data.get("name", request.name)
                    )
                
                # Trigger enrichment in background
                background_tasks.add_task(
                    trigger_enrichments_async,
                    "person",
                    canon_id,
                    person_data
                )
                
                return UniversalSearchResponse(
                    canon_id=canon_id,
                    name=person_data.get("name", request.name),
                    entity_type="person",
                    status="enriching",
                    message="Person found and enrichment started"
                )
        
        else:  # business
            # Search Data Axle for business
            business_data = await search_data_axle_business(request.name, request.address, request.ein)
            
            if not business_data:
                raise HTTPException(404, f"No business found with name '{request.name}'")
            
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
                
                # Trigger enrichment in background
                background_tasks.add_task(
                    trigger_enrichments_async,
                    "business",
                    canon_id,
                    business_data
                )
                
                return UniversalSearchResponse(
                    canon_id=canon_id,
                    name=business_data.get("name", request.name),
                    entity_type="business",
                    status="enriching",
                    message="Business found and enrichment started"
                )
    
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(504, "External API timeout - please try again")
    except Exception as e:
        raise HTTPException(500, f"Search error: {str(e)}")

__all__ = ["router"]
