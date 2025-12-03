"""Universal search - finds anyone by name using external APIs"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx
import os
import json
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
                print(f"[DATA AXLE] Searching person with email: {name}, {email}")
                response = await client.post(
                    "https://platform.data-axle.com/api/people/search",
                    headers={"X-AUTH-TOKEN": data_axle_key},
                    json={"name": name, "email": email, "limit": 1}
                )
                print(f"[DATA AXLE] Person email search status: {response.status_code}")
                print(f"[DATA AXLE] Response: {response.text[:500]}")
                if response.status_code == 200:
                    people = response.json().get("documents", [])
                    if people:
                        return people[0]
            except Exception as e:
                print(f"[DATA AXLE] Email search error: {e}")
        
        # Fallback: search by name only
        print(f"[DATA AXLE] Searching person by name only: {name}")
        response = await client.post(
            "https://platform.data-axle.com/api/people/search",
            headers={"X-AUTH-TOKEN": data_axle_key},
            json={"name": name, "limit": 1}
        )
        print(f"[DATA AXLE] Person name search status: {response.status_code}")
        print(f"[DATA AXLE] Response: {response.text[:500]}")
        if response.status_code == 200:
            people = response.json().get("documents", [])
            if people:
                return people[0]
    
    return None

async def search_data_axle_business(name: str, address: str = None, ein: str = None) -> dict:
    """Search Data Axle for a business, with EIN priority, then fallback to name search"""
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    if not data_axle_key:
        raise HTTPException(500, "Data Axle API key not configured")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # PRIORITY 1: Try EIN search first if provided
        if ein:
            try:
                print(f"[DATA AXLE] Searching business by EIN: {ein}")
                response = await client.post(
                    "https://platform.data-axle.com/api/businesses/search",
                    headers={"X-AUTH-TOKEN": data_axle_key},
                    json={"ein": ein, "limit": 1}
                )
                print(f"[DATA AXLE] Business EIN search status: {response.status_code}")
                print(f"[DATA AXLE] Response: {response.text[:500]}")
                if response.status_code == 200:
                    businesses = response.json().get("documents", [])
                    if businesses:
                        return businesses[0]
            except Exception as e:
                print(f"[DATA AXLE] EIN search error: {e}")
        
        # PRIORITY 2: Try with address if provided
        if address:
            try:
                print(f"[DATA AXLE] Searching business with address: {name}, {address}")
                response = await client.post(
                    "https://platform.data-axle.com/api/businesses/search",
                    headers={"X-AUTH-TOKEN": data_axle_key},
                    json={"name": name, "address": address, "limit": 1}
                )
                print(f"[DATA AXLE] Business address search status: {response.status_code}")
                print(f"[DATA AXLE] Response: {response.text[:500]}")
                if response.status_code == 200:
                    businesses = response.json().get("documents", [])
                    if businesses:
                        return businesses[0]
            except Exception as e:
                print(f"[DATA AXLE] Address search error: {e}")
        
        # PRIORITY 3: Fallback to name-only search
        print(f"[DATA AXLE] Searching business by name only: {name}")
        response = await client.post(
            "https://platform.data-axle.com/api/businesses/search",
            headers={"X-AUTH-TOKEN": data_axle_key},
            json={"name": name, "limit": 1}
        )
        print(f"[DATA AXLE] Business name search status: {response.status_code}")
        print(f"[DATA AXLE] Response: {response.text[:500]}")
        if response.status_code == 200:
            businesses = response.json().get("documents", [])
            if businesses:
                return businesses[0]
    
    return None

@router.post("/universal-search", response_model=UniversalSearchResponse)
async def universal_search(request: UniversalSearchRequest, bg: BackgroundTasks):
    """Universal search endpoint - searches Data Axle and enriches"""
    
    print(f"[SEARCH] Received request: type={request.entity_type}, name={request.name}, ein={request.ein}")
    
    # Search Data Axle based on entity type
    result = None
    if request.entity_type.lower() == "business":
        result = await search_data_axle_business(request.name, request.address, request.ein)
    else:
        result = await search_data_axle_person(request.name, request.email)
    
    if not result:
        print(f"[SEARCH] No {request.entity_type} found")
        raise HTTPException(404, {"detail": f"No {request.entity_type} found with name '{request.name}'"})
    
    print(f"[SEARCH] Found {request.entity_type}")
    
    # Generate canon_id
    canon_id = str(uuid4())
    
    # Store in database
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entities (canon_id, name, entity_type, source, raw_data)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (canon_id) DO NOTHING
            """,
            canon_id, request.name, request.entity_type, "data_axle", str(result)
        )
    
    # Trigger enrichment in background
    bg.add_task(trigger_enrichments_async, canon_id, request.entity_type, request.name, request.email)
    
    return UniversalSearchResponse(
        canon_id=canon_id,
        name=request.name,
        entity_type=request.entity_type,
        status="enriching",
        message=f"Found {request.entity_type}, starting enrichment"
    )
