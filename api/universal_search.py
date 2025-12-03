"""Universal search - finds anyone by name using multiple external APIs"""
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

async def search_opencorporates(name: str) -> dict:
    """Search OpenCorporates API (free, 500 requests/month)"""
    print(f"[OPENCORPORATES] Searching for business: {name}")
    
    # OpenCorporates provides 500 free API calls per month
    # Sign up at https://opencorporates.com/api_accounts/new for API token
    oc_token = os.getenv("OPENCORPORATES_API_TOKEN")  # Optional, works without token but with rate limits
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            params = {"q": name, "jurisdiction_code": "us"}
            if oc_token:
                params["api_token"] = oc_token
            
            response = await client.get(
                "https://api.opencorporates.com/v0.4/companies/search",
                params=params
            )
            print(f"[OPENCORPORATES] Status: {response.status_code}")
            print(f"[OPENCORPORATES] Response: {response.text[:500]}")
            
            if response.status_code == 200:
                data = response.json()
                companies = data.get("results", {}).get("companies", [])
                if companies:
                    # Return the first match
                    company = companies[0].get("company", {})
                    return {
                        "source": "opencorporates",
                        "name": company.get("name"),
                        "company_number": company.get("company_number"),
                        "jurisdiction": company.get("jurisdiction_code"),
                        "status": company.get("current_status"),
                        "address": company.get("registered_address_in_full"),
                        "incorporation_date": company.get("incorporation_date"),
                        "company_type": company.get("company_type"),
                        "url": company.get("opencorporates_url")
                    }
        except Exception as e:
            print(f"[OPENCORPORATES] Error: {e}")
    
    return None

async def search_data_axle_financial(company_name: str = None, company_id: str = None) -> dict:
    """Search Data Axle Financial Data API (you have access to this)"""
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    if not data_axle_key:
        print("[DATA AXLE] API key not configured")
        return None
    
    print(f"[DATA AXLE] Searching financial data: name={company_name}, id={company_id}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # Build query parameters
            params = {}
            if company_name:
                params["q"] = company_name
            if company_id:
                params["company_id"] = company_id
            
            response = await client.get(
                "https://platform.data-axle.com/v1/financial_data_combined/query",
                headers={"Authorization": f"Bearer {data_axle_key}"},
                params=params
            )
            print(f"[DATA AXLE] Financial search status: {response.status_code}")
            print(f"[DATA AXLE] Response: {response.text[:500]}")
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if results:
                    company = results[0]
                    return {
                        "source": "data_axle_financial",
                        "company_id": company.get("company_id"),
                        "name": company.get("company_name"),
                        "address": company.get("address"),
                        "revenue": company.get("revenue"),
                        "employees": company.get("employee_count"),
                        "industry": company.get("industry")
                    }
        except Exception as e:
            print(f"[DATA AXLE] Financial search error: {e}")
    
    return None

async def search_business(name: str, ein: str = None) -> dict:
    """Search for business using multiple sources"""
    print(f"[BUSINESS SEARCH] name={name}, ein={ein}")
    
    # Try OpenCorporates first (free, good US coverage)
    result = await search_opencorporates(name)
    if result:
        return result
    
    # Try Data Axle Financial Data (you have access)
    result = await search_data_axle_financial(company_name=name)
    if result:
        return result
    
    # TODO: Add more sources as needed:
    # - Secretary of State APIs (state-specific, often free)
    # - IRS EIN verification (requires different setup)
    # - Middesk API (paid, but good for EIN verification)
    # - EINsearch.com API (paid)
    
    return None

async def search_person(name: str, email: str = None) -> dict:
    """Search for person - placeholder for future implementation"""
    print(f"[PERSON SEARCH] name={name}, email={email}")
    
    # TODO: Implement person search with these options:
    # - Whitepages API (paid, but good US person data)
    # - Melissa Personator API (paid)
    # - People Data Labs API (paid)
    # - Searchbug API (paid)
    # - FastPeopleSearch.com (scraping, not recommended)
    
    # For now, return placeholder
    return {
        "source": "placeholder",
        "name": name,
        "email": email,
        "note": "Person search not yet implemented - add Whitepages or similar API"
    }

@router.post("/universal-search", response_model=UniversalSearchResponse)
async def universal_search(request: UniversalSearchRequest, bg: BackgroundTasks):
    """Universal search endpoint - searches multiple data sources"""
    
    print(f"[SEARCH] Received: type={request.entity_type}, name={request.name}, ein={request.ein}")
    
    # Search based on entity type
    result = None
    if request.entity_type.lower() == "business":
        result = await search_business(request.name, request.ein)
    else:
        result = await search_person(request.name, request.email)
    
    if not result:
        print(f"[SEARCH] No {request.entity_type} found")
        raise HTTPException(404, {"detail": f"No {request.entity_type} found with name '{request.name}'"})
    
    print(f"[SEARCH] Found {request.entity_type} from {result.get('source', 'unknown')}")
    
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
            canon_id, request.name, request.entity_type, result.get("source", "unknown"), json.dumps(result)
        )
    
    # Trigger enrichment in background
    bg.add_task(trigger_enrichments_async, canon_id, request.entity_type, request.name, request.email)
    
    return UniversalSearchResponse(
        canon_id=canon_id,
        name=request.name,
        entity_type=request.entity_type,
        status="enriching",
        message=f"Found {request.entity_type} from {result.get('source', 'unknown')}, starting enrichment"
    )
