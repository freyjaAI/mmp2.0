"""Phone Lookup MVP - Simple endpoint for business owner + associates contact info"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import os
from typing import List, Optional

router = APIRouter(prefix="/api", tags=["phone-lookup"])

class Associate(BaseModel):
    name: str
    phone: Optional[str] = None
    address: Optional[str] = None
    relationship: str

class PhoneLookupResponse(BaseModel):
    business_name: str
    business_phone: Optional[str] = None
    business_address: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_address: Optional[str] = None
    associates: List[Associate] = []

class PhoneLookupRequest(BaseModel):
    business_name: str

@router.post("/phone-lookup-mvp", response_model=PhoneLookupResponse)
async def phone_lookup_mvp(request: PhoneLookupRequest):
    """
    Phone lookup MVP - returns business + owner + associates contact info
    Uses Data Axle API (free tier) with public page scraping fallback
    """
    business_name = request.business_name
    print(f"[PHONE LOOKUP MVP] Searching for: {business_name}")
    
    # Initialize response
    response = PhoneLookupResponse(
        business_name=business_name,
        associates=[]
    )
    
    # Step 1: Search Data Axle Business API
    data_axle_key = os.getenv("DATA_AXLE_API_KEY")
    
    if data_axle_key:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Try Data Axle Business Search API
                url = "https://platform.data-axle.com/api/businesses/search"
                headers = {"Authorization": f"Bearer {data_axle_key}"}
                payload = {
                    "name": business_name,
                    "select": "name,address,phone,owner_name",
                    "limit": 1
                }
                
                print(f"[DATA AXLE] Searching business: {url}")
                resp = await client.post(url, json=payload, headers=headers, timeout=15)
                
                print(f"[DATA AXLE] Business search status: {resp.status_code}")
                print(f"[DATA AXLE] Response: {resp.text[:500]}")
                
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", [])
                    if results and len(results) > 0:
                        business = results[0]
                        response.business_phone = business.get("phone", "")
                        response.business_address = business.get("address", "")
                        response.owner_name = business.get("owner_name", "")
                        print(f"[DATA AXLE] Found business: {business.get('name')}")
        except Exception as e:
            print(f"[DATA AXLE] Business search error: {e}")
    
    # Step 2: If we have owner name, search Data Axle Person API for owner contact
    if response.owner_name and data_axle_key:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                url = "https://platform.data-axle.com/api/people/search"
                headers = {"Authorization": f"Bearer {data_axle_key}"}
                payload = {
                    "name": response.owner_name,
                    "select": "name,address,phone,job_title",
                    "limit": 5
                }
                
                print(f"[DATA AXLE] Searching owner: {response.owner_name}")
                resp = await client.post(url, json=payload, headers=headers, timeout=15)
                
                print(f"[DATA AXLE] Person search status: {resp.status_code}")
                
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", [])
                    if results and len(results) > 0:
                        # Use first person result
                        owner = results[0]
                        response.owner_phone = owner.get("phone", "")
                        response.owner_address = owner.get("address", "")
                        print(f"[DATA AXLE] Found owner contact")
                        
                        # Remaining people are associates
                        for i, person in enumerate(results[1:5], 1):  # Cap at 4 associates
                            associate = Associate(
                                name=person.get("name", ""),
                                phone=person.get("phone", ""),
                                address=person.get("address", ""),
                                relationship=person.get("job_title", "associate")
                            )
                            response.associates.append(associate)
        except Exception as e:
            print(f"[DATA AXLE] Person search error: {e}")
    
    # Step 3: Fallback to OpenCorporates for basic business info if Data Axle failed
    if not response.business_address:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                oc_token = os.getenv("OPENCORPORATES_API_TOKEN")
                params = {"q": business_name, "jurisdiction_code": "us"}
                if oc_token:
                    params["api_token"] = oc_token
                
                print(f"[OPENCORPORATES] Fallback search")
                resp = await client.get(
                    "https://api.opencorporates.com/v0.4/companies/search",
                    params=params,
                    timeout=15
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    companies = data.get("results", {}).get("companies", [])
                    if companies:
                        company = companies[0].get("company", {})
                        response.business_address = company.get("registered_address_in_full", "")
                        print(f"[OPENCORPORATES] Found business address")
        except Exception as e:
            print(f"[OPENCORPORATES] Error: {e}")
    
    # Check if we found any data
    if not response.business_phone and not response.business_address and not response.owner_phone:
        raise HTTPException(
            status_code=404,
            detail=f"No contact information found for business: {business_name}"
        )
    
    return response
