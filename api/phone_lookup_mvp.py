"""Phone Lookup MVP - Simple working version using GET requests"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import os
from typing import List, Optional

router = APIRouter(prefix="/api", tags=["phone-lookup"])

# Data Axle API configuration
DATA_AXLE_API_KEY = os.getenv("DATA_AXLE_API_KEY")
DATA_AXLE_BASE_URL = "https://api.data-axle.com/v1/places"

class PhoneLookupRequest(BaseModel):
    business_name: str
    owner_name: Optional[str] = None
    ein: Optional[str] = None

class Associate(BaseModel):
    name: str
    phone: Optional[str] = None
    address: Optional[str] = None
    title: Optional[str] = None

class PhoneLookupResponse(BaseModel):
    business_name: str
    business_phone: Optional[str] = None
    business_address: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_address: Optional[str] = None
    associates: List[Associate] = []
    data_source: str = "data-axle"

@router.post("/phone-lookup-mvp", response_model=PhoneLookupResponse)
async def lookup_phone(request: PhoneLookupRequest):
    """
    Phone Lookup MVP - Find business contact info using simple GET requests
    """
    
    if not DATA_AXLE_API_KEY:
        raise HTTPException(status_code=500, detail="Data Axle API key not configured")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = {"X-AUTH-TOKEN": DATA_AXLE_API_KEY}
            
            # Use simple GET request with query parameters
            search_url = f"{DATA_AXLE_BASE_URL}/search"
            params = {
                "query": request.business_name,
                "limit": 5
            }
            
            response = await client.get(
                search_url,
                headers=headers,
                params=params
            )
            
            # Handle non-200 responses
            if response.status_code != 200:
                # Return empty result instead of crashing
                return PhoneLookupResponse(
                    business_name=request.business_name,
                    data_source="no-results"
                )
            
            # Try to parse JSON, handle any errors
            try:
                data = response.json()
            except Exception:
                return PhoneLookupResponse(
                    business_name=request.business_name,
                    data_source="json-error"
                )
            
            # Extract results
            results = data.get("documents", []) if isinstance(data, dict) else []
            
            if not results:
                return PhoneLookupResponse(
                    business_name=request.business_name,
                    data_source="no-results"
                )
            
            # Get first result
            business = results[0] if results else {}
            
            # Extract business info
        business_phone = business.get("phone")
        
        # Build address from component fields
        address_parts = [
            business.get("street"),
            business.get("city"),
            business.get("state"),
            business.get("zip")
        ]
        business_address = ", ".join(p for p in address_parts if p)                 
                    return PhoneLookupResponse(
                business_name=request.business_name,
                business_phone=business_phone,
                business_address=business_address,
                owner_name=request.owner_name,
                data_source="data-axle-success"
            )
            
    except httpx.TimeoutException:
        return PhoneLookupResponse(
            business_name=request.business_name,
            data_source="timeout"
        )
    except Exception as e:
        return PhoneLookupResponse(
            business_name=request.business_name,
            data_source=f"error: {str(e)[:50]}"
        )
