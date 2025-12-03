"""Phone Lookup MVP - Returns business owner + associates contact info using Data Axle APIs"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import os
from typing import List, Optional

router = APIRouter(prefix="/api", tags=["phone-lookup"])

# Data Axle API configuration
DATA_AXLE_API_KEY = os.getenv("DATA_AXLE_API_KEY")
DATA_AXLE_BASE_URL = "https://api.data-axle.com/v1"

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

@router.post("/phone-lookup-mvp", response_model=PhoneLookupResponse)
async def lookup_phone(request: PhoneLookupRequest):
    """
    Phone Lookup MVP - Find business owner and associates contact info.
    
    Input: business_name, owner_name (optional), ein (optional)
    Output: Owner phone/address + Associates with their phone/address
    """
    
    if not DATA_AXLE_API_KEY:
        raise HTTPException(status_code=500, detail="Data Axle API key not configured")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            headers = {
                "Authorization": f"Bearer {DATA_AXLE_API_KEY}",
                "Content-Type": "application/json"
            }
            
            # STEP 1: Find the business using appropriate API
            business_data = None
            
            # If EIN provided, use Financial Data API
            if request.ein:
                financial_response = await client.post(
                    f"{DATA_AXLE_BASE_URL}/financial_data/search",
                    headers=headers,
                    json={
                        "filter": {
                            "relation": "equals",
                            "attribute": "company_ein",
                            "value": request.ein
                        },
                        "limit": 1,
                        "fields": ["company_name", "company_ein", "company_address", "company_city", "company_state", "company_postal_code", "phone"]
                    }
                )
                
                if financial_response.status_code == 200:
                    financial_data = financial_response.json()
                    if financial_data.get("documents") and len(financial_data["documents"]) > 0:
                        business_data = financial_data["documents"][0]
            
            # If no EIN or EIN search failed, use Places API with business name
            if not business_data:
                places_response = await client.post(
                    f"{DATA_AXLE_BASE_URL}/places/search",
                    headers=headers,
                    json={
                        "query": request.business_name,
                        "limit": 5,
                        "fields": ["name", "phone", "street", "city", "state", "zip", "id"]
                    }
                )
                
                if places_response.status_code != 200:
                    raise HTTPException(
                        status_code=places_response.status_code,
                        detail=f"Data Axle Places API error: {places_response.text}"
                    )
                
                places_data = places_response.json()
                
                if not places_data.get("documents") or len(places_data["documents"]) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No business found for: {request.business_name}"
                    )
                
                business_data = places_data["documents"][0]
            
            # Extract business information
            response_data = PhoneLookupResponse(
                business_name=business_data.get("name") or business_data.get("company_name", request.business_name),
                business_phone=business_data.get("phone"),
                business_address=format_address(business_data),
                owner_name=request.owner_name,
                associates=[]
            )
            
            # STEP 2: Get contacts/people at the business (for owner + associates)
            # Use Places API with contacts perspective
            place_id = business_data.get("id")
            
            if place_id:
                contacts_response = await client.post(
                    f"{DATA_AXLE_BASE_URL}/places/search",
                    headers=headers,
                    json={
                        "filter": {
                            "relation": "equals",
                            "attribute": "id",
                            "value": place_id
                        },
                        "perspective": "contacts",
                        "limit": 20,
                        "fields": ["first_name", "last_name", "phone", "street", "city", "state", "zip", "professional_title"]
                    }
                )
                
                if contacts_response.status_code == 200:
                    contacts_data = contacts_response.json()
                    
                    if contacts_data.get("documents"):
                        for contact in contacts_data["documents"]:
                            contact_name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
                            
                            if not contact_name:
                                continue
                            
                            # Check if this is the owner
                            if request.owner_name and request.owner_name.lower() in contact_name.lower():
                                response_data.owner_name = contact_name
                                response_data.owner_phone = contact.get("phone")
                                response_data.owner_address = format_address(contact)
                            else:
                                # Add as associate
                                associate = Associate(
                                    name=contact_name,
                                    phone=contact.get("phone"),
                                    address=format_address(contact),
                                    title=contact.get("professional_title")
                                )
                                response_data.associates.append(associate)
            
            return response_data
            
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Data Axle API timeout")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Error connecting to Data Axle API: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def format_address(data: dict) -> Optional[str]:
    """Format address from Data Axle response data"""
    address_parts = []
    
    # Handle different field names from different APIs
    street = data.get("street") or data.get("company_address")
    city = data.get("city") or data.get("company_city")
    state = data.get("state") or data.get("company_state")
    zip_code = data.get("zip") or data.get("company_postal_code")
    
    if street:
        address_parts.append(street)
    if city:
        address_parts.append(city)
    if state:
        address_parts.append(state)
    if zip_code:
        address_parts.append(str(zip_code))
    
    return ", ".join(address_parts) if address_parts else None
