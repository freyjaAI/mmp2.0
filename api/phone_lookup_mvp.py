"""Phone Lookup MVP - Returns business owner + associates contact info"""
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
    relationship: str

class PhoneLookupResponse(BaseModel):
    business_name: str
    business_phone: Optional[str] = None
    business_address: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_address: Optional[str] = None
    associates: List[Associate] = []

@router.post("/phone-lookup", response_model=PhoneLookupResponse)
async def lookup_phone(request: PhoneLookupRequest):
    """
    Lookup business owner and associates contact information.
    Accepts: business_name, owner_name (optional), ein (optional)
    Returns: phone numbers and addresses for business owner and associates
    """
    
    if not DATA_AXLE_API_KEY:
        raise HTTPException(status_code=500, detail="Data Axle API key not configured")
    
    try:
        async with httpx.AsyncClient() as client:
            # Search for business using Data Axle Places API
            search_params = {
                "company": request.business_name,
                "limit": 10
            }
            
            # Add optional parameters if provided
            if request.ein:
                search_params["ein"] = request.ein
            if request.owner_name:
                search_params["contact_name"] = request.owner_name
            
            headers = {
                "Authorization": f"Bearer {DATA_AXLE_API_KEY}",
                "Content-Type": "application/json"
            }
            
            # Query Data Axle Places API for business information
            business_response = await client.get(
                f"{DATA_AXLE_BASE_URL}/places/search",
                headers=headers,
                params=search_params,
                timeout=30.0
            )
            
            if business_response.status_code != 200:
                raise HTTPException(
                    status_code=business_response.status_code,
                    detail=f"Data Axle API error: {business_response.text}"
                )
            
            business_data = business_response.json()
            
            # Check if we got any results
            if not business_data.get("results") or len(business_data["results"]) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No contact information found for business: {request.business_name}"
                )
            
            # Get the first matching business
            business = business_data["results"][0]
            
            # Extract business contact information
            response_data = PhoneLookupResponse(
                business_name=business.get("company_name", request.business_name),
                business_phone=business.get("phone"),
                business_address=format_address(business),
                owner_name=request.owner_name,
                associates=[]
            )
            
            # If we have a contact name from the business, use it
            if business.get("contact_name"):
                response_data.owner_name = business.get("contact_name")
            
            # Try to get additional contact information for owner
            if response_data.owner_name:
                try:
                    contact_response = await client.get(
                        f"{DATA_AXLE_BASE_URL}/contacts/search",
                        headers=headers,
                        params={
                            "name": response_data.owner_name,
                            "company": request.business_name,
                            "limit": 5
                        },
                        timeout=30.0
                    )
                    
                    if contact_response.status_code == 200:
                        contact_data = contact_response.json()
                        if contact_data.get("results"):
                            owner_contact = contact_data["results"][0]
                            response_data.owner_phone = owner_contact.get("phone")
                            response_data.owner_address = format_address(owner_contact)
                            
                            # Add other contacts as associates
                            for contact in contact_data["results"][1:]:
                                associate = Associate(
                                    name=contact.get("full_name", "Unknown"),
                                    phone=contact.get("phone"),
                                    address=format_address(contact),
                                    relationship=contact.get("title", "Associate")
                                )
                                response_data.associates.append(associate)
                except Exception:
                    # If contact lookup fails, continue with business data only
                    pass
            
            return response_data
            
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Data Axle API timeout")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Error connecting to Data Axle API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

def format_address(data: dict) -> Optional[str]:
    """Format address from Data Axle response data"""
    address_parts = []
    
    if data.get("address"):
        address_parts.append(data["address"])
    if data.get("city"):
        address_parts.append(data["city"])
    if data.get("state"):
        address_parts.append(data["state"])
    if data.get("zip"):
        address_parts.append(data["zip"])
    
    return ", ".join(address_parts) if address_parts else None
