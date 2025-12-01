import os
import stripe
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from api.db import get_db

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
router = APIRouter()

@router.post("/portal/checkout")
async def create_checkout_session(request: Request):
    """Create Stripe checkout session for pay-as-you-go billing"""
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="setup",
            success_url=f"{request.base_url}portal/success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{request.base_url}portal",
            metadata={
                "product": "mmp2.0_api_access"
            }
        )
        return {"id": session.id}
    except Exception as e:
        return {"error": str(e)}

@router.get("/portal/success")
async def checkout_success(session_id: str):
    """Handle successful checkout - create API key in database"""
    try:
        # Retrieve session to get customer ID
        session = stripe.checkout.Session.retrieve(session_id)
        customer_id = session.customer
        
        if not customer_id:
            return RedirectResponse(url="/portal?error=no_customer")
        
        # Insert API key into database
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute(
            "INSERT INTO api_keys (stripe_customer_id) VALUES (%s) RETURNING api_key",
            (customer_id,)
        )
        api_key = cur.fetchone()[0]
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Return success page with API key
        return {
            "success": True,
            "api_key": str(api_key),
            "message": "Your API key has been created. Store it securely - it won't be shown again."
        }
    except Exception as e:
        return RedirectResponse(url=f"/portal?error={str(e)}")
