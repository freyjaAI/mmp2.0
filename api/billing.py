import os
import stripe
from fastapi import HTTPException, Request
from api.db import get_db

stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

def get_api_key(request: Request):
    """Extract and validate API key from request headers"""
    key = request.headers.get("X-API-Key")
    if not key:
        raise HTTPException(status_code=401, detail="API key required")
    return key

def meter_lookup(api_key: str, rows: int = 1):
    """Stripe metered billing - $0.50 per lookup row"""
    conn = get_db()
    cur = conn.cursor()
    
    # Get Stripe customer ID from API key
    cur.execute(
        "SELECT stripe_customer_id FROM api_keys WHERE api_key = %s",
        (api_key,)
    )
    result = cur.fetchone()
    
    if not result:
        raise HTTPException(status_code=402, detail="Invalid API key or payment required")
    
    customer_id = result[0]
    
    try:
        # Create metered usage record
        stripe.InvoiceItem.create(
            customer=customer_id,
            currency="usd",
            amount=int(rows * 50),  # $0.50 per row in cents
            description=f"{rows} CLEAR report lookup(s)"
        )
        return customer_id
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=402, detail=f"Payment failed: {str(e)}")
    finally:
        cur.close()
        conn.close()
