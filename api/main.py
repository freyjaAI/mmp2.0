"""
MMP 2.0 - Risk Analytics API
FastAPI endpoint for risk intelligence reports
"""

from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
import json
from api.clear_clone import router as clear_router

app = FastAPI(
    title="MMP 2.0 Risk Analytics API",
    description="Production-grade risk intelligence system",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include CLEAR clone router
app.include_router(clear_router)

# Seed endpoint (temporary)
@app.get("/seed")
def seed_database():
    from api import init_db
    init_db.init()
    return {"status": "success", "message": "Database seeded"}

# Database connection
def get_db_connection():
    """Get PostgreSQL database connection"""
    dsn = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/riskdb')
    try:
        conn = psycopg2.connect(dsn)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Response models
class RiskScoreResponse(BaseModel):
    overall: int
    level: str
    categories: Dict[str, int]
    breakdown: Dict[str, Any]

class PersonInfo(BaseModel):
    name: Optional[str]
    dob: Optional[str]
    address: Optional[str]
    confidence: Optional[float]
    flags: Optional[Dict[str, bool]]

class RiskSignal(BaseModel):
    signal_type: str
    event_count: int
    latest_event: Optional[str]
    earliest_event: Optional[str]
    max_severity: int
    avg_severity: float
    sources: list

class RiskTimelineEvent(BaseModel):
    date: Optional[str]
    type: str
    severity: int
    source: str
    details: Optional[Dict[str, Any]]

class BusinessRelationship(BaseModel):
    business_canon_id: str
    business_name: Optional[str]
    relationship_type: str
    start_date: Optional[str]
    end_date: Optional[str]

class RiskReportResponse(BaseModel):
    person_canon_id: str
    generated_at: str
    person: PersonInfo
    risk_score: RiskScoreResponse
    signals: list
    timeline: list
    business_relationships: list
    source_citations: Dict[str, int]

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "MMP 2.0 Risk Analytics API"
    }

# Main risk report endpoint
@app.get("/risk-report/{person_canon_id}", response_model=RiskReportResponse)
async def get_risk_report(
    person_canon_id: str = Path(..., description="UUID of the canonical person entity")
):
    """
    Get comprehensive risk intelligence report for a person
    
    This endpoint returns:
    - Overall risk score (1-999) with categorical breakdown
    - Risk level classification (CLEAR to CRITICAL)
    - Aggregated risk signals by type
    - Chronological timeline of risk events
    - Business relationships and affiliations
    - Source citations
    
    Replaces enterprise solutions like Thomson Reuters CLEAR at a fraction of the cost.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Call the main PostgreSQL function that builds the complete report
        cur.execute(
            "SELECT get_person_risk_report(%s) as report",
            (person_canon_id,)
        )
        
        result = cur.fetchone()
        
        if not result or not result['report']:
            raise HTTPException(
                status_code=404,
                detail=f"Person with ID {person_canon_id} not found"
            )
        
        report_data = result['report']
        
        # Check if error in report
        if 'error' in report_data:
            raise HTTPException(
                status_code=404,
                detail=report_data['error']
            )
        
        return report_data
        
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

# Quick score endpoint (lightweight version)
@app.get("/risk-score/{person_canon_id}")
async def get_risk_score(
    person_canon_id: str = Path(..., description="UUID of the canonical person entity")
):
    """
    Get just the risk score without full report details
    Faster endpoint for score-only queries
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Call risk scoring function
        cur.execute(
            "SELECT * FROM calculate_person_risk_score(%s)",
            (person_canon_id,)
        )
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Person with ID {person_canon_id} not found"
            )
        
        return {
            "person_canon_id": person_canon_id,
            "score": result['overall_score'],
            "level": result['risk_level'],
            "categories": result['category_scores'],
            "breakdown": result['risk_breakdown']
        }
        
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

# Risk signals endpoint
@app.get("/risk-signals/{person_canon_id}")
async def get_risk_signals(
    person_canon_id: str = Path(..., description="UUID of the canonical person entity")
):
    """
    Get aggregated risk signals for a person
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute(
            "SELECT * FROM get_person_risk_signals(%s)",
            (person_canon_id,)
        )
        
        signals = cur.fetchall()
        
        return {
            "person_canon_id": person_canon_id,
            "signals": [dict(signal) for signal in signals]
        }
        
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

# Timeline endpoint
@app.get("/risk-timeline/{person_canon_id}")
async def get_risk_timeline(
    person_canon_id: str = Path(..., description="UUID of the canonical person entity"),
    limit: int = 50
):
    """
    Get chronological timeline of risk events
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute(
            "SELECT * FROM get_person_risk_timeline(%s, %s)",
            (person_canon_id, limit)
        )
        
        timeline = cur.fetchall()
        
        return {
            "person_canon_id": person_canon_id,
            "timeline": [dict(event) for event in timeline]
        }
        
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
