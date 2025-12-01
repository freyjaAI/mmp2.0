from fastapi import APIRouter, Response
from typing import Dict
import redis
import os
import io
import base64
from datetime import datetime

router = APIRouter(prefix="/visual", tags=["visuals"])

# ==================== RISK GAUGE SVG ====================

def risk_gauge_svg(score: int) -> str:
    """
    Generate an SVG risk gauge (0-100 dial)
    Low risk (0-33): Green
    Medium risk (34-66): Yellow
    High risk (67-100): Red
    """
    # Determine color
    if score <= 33:
        color = "#10b981"  # Green
        label = "LOW RISK"
    elif score <= 66:
        color = "#f59e0b"  # Yellow/Amber
        label = "MEDIUM RISK"
    else:
        color = "#ef4444"  # Red
        label = "HIGH RISK"
    
    # Calculate needle rotation (0-100 maps to -90 to 90 degrees)
    rotation = (score / 100 * 180) - 90
    
    svg = f'''<svg width="200" height="120" xmlns="http://www.w3.org/2000/svg">
        <!-- Background arc -->
        <path d="M 20 100 A 80 80 0 0 1 180 100" fill="none" stroke="#e5e7eb" stroke-width="15"/>
        <!-- Colored arc (based on score) -->
        <path d="M 20 100 A 80 80 0 0 1 {20 + (score/100 * 160)} {100 - (score/100 * 80)}" fill="none" stroke="{color}" stroke-width="15"/>
        <!-- Center pivot -->
        <circle cx="100" cy="100" r="8" fill="#374151"/>
        <!-- Needle -->
        <line x1="100" y1="100" x2="100" y2="30" stroke="#374151" stroke-width="3" transform="rotate({rotation} 100 100)"/>
        <!-- Score text -->
        <text x="100" y="85" text-anchor="middle" font-size="24" font-weight="bold" fill="{color}">{score}</text>
        <!-- Label -->
        <text x="100" y="115" text-anchor="middle" font-size="10" fill="#6b7280">{label}</text>
    </svg>'''
    
    return svg

@router.get("/gauge")
def get_risk_gauge(score: int = 50):
    """Return SVG risk gauge for given score (0-100)"""
    svg_content = risk_gauge_svg(score)
    return Response(content=svg_content, media_type="image/svg+xml")

# ==================== TIMELINE STUB ====================

@router.get("/timeline")
def get_timeline(person_canon_id: str):
    """
    Return timeline visualization (stub for now)
    Future: Generate PNG with matplotlib showing criminal/eviction/bankruptcy events
    """
    return {"message": "Timeline visualization coming soon", "person_canon_id": person_canon_id}

# ==================== NETWORK GRAPH STUB ====================

@router.get("/network")
def get_network_graph(person_canon_id: str):
    """
    Return network graph visualization (stub for now)
    Future: Generate PNG with networkx showing associates and business connections
    """
    return {"message": "Network graph visualization coming soon", "person_canon_id": person_canon_id}
