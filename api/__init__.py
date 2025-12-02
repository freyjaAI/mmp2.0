"""API package for MMP 2.0 Risk Analytics.

This package contains all API endpoints and supporting modules.
"""

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
# from api.billing import router as billing_router
from api.bulk import router as bulk_router
from api.clear_clone import router as clear_router
from api.search import router as search_router
from api.universal_search import router as universal_search_router

# Create main FastAPI app
app = FastAPI(
    title="MMP 2.0 Risk Analytics API",
    description="Production-grade risk intelligence system",
    version="2.0"
)

# Include all routers
# app.include_router(billing_router)
app.include_router(bulk_router)
app.include_router(clear_router)
app.include_router(search_router)
app.include_router(universal_search_router)

# Dashboard route
@app.get("/dashboard")
async def get_dashboard():
    dashboard_path = Path(__file__).parent.parent / "dashboard.html"
    return FileResponse(str(dashboard_path))

@app.get("/search")
async def get_search_dashboard():
    search_path = Path(__file__).parent.parent / "portal/search_dashboard.html"
    return FileResponse(str(search_path))
    

@app.get("/")
def root():
    return {"message": "MMP 2.0 Risk Analytics API", "docs": "/docs"}
