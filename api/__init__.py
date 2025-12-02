"""API package for MMP 2.0 Risk Analytics.

This package contains all API endpoints and supporting modules.
"""

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
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
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MMP 2.0 - Universal Search</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background: #0f1419; color: #e6edf3; padding: 40px 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        h1 { font-size: 32px; margin-bottom: 8px; color: #fff; }
        .subtitle { color: #8b949e; margin-bottom: 40px; }
        .search-box { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 24px; margin-bottom: 20px; }
        label { display: block; margin-bottom: 8px; color: #c9d1d9; font-weight: 500; }
        input, select { width: 100%; padding: 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #e6edf3; font-size: 16px; margin-bottom: 16px; }
        input:focus, select:focus { outline: none; border-color: #58a6ff; }
        button { width: 100%; padding: 12px 24px; background: #238636; border: none; border-radius: 6px; color: #fff; font-size: 16px; font-weight: 600; cursor: pointer; transition: background 0.2s; }
        button:hover { background: #2ea043; }
        button:active { background: #1f7a2e; }
        button:disabled { background: #21262d; color: #8b949e; cursor: not-allowed; }
        .result { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-top: 20px; }
        .result h3 { color: #58a6ff; margin-bottom: 12px; }
        .result pre { background: #0d1117; padding: 16px; border-radius: 6px; overflow-x: auto; font-size: 14px; line-height: 1.5; }
        .error { background: #490b0b; border-color: #da3633; }
        .success { background: #0e3f1c; border-color: #238636; }
        .loading { text-align: center; padding: 40px; color: #8b949e; }
    </style>
</head>
<body>
    <div class="container">
        <h1>MMP 2.0 Universal Search</h1>
        <div class="subtitle">Search for any person or business - we'll find them and build a risk report</div>
        <div class="search-box">
            <label for="name">Name</label>
            <input type="text" id="name" placeholder="e.g., John Smith or Tesla Inc">
            <label for="entity_type">Type</label>
            <select id="entity_type">
                <option value="person">Person</option>
                <option value="business">Business</option>
            </select>
            <button onclick="search()" id="searchBtn">Search</button>
        </div>
        <div id="results"></div>
    </div>
    <script>
        async function search() {
            const name = document.getElementById('name').value.trim();
            const entity_type = document.getElementById('entity_type').value;
            const resultsDiv = document.getElementById('results');
            const searchBtn = document.getElementById('searchBtn');
            if (!name) {
                resultsDiv.innerHTML = '<div class="result error"><h3>Error</h3><p>Please enter a name</p></div>';
                return;
            }
            searchBtn.disabled = true;
            searchBtn.textContent = 'Searching...';
            resultsDiv.innerHTML = '<div class="loading">Searching Data Axle API...</div>';
            try {
                const response = await fetch('/api/universal-search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, entity_type })
                });
                const data = await response.json();
                if (response.ok) {
                    resultsDiv.innerHTML = `
                        <div class="result success">
                            <h3>✓ Search Complete</h3>
                            <p style="margin: 12px 0; color: #7ee787;">
                                Canon ID: <strong>${data.canon_id}</strong>
                            </p>
                            <p style="margin-bottom: 12px; color: #c9d1d9;">
                                ${data.message || 'Record created. Background enrichment started.'}
                            </p>
                            <pre>${JSON.stringify(data, null, 2)}</pre>
                            <p style="margin-top: 12px; color: #8b949e; font-size: 14px;">
                                View full report at: <a href="/clear/${entity_type}/${data.canon_id}" style="color: #58a6ff;">/clear/${entity_type}/${data.canon_id}</a>
                            </p>
                        </div>
                    `;
                } else {
                    resultsDiv.innerHTML = `
                        <div class="result error">
                            <h3>Error</h3>
                            <pre>${JSON.stringify(data, null, 2)}</pre>
                        </div>
                    `;
                }
            } catch (error) {
                resultsDiv.innerHTML = `
                    <div class="result error">
                        <h3>Error</h3>
                        <p>${error.message}</p>
                    </div>
                `;
            } finally {
                searchBtn.disabled = false;
                searchBtn.textContent = 'Search';
            }
        }
        document.getElementById('name').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') search();
        });
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)
