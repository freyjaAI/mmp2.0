from fastapi import APIRouter, UploadFile, File
from api.db import fetch_many
import csv, io, pandas as pd

router = APIRouter(prefix="/clear", tags=["bulk"])

@router.post("/bulk")
async def bulk_lookup(file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode('utf-8')), dtype=str)
    ids = df['person_canon_id'].dropna().unique().tolist()
    if len(ids) > 10_000:
        return {"error": "Max 10,000 IDs per request"}
    
    results = await fetch_many(ids)
    return {"count": len(results), "data": results}
