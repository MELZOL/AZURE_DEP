from pydantic import BaseModel, Field 
from typing import Optional, List 
from datetime import date 
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from db import fetch_rows
import json

app = FastAPI(title="Azure SQL Demo API", version="1.0")

class Health(BaseModel):
    ok: bool


class EventFilter(BaseModel):
    customer_code: int = Field(...,description="e.g. 9001")
    start : date | None = Field(None, description = "YYYY-MM-DD")
    end : date | None = Field(None, description = "YYYY-MM-DD")
    min_amount : float |None = Field(None , ge=0)

class Event(BaseModel):
    event_id : str
    amount : float
    payload_json : Optional[dict] = None


@app.post("/events/search", response_model = List[Event])
def search_events(filter: EventFilter):
    rows = fetch_rows("""
        SELECT event_id, amount, payload_json, event_time_utc
        FROM dbo.ingested_events
        WHERE customer_id = :code
        ORDER BY event_time_utc DESC
        OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY
    """,{
        "code": filter.customer_code,
        "start": filter.start,
        "end": filter.end,
        "min_amount": filter.min_amount,
    })

    for r in rows:
        v = r.get("payload_json")
        if isinstance(v, str):
            v = v.strip()
            if v:
                try:
                    r["payload_json"] = json.loads(v)
                except Exception:
                    r["payload_json"] = None  # or leave as original string if you prefer
    return rows
    