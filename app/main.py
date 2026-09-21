"""FastAPI application: lab sensor timeline merging service."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from . import service
from .database import init_db
from .models import (
    CreateSessionRequest,
    PullResponse,
    QuarantineResponse,
    ReportRequest,
    ReportResponse,
    SessionResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Dependencies and the database schema are initialised here, so starting
    # the process is all the operator needs to do.
    init_db()
    yield


app = FastAPI(
    title="Laboratory Sensor Timeline API",
    description=(
        "Merges out-of-order batch readings from a fixed set of lab sensors "
        "into one deterministic timeline using a per-session watermark."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(service.ApiError)
async def api_error_handler(request, exc: service.ApiError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sessions", response_model=SessionResponse, status_code=201,
          tags=["sessions"])
def create_session(body: CreateSessionRequest) -> dict:
    return service.create_session(body.sources, body.allowed_lateness_seconds)


@app.get("/sessions/{session_id}", response_model=SessionResponse,
         tags=["sessions"])
def get_session(session_id: str) -> dict:
    return service.get_session(session_id)


@app.post("/sessions/{session_id}/readings", response_model=ReportResponse,
          tags=["readings"])
def report_readings(session_id: str, body: ReportRequest) -> dict:
    return service.report_readings(session_id, body.readings)


@app.get("/sessions/{session_id}/stream", response_model=PullResponse,
         tags=["readings"])
def pull_stream(
    session_id: str,
    after: int = Query(0, ge=0, description="Only return readings with publish_seq greater than this."),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    return service.pull_published(session_id, after=after, limit=limit)


@app.get("/sessions/{session_id}/quarantine", response_model=QuarantineResponse,
         tags=["readings"])
def list_quarantined(session_id: str) -> dict:
    return service.list_quarantined(session_id)
