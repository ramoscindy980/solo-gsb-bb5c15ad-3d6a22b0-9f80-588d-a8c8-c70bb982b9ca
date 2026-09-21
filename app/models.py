"""Pydantic request / response models.

Timestamps may be uploaded as ISO-8601 strings (timezone required) or as
numeric epoch seconds. Internally every timestamp is an epoch-seconds float;
responses always render timestamps as UTC ISO-8601 strings.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


def parse_observed_at(value: Any) -> float:
    """Validate an incoming timestamp and return epoch seconds (UTC)."""
    if isinstance(value, bool):
        raise ValueError("timestamp must be an ISO-8601 string or epoch number")
    if isinstance(value, (int, float)):
        epoch = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp must not be empty")
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid ISO-8601 timestamp: {value!r}") from exc
        if dt.tzinfo is None:
            raise ValueError(
                "naive timestamp is not allowed; include a timezone, "
                f"got {value!r}"
            )
        epoch = dt.timestamp()
    else:
        raise ValueError("timestamp must be an ISO-8601 string or epoch number")
    # Reject absurd values (datetime supports year 1..9999, keep some slack).
    if not (-10_000_000_000 < epoch < 10_000_000_000):
        raise ValueError("timestamp is out of range")
    return epoch


def to_iso(epoch: float | None) -> str | None:
    """Render epoch seconds as a UTC ISO-8601 string with milliseconds."""
    if epoch is None:
        return None
    return (
        datetime.fromtimestamp(epoch, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class CreateSessionRequest(BaseModel):
    #: Fixed list of sources allowed to report into this session.
    sources: list[str] = Field(min_length=1)
    #: A reading is "on time" while its time is within this many seconds of
    #: the slowest source. >= 0, may be fractional.
    allowed_lateness_seconds: float = Field(default=0.0, ge=0)

    @field_validator("sources")
    @classmethod
    def _validate_sources(cls, sources: list[str]) -> list[str]:
        cleaned = [s.strip() for s in sources]
        if any(not s for s in cleaned):
            raise ValueError("source ids must be non-empty")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("source ids must be unique")
        return cleaned


class ReadingIn(BaseModel):
    source_id: str = Field(min_length=1)
    seq: int = Field(ge=0)
    observed_at: Any
    payload: Any = None

    @field_validator("observed_at")
    @classmethod
    def _validate_observed_at(cls, value: Any) -> float:
        return parse_observed_at(value)

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("source_id must be non-empty")
        return value


class ReportRequest(BaseModel):
    readings: list[ReadingIn] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def _reject_duplicate_keys(self) -> "ReportRequest":
        seen: set[tuple[str, int]] = set()
        for item in self.readings:
            key = (item.source_id, item.seq)
            if key in seen:
                raise ValueError(
                    f"duplicate (source_id, seq) inside the same batch: {key}"
                )
            seen.add(key)
        return self


class ReadingOut(BaseModel):
    source_id: str
    seq: int
    observed_at: str
    payload: Any = None
    publish_seq: int
    watermark_at_publish: str | None = None


class QuarantineItem(BaseModel):
    source_id: str
    seq: int
    observed_at: str
    payload: Any = None
    quarantined_watermark: str
    received_at: str


class ReportedItem(BaseModel):
    source_id: str
    seq: int
    status: Literal["buffered", "published", "duplicate", "conflict", "quarantined"]
    publish_seq: int | None = None


class ReportResponse(BaseModel):
    session_id: str
    accepted: int
    duplicates: int
    conflicts: int
    quarantined: int
    published_count: int
    watermark: str | None
    published: list[ReadingOut]
    items: list[ReportedItem]


class PullResponse(BaseModel):
    session_id: str
    watermark: str | None
    next_after: int | None
    has_more: bool
    readings: list[ReadingOut]


class QuarantineResponse(BaseModel):
    session_id: str
    items: list[QuarantineItem]


class SessionResponse(BaseModel):
    session_id: str
    sources: list[str]
    allowed_lateness_seconds: float
    watermark: str | None
    sources_reported: int
    total_sources: int
    published_count: int
    quarantined_count: int
    buffered_count: int
    created_at: str
