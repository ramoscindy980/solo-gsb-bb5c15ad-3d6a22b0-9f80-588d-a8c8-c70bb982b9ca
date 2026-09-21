"""Business logic: sessions, idempotent ingest, watermarks and publishing.

Watermark rule
--------------
Once *every* declared source has reported at least once, the session watermark
is::

    watermark = min over sources (latest observed_at(source)) - allowed_lateness

Only readings with ``observed_at <= watermark`` are published; the entire
boundary timestamp closes atomically so equal-time readings are emitted
together. The watermark is monotonic: it is only ever advanced, never moved
backwards. Publishing therefore appends to the output stream and cannot
rewrite history.

After a timestamp has been published, any further reading with
``observed_at <= published_watermark`` would be unable to take its correct
position (its equals/lower ties are already emitted). Such late readings go
to the quarantine table (viewable via the quarantine endpoint) and never
modify published output.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any

from .database import reader, transaction
from .models import to_iso


class ApiError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


# --------------------------------------------------------------------------- #
# Sessions
# --------------------------------------------------------------------------- #

def create_session(sources: list[str], allowed_lateness: float) -> dict[str, Any]:
    session_id = uuid.uuid4().hex
    now = time.time()
    with transaction() as conn:
        conn.execute(
            "INSERT INTO sessions (id, allowed_lateness_seconds, source_count, "
            "published_watermark, created_at) VALUES (?, ?, ?, NULL, ?)",
            (session_id, float(allowed_lateness), len(sources), now),
        )
        conn.executemany(
            "INSERT INTO session_sources (session_id, source_id, pos) VALUES (?, ?, ?)",
            [(session_id, source_id, pos) for pos, source_id in enumerate(sources)],
        )
    return get_session(session_id)


def _require_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise ApiError(404, f"session {session_id!r} not found")
    return row


def _get_source_ids(conn: sqlite3.Connection, session_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT source_id FROM session_sources WHERE session_id = ? ORDER BY pos",
        (session_id,),
    ).fetchall()
    return [r["source_id"] for r in rows]


def get_session(session_id: str) -> dict[str, Any]:
    with reader() as conn:
        row = _require_session(conn, session_id)
        sources = _get_source_ids(conn, session_id)
        reported = conn.execute(
            "SELECT COUNT(*) AS c FROM ("
            "  SELECT 1 FROM readings "
            "  WHERE session_id = ? AND status != 'quarantined' "
            "  GROUP BY source_id)",
            (session_id,),
        ).fetchone()["c"]
        counts = conn.execute(
            "SELECT status, COUNT(*) AS c FROM readings "
            "WHERE session_id = ? GROUP BY status",
            (session_id,),
        ).fetchall()
    by_status = {r["status"]: r["c"] for r in counts}
    return {
        "session_id": row["id"],
        "sources": sources,
        "allowed_lateness_seconds": row["allowed_lateness_seconds"],
        "watermark": to_iso(row["published_watermark"]),
        "sources_reported": reported,
        "total_sources": row["source_count"],
        "published_count": by_status.get("published", 0),
        "quarantined_count": by_status.get("quarantined", 0),
        "buffered_count": by_status.get("buffered", 0),
        "created_at": to_iso(row["created_at"]),
    }


# --------------------------------------------------------------------------- #
# Publishing helpers (must be called inside a transaction)
# --------------------------------------------------------------------------- #

def _row_to_reading_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": to_iso(row["observed_at"]),
        "payload": json.loads(row["payload"]) if row["payload"] is not None else None,
        "publish_seq": row["publish_seq"],
        "watermark_at_publish": to_iso(row["published_watermark"]),
    }


def _publish_below_watermark(
    conn: sqlite3.Connection, session_id: str, new_watermark: float
) -> list[dict[str, Any]]:
    """Publish buffered readings with observed_at at or below the watermark.

    The whole boundary timestamp closes atomically, so every reading sharing
    it is emitted together. Deterministic global order: observed_at, then
    source_id, then seq. publish_seq values are allocated contiguously.
    """
    max_seq_row = conn.execute(
        "SELECT COALESCE(MAX(publish_seq), 0) AS m FROM readings WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    next_seq = max_seq_row["m"] + 1

    pending = conn.execute(
        "SELECT id FROM readings "
        "WHERE session_id = ? AND status = 'buffered' AND observed_at <= ? "
        "ORDER BY observed_at ASC, source_id ASC, seq ASC",
        (session_id, new_watermark),
    ).fetchall()

    published: list[dict[str, Any]] = []
    for row in pending:
        conn.execute(
            "UPDATE readings SET status = 'published', publish_seq = ?, "
            "published_watermark = ? WHERE id = ?",
            (next_seq, new_watermark, row["id"]),
        )
        out = conn.execute("SELECT * FROM readings WHERE id = ?", (row["id"],)).fetchone()
        published.append(_row_to_reading_out(out))
        next_seq += 1

    conn.execute(
        "UPDATE sessions SET published_watermark = ? WHERE id = ?",
        (new_watermark, session_id),
    )
    return published


def _advance_watermark(
    conn: sqlite3.Connection, session: sqlite3.Row
) -> list[dict[str, Any]]:
    """Recompute the watermark from the latest reading of every source.

    A source's event time is the maximum ``observed_at`` it has reported
    (quarantined rows excluded). The session watermark is the minimum such
    time across all declared sources, minus the allowed lateness.
    """
    total_sources = session["source_count"]
    reported = conn.execute(
        "SELECT COUNT(*) AS c FROM ("
        "  SELECT 1 FROM readings "
        "  WHERE session_id = ? AND status != 'quarantined' "
        "  GROUP BY source_id)",
        (session["id"],),
    ).fetchone()["c"]
    if reported < total_sources:
        return []  # every source must report at least once first

    slowest = conn.execute(
        "SELECT MIN(mx) AS m FROM ("
        "  SELECT MAX(observed_at) AS mx FROM readings "
        "  WHERE session_id = ? AND status != 'quarantined' "
        "  GROUP BY source_id)",
        (session["id"],),
    ).fetchone()["m"]
    candidate = slowest - session["allowed_lateness_seconds"]

    current = session["published_watermark"]
    if current is not None and candidate <= current:
        return []  # watermark only advances

    return _publish_below_watermark(conn, session["id"], candidate)


# --------------------------------------------------------------------------- #
# Ingest
# --------------------------------------------------------------------------- #

def report_readings(
    session_id: str, items: list[Any]
) -> dict[str, Any]:
    now = time.time()
    item_results: list[dict[str, Any]] = []
    counters = {"buffered": 0, "published": 0, "duplicate": 0,
                "conflict": 0, "quarantined": 0}

    with transaction() as conn:
        session = _require_session(conn, session_id)
        source_rows = conn.execute(
            "SELECT source_id FROM session_sources WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        allowed_sources = {r["source_id"] for r in source_rows}

        for item in items:
            if item.source_id not in allowed_sources:
                raise ApiError(
                    400,
                    f"source_id {item.source_id!r} is not part of session "
                    f"{session_id!r}; declared sources: {sorted(allowed_sources)}",
                )

            incoming_payload = (
                json.dumps(item.payload, separators=(",", ":"), sort_keys=True)
                if item.payload is not None
                else None
            )

            existing = conn.execute(
                "SELECT * FROM readings WHERE session_id = ? AND source_id = ? "
                "AND seq = ?",
                (session_id, item.source_id, item.seq),
            ).fetchone()

            if existing is not None:
                # Idempotency: same content is a plain replay; a different
                # content under the same key is a conflict, never an overwrite.
                if (
                    existing["observed_at"] == item.observed_at
                    and existing["payload"] == incoming_payload
                ):
                    status = "duplicate"
                else:
                    status = "conflict"
                counters[status] += 1
                item_results.append({
                    "source_id": item.source_id,
                    "seq": item.seq,
                    "status": status,
                    "publish_seq": existing["publish_seq"],
                })
                continue

            published_wm = session["published_watermark"]
            if published_wm is not None and item.observed_at <= published_wm:
                # The boundary timestamp is already closed and published, so
                # this reading can no longer take its deterministic position.
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq, observed_at, "
                    "payload, status, publish_seq, published_watermark, "
                    "quarantined_watermark, received_at) "
                    "VALUES (?, ?, ?, ?, ?, 'quarantined', NULL, NULL, ?, ?)",
                    (session_id, item.source_id, item.seq, item.observed_at,
                     incoming_payload, published_wm, now),
                )
                counters["quarantined"] += 1
                item_results.append({
                    "source_id": item.source_id,
                    "seq": item.seq,
                    "status": "quarantined",
                    "publish_seq": None,
                })
                continue

            conn.execute(
                "INSERT INTO readings (session_id, source_id, seq, observed_at, "
                "payload, status, publish_seq, published_watermark, "
                "quarantined_watermark, received_at) "
                "VALUES (?, ?, ?, ?, ?, 'buffered', NULL, NULL, NULL, ?)",
                (session_id, item.source_id, item.seq, item.observed_at,
                 incoming_payload, now),
            )
            counters["buffered"] += 1
            item_results.append({
                "source_id": item.source_id,
                "seq": item.seq,
                "status": "buffered",
                "publish_seq": None,
            })

        # All inserts are visible to the recomputation in the same transaction.
        published = _advance_watermark(conn, session)

        final_session = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        watermark = final_session["published_watermark"]

        # Reflect final status of items buffered in this batch that got
        # published by the watermark advancement above.
        if published:
            published_keys = {(p["source_id"], p["seq"]): p for p in published}
            for result in item_results:
                pub = published_keys.get((result["source_id"], result["seq"]))
                if pub is not None:
                    result["status"] = "published"
                    result["publish_seq"] = pub["publish_seq"]
                    counters["buffered"] -= 1
                    counters["published"] += 1

    accepted = counters["buffered"] + counters["published"]
    return {
        "session_id": session_id,
        "accepted": accepted,
        "duplicates": counters["duplicate"],
        "conflicts": counters["conflict"],
        "quarantined": counters["quarantined"],
        "published_count": len(published),
        "watermark": to_iso(watermark),
        "published": published,
        "items": item_results,
    }


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #

def pull_published(
    session_id: str, after: int = 0, limit: int = 100
) -> dict[str, Any]:
    if after < 0:
        raise ApiError(422, "after must be >= 0")
    if not (1 <= limit <= 1000):
        raise ApiError(422, "limit must be between 1 and 1000")

    with reader() as conn:
        row = _require_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM readings WHERE session_id = ? AND status = 'published' "
            "AND publish_seq > ? ORDER BY publish_seq ASC LIMIT ?",
            (session_id, after, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    readings = [_row_to_reading_out(r) for r in page]
    # Cursor for the next page whenever a page was returned; callers can stop
    # paging once has_more is false.
    next_after = page[-1]["publish_seq"] if page else None
    return {
        "session_id": session_id,
        "watermark": to_iso(row["published_watermark"]),
        "next_after": next_after,
        "has_more": has_more,
        "readings": readings,
    }


def list_quarantined(session_id: str) -> dict[str, Any]:
    with reader() as conn:
        _require_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM readings WHERE session_id = ? AND status = 'quarantined' "
            "ORDER BY observed_at ASC, source_id ASC, seq ASC, id ASC",
            (session_id,),
        ).fetchall()
    items = [{
        "source_id": r["source_id"],
        "seq": r["seq"],
        "observed_at": to_iso(r["observed_at"]),
        "payload": json.loads(r["payload"]) if r["payload"] is not None else None,
        "quarantined_watermark": to_iso(r["quarantined_watermark"]),
        "received_at": to_iso(r["received_at"]),
    } for r in rows]
    return {"session_id": session_id, "items": items}
