"""SQLite connection management and schema initialisation.

A single process-wide connection is serialised by a re-entrant lock. Writes run
inside explicit ``BEGIN IMMEDIATE`` transactions so that a report batch is
atomic: watermark advancement, publishing and quarantine happen together and
history can never be half-rewritten.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from .config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                       TEXT PRIMARY KEY,
    allowed_lateness_seconds REAL NOT NULL,
    source_count             INTEGER NOT NULL,
    -- NULL until every source has reported at least once and the
    -- first batch of readings is published.
    published_watermark      REAL,
    created_at               REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS session_sources (
    session_id TEXT NOT NULL,
    source_id  TEXT NOT NULL,
    pos        INTEGER NOT NULL,
    PRIMARY KEY (session_id, source_id),
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS readings (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id             TEXT NOT NULL,
    source_id              TEXT NOT NULL,
    seq                    INTEGER NOT NULL,
    observed_at            REAL NOT NULL,  -- epoch seconds (UTC)
    payload                TEXT,           -- raw JSON supplied by the client
    -- 'buffered' | 'published' | 'quarantined'
    status                 TEXT NOT NULL,
    -- Assigned at publish time; dense, globally ordered within a session.
    publish_seq            INTEGER,
    -- Session watermark at the moment the row was published.
    published_watermark    REAL,
    -- Already-published watermark a late reading fell behind (for quarantine).
    quarantined_watermark  REAL,
    received_at            REAL NOT NULL,
    -- Idempotency key: a repeat upload of the same (source, seq) is a replay.
    UNIQUE (session_id, source_id, seq),
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_readings_publish
    ON readings(session_id, publish_seq);
CREATE INDEX IF NOT EXISTS idx_readings_pending
    ON readings(session_id, status, observed_at, source_id, seq);
CREATE INDEX IF NOT EXISTS idx_readings_quarantine
    ON readings(session_id, status, id);
"""

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def init_db() -> None:
    """Open the database (creating its directory) and apply the schema."""
    global _conn
    with _lock:
        if _conn is not None:
            return
        db_dir = os.path.dirname(os.path.abspath(settings.db_path))
        os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(
            settings.db_path,
            check_same_thread=False,
            isolation_level=None,  # we manage transactions explicitly
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.executescript(_SCHEMA)
        _conn = conn


def _get_conn() -> sqlite3.Connection:
    if _conn is None:
        init_db()
    assert _conn is not None
    return _conn


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serialize writers and hold one immediate transaction for the block."""
    conn = _get_conn()
    with _lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


@contextmanager
def reader() -> Iterator[sqlite3.Connection]:
    """Serialize access to the shared connection for a read-only statement."""
    conn = _get_conn()
    with _lock:
        yield conn
