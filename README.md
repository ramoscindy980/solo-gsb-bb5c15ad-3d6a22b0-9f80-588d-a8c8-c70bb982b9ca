# Laboratory Sensor Timeline API

A FastAPI service that merges **out-of-order, batched sensor readings** from a
fixed set of laboratory sources into one deterministic timeline, using an
event-time **watermark** with bounded late arrival handling. Storage is a
single SQLite file (WAL mode); nothing else is required.

## Quick start (one command)

```bash
docker compose up --build
```

Then open:

- API base: <http://localhost:8000>
- Interactive docs (Swagger UI): <http://localhost:8000/docs>
- Health check: <http://localhost:8000/health>

The image installs all Python dependencies and creates/initialises the SQLite
database automatically on startup — no separate migration or init step. Data
is kept in the named Docker volume `sensor-data` (mounted at `/data`).

To stop it: `docker compose down` (add `-v` to also delete the database).

## Configuration

All configuration is via environment variables; every value has a default.

| Variable                   | Default            | Description                                        |
| -------------------------- | ------------------ | -------------------------------------------------- |
| `SENSOR_DB_PATH`           | `./data/sensors.db` | SQLite database file (directory created on boot)  |
| `SENSOR_HOST`              | `0.0.0.0`          | Bind address                                       |
| `SENSOR_PORT`              | `8000`             | Bind port                                          |
| `SENSOR_LOG_LEVEL`         | `info`             | Uvicorn log level                                  |

The service listens on **port 8000** inside the container; `docker-compose.yml`
publishes it as `8000:8000` (change the left side to use a different host
port). Copy `.env.example` to `.env` and adjust as needed.

## Data model and semantics

### Session

A session is created with:

- a **fixed, ordered list of `sources`** — only those `source_id` values can
  report afterwards;
- **`allowed_lateness_seconds`** — how far behind the slowest source the
  watermark trails (non-negative, fractional values allowed).

### Watermark

Once **every declared source has reported at least once**, the session
watermark is computed after every batch as:

```
watermark = min over sources (latest observed_at(source)) − allowed_lateness_seconds
```

A source's latest time is the maximum `observed_at` of its on-time readings.
The watermark is **monotonic**: it is only ever advanced, never moved
backwards.

When the watermark advances to `W`, all buffered readings with
`observed_at <= W` are **published** in a single flush, closing the whole
boundary timestamp atomically. Readings with `observed_at > W` stay buffered
until a later batch advances the watermark.

### Deterministic order

Published readings receive a dense, gapless `publish_seq` in this total order:

1. `observed_at` ascending
2. `source_id` ascending (lexicographic)
3. `seq` ascending

That makes the merged stream fully reproducible regardless of arrival order.

### Idempotency and conflicts

`(session_id, source_id, seq)` is the unique key of a reading:

- Re-uploading the **same** `(source_id, seq)` with identical `observed_at`
  and `payload` is a replay — counted as `duplicate`, with no side effects.
- Re-uploading the **same key with different content** is reported as
  `conflict`; the stored reading is never overwritten.
- A key repeated within one request body is rejected with `422`.

### Late arrivals (quarantine)

After a timestamp has been published, any new reading whose
`observed_at <= published_watermark` can no longer take its correct position
in the stream (its peers are already emitted). Such readings are stored as
**quarantined** and exposed via the quarantine endpoint. They:

- never change previously published readings or `publish_seq` values;
- never move the watermark backwards;
- are not returned by the published-stream endpoint.

### Timestamps

`observed_at` accepts either:

- an ISO-8601 string with timezone, e.g. `"2026-09-21T10:00:00Z"` or
  `"2026-09-21T12:00:00+02:00"`; or
- numeric epoch seconds (`integer` or `float`).

Naive timestamps (without a timezone) are rejected. Responses always render
timestamps as UTC ISO-8601 strings (`...Z`).

## API

### `POST /sessions` — create a session

```bash
curl -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"sources": ["thermo-a", "thermo-b"], "allowed_lateness_seconds": 5}'
```

### `POST /sessions/{session_id}/readings` — batch report

Batch size is 1–1000 items. Unknown sources make the whole batch fail
atomically (`400`). The response shows per-item outcomes and any readings
published by this batch:

```bash
curl -X POST http://localhost:8000/sessions/<id>/readings \
  -H 'Content-Type: application/json' \
  -d '{"readings": [
    {"source_id": "thermo-a", "seq": 3, "observed_at": "2026-09-21T10:00:30Z", "payload": {"c": 20.3}},
    {"source_id": "thermo-a", "seq": 2, "observed_at": "2026-09-21T10:00:10Z", "payload": {"c": 20.2}},
    {"source_id": "thermo-b", "seq": 1, "observed_at": "2026-09-21T10:00:20Z", "payload": {"c": 19.9}}
  ]}'
```

Per-item `status` is one of `buffered`, `published`, `duplicate`, `conflict`,
`quarantined`.

### `GET /sessions/{session_id}/stream` — pull published readings

Cursor pagination over the append-only stream:

```bash
curl "http://localhost:8000/sessions/<id>/stream?after=0&limit=100"
```

- `after` — return readings with `publish_seq` strictly greater (default `0`);
- `limit` — page size, 1–1000 (default `100`);
- response includes `next_after` (cursor for the next page; `null` when the
  page is empty) and `has_more`.

### `GET /sessions/{session_id}/quarantine` — view isolated late readings

```bash
curl http://localhost:8000/sessions/<id>/quarantine
```

Items are ordered by `(observed_at, source_id, seq)` and include
`quarantined_watermark` (the watermark they fell behind).

### Also available

- `GET /sessions/{session_id}` — session status (watermark, sources reported,
  published / buffered / quarantined counts).
- `GET /health` — liveness probe (used by the Docker health check).
- `GET /docs` / `GET /openapi.json` — interactive docs and OpenAPI schema.

## Local development (without Docker)

Requires Python 3.11+:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
SENSOR_DB_PATH=./data/sensors.db \
  .venv/bin/uvicorn app.main:app --reload --port 8000
```

### Tests

```bash
.venv/bin/pip install pytest httpx
.venv/bin/python -m pytest -q
```

The suite covers: watermark computation and monotonicity, deterministic
tie-breaking, bounded lateness, idempotent replays, conflicts, atomic batch
rejection, pagination, timestamp formats, persistence of published history,
and quarantine of late readings.

## Project layout

```
app/
  main.py        FastAPI app, routes, lifespan (DB init on startup)
  service.py     Watermark, publishing, idempotency and quarantine logic
  database.py    SQLite connection, transactions and schema
  models.py      Pydantic models and timestamp helpers
tests/           Pytest end-to-end tests
Dockerfile       Runtime image (non-root user, /data volume, healthcheck)
docker-compose.yml  Single-command deployment with a named volume
requirements.txt
```

## Concurrency notes

Writes run inside `BEGIN IMMEDIATE` transactions serialised by a process lock,
so a report batch is atomic: inserts, watermark advancement, publishing and
quarantine commit together. The deployment is a single container backed by one
SQLite file, which is the intended topology for this service; scale
vertically or place the database on fast persistent storage for high ingest
rates.
