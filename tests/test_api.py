"""End-to-end behaviour tests for the sensor timeline API."""
from __future__ import annotations

from datetime import datetime, timezone


def _iso(epoch_seconds: float) -> str:
    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _create_session(client, sources, allowed_lateness=0.0):
    resp = client.post(
        "/sessions",
        json={"sources": sources, "allowed_lateness_seconds": allowed_lateness},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["session_id"]


def _report(client, session_id, readings):
    resp = client.post(
        f"/sessions/{session_id}/readings", json={"readings": readings}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


# --------------------------------------------------------------------------- #
# Session management
# --------------------------------------------------------------------------- #

def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_session_pins_sources_and_lateness(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=2.5)
    info = client.get(f"/sessions/{sid}").json()
    assert info["sources"] == ["a", "b"]
    assert info["allowed_lateness_seconds"] == 2.5
    assert info["watermark"] is None
    assert info["sources_reported"] == 0


def test_create_session_rejects_duplicate_and_empty_sources(client):
    r = client.post("/sessions", json={"sources": ["a", "a"]})
    assert r.status_code == 422
    r = client.post("/sessions", json={"sources": []})
    assert r.status_code == 422
    r = client.post("/sessions", json={"sources": ["a"], "allowed_lateness_seconds": -1})
    assert r.status_code == 422


def test_session_not_found(client):
    assert client.get("/sessions/nope").status_code == 404
    assert client.post(
        "/sessions/nope/readings", json={"readings": [
            {"source_id": "a", "seq": 1, "observed_at": 0}
        ]}
    ).status_code == 404


# --------------------------------------------------------------------------- #
# Watermark basics
# --------------------------------------------------------------------------- #

def test_nothing_published_until_all_sources_report(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    out = _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 100, "payload": {"v": 1}},
    ])
    assert out["watermark"] is None
    assert out["published_count"] == 0
    assert out["items"][0]["status"] == "buffered"
    assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []


def test_watermark_publishes_at_and_below_slowest_source(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    # Source a at 100, source b at 110 -> watermark = min(100, 110) = 100.
    # Readings at or below the watermark are published, the whole boundary
    # timestamp closing atomically.
    out = _report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": 100},
        {"source_id": "a", "seq": 1, "observed_at": 99},
        {"source_id": "b", "seq": 1, "observed_at": 110},
    ])
    assert out["watermark"] == _iso(100)
    assert [(r["source_id"], r["seq"]) for r in out["published"]] == [
        ("a", 1),
        ("a", 2),
    ]

    stream = client.get(f"/sessions/{sid}/stream").json()
    assert [r["seq"] for r in stream["readings"]] == [1, 2]
    assert stream["readings"][0]["observed_at"] == _iso(99)
    assert stream["readings"][1]["observed_at"] == _iso(100)


def test_allowed_lateness_shifts_watermark_back(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=10)
    # slowest source max is a@100 -> watermark = 100 - 10 = 90
    out = _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 90},   # at wm -> published
        {"source_id": "a", "seq": 2, "observed_at": 89},   # below wm -> published
        {"source_id": "a", "seq": 3, "observed_at": 100},  # sets source time
        {"source_id": "b", "seq": 1, "observed_at": 105},
    ])
    assert out["watermark"] == _iso(90)
    # deterministic (observed_at, source, seq) order
    assert [r["seq"] for r in out["published"]] == [2, 1]
    info = client.get(f"/sessions/{sid}").json()
    # a@100 and b@105 are ahead of the watermark and wait in the buffer
    assert info["buffered_count"] == 2
    assert info["published_count"] == 2


def test_watermark_is_monotonic(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "b", "seq": 1, "observed_at": 100},
    ])
    # b regresses in time: watermark must stay at 100 and the old reading,
    # being at the published boundary, is quarantined rather than replayed.
    out = _report(client, sid, [
        {"source_id": "b", "seq": 2, "observed_at": 90},
    ])
    assert out["watermark"] == _iso(100)
    assert out["published_count"] == 0
    assert out["quarantined"] == 1
    q = client.get(f"/sessions/{sid}/quarantine").json()["items"]
    assert [(i["source_id"], i["seq"]) for i in q] == [("b", 2)]


def test_deterministic_ordering_tie_breaks_on_source_and_seq(client):
    sid = _create_session(client, ["b", "a", "c"], allowed_lateness=0)
    # Every source reaches t=50 in one batch -> watermark 50, and the four
    # t=50 readings must be emitted in (observed_at, source_id, seq) order.
    out = _report(client, sid, [
        {"source_id": "c", "seq": 9, "observed_at": 50},
        {"source_id": "a", "seq": 2, "observed_at": 50},
        {"source_id": "b", "seq": 7, "observed_at": 50},
        {"source_id": "a", "seq": 1, "observed_at": 50},
    ])
    keys = [(r["observed_at"], r["source_id"], r["seq"]) for r in out["published"]]
    assert keys == [
        (_iso(50), "a", 1),
        (_iso(50), "a", 2),
        (_iso(50), "b", 7),
        (_iso(50), "c", 9),
    ]
    seqs = [r["publish_seq"] for r in out["published"]]
    assert seqs == [1, 2, 3, 4]


# --------------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------------- #

def test_duplicate_upload_is_idempotent(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    payload = {"temp_c": 21.5, "unit": "C"}
    body = [
        {"source_id": "a", "seq": 1, "observed_at": 90, "payload": payload},
        {"source_id": "b", "seq": 1, "observed_at": 100},
    ]
    first = _report(client, sid, body)
    assert first["accepted"] == 2
    second = _report(client, sid, body)
    assert second["duplicates"] == 2
    assert second["accepted"] == 0
    assert second["published_count"] == 0  # no republishing

    stream = client.get(f"/sessions/{sid}/stream").json()
    # watermark(90) closed a@90; b@100 stays buffered. Replays add nothing.
    assert len(stream["readings"]) == 1
    row = stream["readings"][0]
    assert row["source_id"] == "a"
    assert row["payload"] == {"temp_c": 21.5, "unit": "C"}


def test_conflicting_same_key_does_not_overwrite(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 90, "payload": {"v": 1}},
        {"source_id": "b", "seq": 1, "observed_at": 90},
    ])
    # Same (source, seq), different content -> conflict, original preserved.
    out = _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 999, "payload": {"v": 2}},
    ])
    assert out["conflicts"] == 1
    row = client.get(f"/sessions/{sid}/stream").json()["readings"][0]
    assert row["payload"] == {"v": 1}
    assert row["observed_at"] == _iso(90)


def test_unknown_source_rejected_batch_atomically(client):
    sid = _create_session(client, ["a"], allowed_lateness=0)
    r = client.post(f"/sessions/{sid}/readings", json={"readings": [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "zzz", "seq": 1, "observed_at": 100},
    ]})
    assert r.status_code == 400
    # First item must have been rolled back together with the bad one.
    info = client.get(f"/sessions/{sid}").json()
    assert info["buffered_count"] == 0


def test_duplicate_key_within_one_batch_rejected(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    r = client.post(f"/sessions/{sid}/readings", json={"readings": [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "a", "seq": 1, "observed_at": 100},
    ]})
    assert r.status_code == 422


# --------------------------------------------------------------------------- #
# Late arrivals / quarantine
# --------------------------------------------------------------------------- #

def test_reading_behind_published_watermark_is_quarantined(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 90},
        {"source_id": "b", "seq": 1, "observed_at": 90},
        {"source_id": "a", "seq": 2, "observed_at": 100},
        {"source_id": "b", "seq": 2, "observed_at": 100},
    ])  # watermark advances to 100; t=90 and t=100 published

    # Late arrival for an already-closed moment.
    out = _report(client, sid, [
        {"source_id": "a", "seq": 100, "observed_at": 80, "payload": {"late": True}},
    ])
    assert out["quarantined"] == 1
    assert out["items"][0]["status"] == "quarantined"
    assert out["published_count"] == 0
    assert out["watermark"] == _iso(100)  # history untouched

    q = client.get(f"/sessions/{sid}/quarantine").json()["items"]
    assert len(q) == 1
    assert q[0]["source_id"] == "a"
    assert q[0]["seq"] == 100
    assert q[0]["observed_at"] == _iso(80)
    assert q[0]["quarantined_watermark"] == _iso(100)

    # Published stream unchanged.
    stream = client.get(f"/sessions/{sid}/stream").json()
    assert [(r["source_id"], r["seq"]) for r in stream["readings"]] == [
        ("a", 1),
        ("b", 1),
        ("a", 2),
        ("b", 2),
    ]


def test_reading_at_published_watermark_is_quarantined(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "b", "seq": 1, "observed_at": 100},
        {"source_id": "a", "seq": 2, "observed_at": 110},
        {"source_id": "b", "seq": 2, "observed_at": 110},
    ])  # watermark advances to 110 -> t=100 and t=110 are published

    # A further arrival at (or below) the published watermark is late: its
    # ties are already emitted and it cannot be inserted into history.
    out = _report(client, sid, [
        {"source_id": "a", "seq": 100, "observed_at": 100, "payload": {"late": True}},
    ])
    assert out["quarantined"] == 1
    assert out["items"][0]["status"] == "quarantined"
    assert out["published_count"] == 0
    assert out["watermark"] == _iso(110)  # history untouched

    q = client.get(f"/sessions/{sid}/quarantine").json()["items"]
    assert len(q) == 1
    assert q[0]["source_id"] == "a"
    assert q[0]["seq"] == 100
    assert q[0]["observed_at"] == _iso(100)
    assert q[0]["quarantined_watermark"] == _iso(110)

    # Published stream unchanged.
    stream = client.get(f"/sessions/{sid}/stream").json()
    assert [(r["source_id"], r["seq"]) for r in stream["readings"]] == [
        ("a", 1),
        ("b", 1),
        ("a", 2),
        ("b", 2),
    ]


def test_future_reading_stays_buffered_until_watermark_catches_up(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "b", "seq": 1, "observed_at": 100},
    ])
    # 101 is ahead of the watermark -> buffered, not quarantined.
    out = _report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": 101},
    ])
    assert out["accepted"] == 1
    assert out["published_count"] == 0
    assert client.get(f"/sessions/{sid}/quarantine").json()["items"] == []


def test_advancing_watermark_drains_buffer_only_once(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    first = _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 100},
        {"source_id": "b", "seq": 1, "observed_at": 90},
    ])
    # watermark = 90 (slowest b) -> only b@90 published; a@100 buffers
    assert [(r["source_id"], r["seq"]) for r in first["published"]] == [("b", 1)]
    # b@95 buffered (a already at 100, so watermark becomes 95 immediately
    # and only b@95 comes out; a@100 remains buffered).
    second = _report(client, sid, [
        {"source_id": "b", "seq": 2, "observed_at": 95},
    ])
    assert [(r["source_id"], r["seq"]) for r in second["published"]] == [("b", 2)]
    # b catches up to 100 -> watermark 100, remaining buffer drains once.
    out = _report(client, sid, [
        {"source_id": "b", "seq": 3, "observed_at": 100},
    ])
    assert [(r["source_id"], r["seq"]) for r in out["published"]] == [
        ("a", 1),
        ("b", 3),
    ]
    # Everything exactly once, globally ordered.
    stream = client.get(f"/sessions/{sid}/stream").json()["readings"]
    assert [(r["source_id"], r["seq"]) for r in stream] == [
        ("b", 1),
        ("b", 2),
        ("a", 1),
        ("b", 3),
    ]


# --------------------------------------------------------------------------- #
# Pagination
# --------------------------------------------------------------------------- #

def test_stream_pagination_and_cursor(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 1},
        {"source_id": "a", "seq": 2, "observed_at": 2},
        {"source_id": "a", "seq": 3, "observed_at": 3},
        {"source_id": "b", "seq": 1, "observed_at": 3},
        {"source_id": "a", "seq": 4, "observed_at": 4},
        {"source_id": "b", "seq": 2, "observed_at": 4},
    ])
    # watermark advances to 4 (slowest source b) -> all six readings published
    page1 = client.get(f"/sessions/{sid}/stream?after=0&limit=2").json()
    assert [r["publish_seq"] for r in page1["readings"]] == [1, 2]
    assert page1["has_more"] is True
    cursor = page1["next_after"]

    page2 = client.get(f"/sessions/{sid}/stream",
                       params={"after": cursor, "limit": 2}).json()
    assert [r["publish_seq"] for r in page2["readings"]] == [3, 4]
    assert page2["has_more"] is True

    page3 = client.get(f"/sessions/{sid}/stream",
                       params={"after": page2["next_after"], "limit": 2}).json()
    assert [r["publish_seq"] for r in page3["readings"]] == [5, 6]
    assert page3["has_more"] is False
    # last page still echoes a cursor, but paging past it yields nothing
    assert page3["next_after"] == 6
    tail = client.get(f"/sessions/{sid}/stream",
                      params={"after": 6, "limit": 2}).json()
    assert tail["readings"] == []
    assert tail["next_after"] is None
    assert tail["has_more"] is False


def test_epoch_numbers_and_iso_timestamps_accepted(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    out = _report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": 90},
        {"source_id": "b", "seq": 1, "observed_at": "2020-01-01T00:01:40+00:00"},
    ])
    assert out["watermark"] == _iso(90)


def test_naive_timestamp_rejected(client):
    sid = _create_session(client, ["a", "b"], allowed_lateness=0)
    r = client.post(f"/sessions/{sid}/readings", json={"readings": [
        {"source_id": "a", "seq": 1, "observed_at": "2020-01-01T00:00:00"},
    ]})
    assert r.status_code == 422
