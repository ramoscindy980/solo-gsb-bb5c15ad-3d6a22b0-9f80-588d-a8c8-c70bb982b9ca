"""Pytest fixtures: run the app against a fresh temporary SQLite database."""
from __future__ import annotations

import importlib
import os
import sys
import tempfile

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)

    # Ensure modules are imported with the test database path.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import app.config  # noqa: E402
    importlib.reload(app.config)
    from fastapi.testclient import TestClient
    from app.main import app  # noqa: E402

    with TestClient(app) as test_client:
        yield test_client
