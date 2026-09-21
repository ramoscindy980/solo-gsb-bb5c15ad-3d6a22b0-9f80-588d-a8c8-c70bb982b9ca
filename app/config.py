"""Runtime configuration loaded from environment variables.

Everything has a default so the service starts with zero configuration;
docker-compose only overrides the database path and the port.
"""
from __future__ import annotations

import os


class Settings:
    def __init__(self) -> None:
        # SQLite database file. The directory is created on startup.
        self.db_path: str = os.environ.get("SENSOR_DB_PATH", "./data/sensors.db")
        self.host: str = os.environ.get("SENSOR_HOST", "0.0.0.0")
        self.port: int = int(os.environ.get("SENSOR_PORT", "8000"))
        self.log_level: str = os.environ.get("SENSOR_LOG_LEVEL", "info")


settings = Settings()
