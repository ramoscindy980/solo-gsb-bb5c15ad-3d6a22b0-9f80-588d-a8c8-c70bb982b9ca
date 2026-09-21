FROM python:3.11-slim

# Keep Python output unbuffered and skip .pyc files for cleaner containers.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SENSOR_DB_PATH=/data/sensors.db \
    SENSOR_HOST=0.0.0.0 \
    SENSOR_PORT=8000 \
    SENSOR_LOG_LEVEL=info

WORKDIR /app

# Install dependencies first so Docker can cache the layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
COPY app ./app

# Persistent database directory, owned by the unprivileged runtime user.
RUN mkdir -p /data && useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /data
USER appuser
VOLUME ["/data"]

EXPOSE 8000

# The schema is created automatically on startup (FastAPI lifespan), so the
# container needs no separate initialisation step.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health').status==200 else 1)"

# Shell form so the command honours SENSOR_HOST / SENSOR_PORT / SENSOR_LOG_LEVEL.
CMD uvicorn app.main:app --host ${SENSOR_HOST} --port ${SENSOR_PORT} --log-level ${SENSOR_LOG_LEVEL}
