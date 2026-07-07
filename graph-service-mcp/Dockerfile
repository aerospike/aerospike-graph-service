FROM python:3.13-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Create non-root user
RUN useradd -u 10001 -m app && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml /app/

COPY --chown=app:app src /app/src/
COPY --chown=app:app README.md /app/

ENV GREMLIN_URL="ws://aerospike-graph-service:8182/gremlin" \
    GREMLIN_ALIAS="g" \
    HOST="0.0.0.0" \
    PORT="8000" \
    LOG_LEVEL="INFO"

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -e /app

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "/app/src/graph_service_mcp/server.py"]
