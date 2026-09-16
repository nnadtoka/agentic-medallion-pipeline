# Minimal, dedicated image -- this service needs Python + fastmcp + a
# Postgres client + fastembed, nothing else. Not based on the prefect image
# pipeline-engine uses; that would drag in a large, unrelated dependency
# set for a service that never touches Prefect.

# ---- builder: installs deps and bakes the embedding model ----
FROM python:3.12-slim AS builder

WORKDIR /build

COPY mcp_agent/requirements.txt requirements.txt
RUN python -m pip install --no-cache-dir --user -r requirements.txt

# Bake the embedding model into the image so the server runs offline and
# starts fast -- must match pipeline/catalog_sync/embedding.py's
# DEFAULT_MODEL_NAME. Cached outside /tmp on purpose: the final stage
# mounts a fresh tmpfs over /tmp (read-only root filesystem), which would
# otherwise shadow a model cached there and force a re-download at every
# container start.
ENV FASTEMBED_CACHE_DIR=/build/.fastembed_cache
ENV PATH=/root/.local/bin:$PATH
RUN python -c "from fastembed import TextEmbedding; \
    TextEmbedding(model_name='sentence-transformers/all-MiniLM-L6-v2', cache_dir='/build/.fastembed_cache')"

# ---- final: minimal runtime, non-root ----
FROM python:3.12-slim

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin mcpuser

WORKDIR /app

COPY --from=builder --chown=mcpuser:mcpuser /root/.local /home/mcpuser/.local
COPY --from=builder --chown=mcpuser:mcpuser /build/.fastembed_cache /home/mcpuser/.fastembed_cache
COPY --chown=mcpuser:mcpuser pipeline/__init__.py /app/pipeline/__init__.py
COPY --chown=mcpuser:mcpuser pipeline/catalog_sync /app/pipeline/catalog_sync
COPY --chown=mcpuser:mcpuser mcp_agent /app/mcp_agent

USER mcpuser

ENV PATH=/home/mcpuser/.local/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FASTEMBED_CACHE_DIR=/home/mcpuser/.fastembed_cache

EXPOSE 8000

CMD ["python", "-m", "mcp_agent.server"]
