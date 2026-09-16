FROM prefecthq/prefect:3.8.5-python3.12

WORKDIR /app

COPY pipeline/ingestion/requirements.txt /tmp/ingestion-requirements.txt
COPY pipeline/prefect_flows/requirements.txt /tmp/prefect-requirements.txt
COPY pipeline/catalog_sync/requirements.txt /tmp/catalog-sync-requirements.txt
COPY dbt/requirements.txt /tmp/dbt-requirements.txt

# No torch pre-install needed: pipeline/catalog_sync uses fastembed (ONNX
# Runtime), which has no PyTorch dependency at all -- see
# pipeline/catalog_sync/embedding.py for why.
RUN python -m pip install --no-cache-dir \
    -r /tmp/ingestion-requirements.txt \
    -r /tmp/prefect-requirements.txt \
    -r /tmp/catalog-sync-requirements.txt \
    -r /tmp/dbt-requirements.txt

# Bake the embedding model into the image so catalog-sync runs offline --
# must match pipeline/catalog_sync/embedding.py's DEFAULT_MODEL_NAME.
# fastembed (ONNX Runtime), not sentence-transformers/torch: same model,
# no CUDA-bundled multi-GB dependency for CPU-only use.
RUN python -c "from fastembed import TextEmbedding; \
    TextEmbedding(model_name='sentence-transformers/all-MiniLM-L6-v2')"

COPY pipeline/quality/requirements.txt /tmp/quality-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/quality-requirements.txt

COPY pipeline/promotion/requirements.txt /tmp/promotion-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/promotion-requirements.txt

COPY pipeline /app/pipeline
COPY dbt /app/dbt

# Data files only (not the mcp_agent package itself -- this image never
# runs the MCP server, and doesn't install fastmcp). Read by
# pipeline/catalog_sync/seed_templates.py.
COPY mcp_agent/templates /app/mcp_agent/templates

CMD ["python", "-m", "pipeline.prefect_flows.serve_flows"]
