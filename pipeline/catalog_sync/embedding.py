"""Local embedding model for the semantic catalog.

Toy-scale choice: a small local model, not an external API -- no extra
secret, no network dependency at query time, reproducible in Docker. Runs
through `fastembed` (ONNX Runtime) rather than `sentence-transformers`
(PyTorch): same model weights, same output, but no CUDA-bundled torch --
`pip install torch` on Linux otherwise pulls a multi-GB GPU stack even for
CPU-only use, which is disproportionate for a laptop-scale toy pipeline
image. The model name is fixed to one that outputs `EMBEDDING_DIM`
dimensions, matching the `vector(384)` column in
`catalog.dataset_embeddings` (postgres/init/031_catalog_tables.sql).
Changing the model means changing both constants together and recreating
that column.
"""

import os
from functools import lru_cache

DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


@lru_cache(maxsize=1)
def _get_model():
    # Imported lazily so importing this module doesn't require fastembed
    # unless a model is actually needed -- e.g. manifest-only unit tests
    # stay light.
    from fastembed import TextEmbedding

    model_name = os.getenv("CATALOG_EMBEDDING_MODEL", DEFAULT_MODEL_NAME)
    # fastembed defaults to a cache dir under /tmp. Fine normally, but a
    # container with a read-only root filesystem + a fresh tmpfs over /tmp
    # (see docker/mcp-server) would silently shadow a model baked in at
    # build time, forcing a re-download (or a hard failure with no
    # network) at every startup. FASTEMBED_CACHE_DIR lets the Dockerfile
    # and the runtime process agree on a path outside /tmp; unset
    # (the default everywhere else) preserves fastembed's own default.
    cache_dir = os.getenv("FASTEMBED_CACHE_DIR")
    return TextEmbedding(model_name=model_name, cache_dir=cache_dir)


def embed_texts(texts):
    """Embed a list of strings, returning one 384-dim float list per input."""

    if not texts:
        return []

    model = _get_model()
    return [vector.tolist() for vector in model.embed(list(texts))]
