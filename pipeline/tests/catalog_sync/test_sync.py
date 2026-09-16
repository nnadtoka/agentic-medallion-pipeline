import json

import pytest

import pipeline.catalog_sync.sync as sync_module
from pipeline.catalog_sync.sync import _hash_description


class FakeCursor:
    def __init__(self, fetchall_results=None, fetchone_results=None):
        self.executed = []
        self._fetchall_queue = list(fetchall_results or [])
        self._fetchone_queue = list(fetchone_results or [])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._fetchall_queue.pop(0) if self._fetchall_queue else []

    def fetchone(self):
        return self._fetchone_queue.pop(0) if self._fetchone_queue else None


class FakeConnection:
    def __init__(self, cursor=None):
        self.cursor_obj = cursor or FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        self.closed = True


def _write_manifest(tmp_path, nodes):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"nodes": nodes}))
    return manifest_path


def _seller_manifest(tmp_path, description="One row per seller."):
    return _write_manifest(
        tmp_path,
        {
            "model.project.dim_seller": {
                "resource_type": "model",
                "schema": "marts_candidate",
                "name": "dim_seller",
                "description": description,
                "columns": {"seller_id": {"description": "Natural key."}},
            },
        },
    )


def _patch_execute_values(monkeypatch):
    """Record every execute_values call (there are two: upsert, then history)."""

    calls = []

    def fake_execute_values(cursor, query, rows):
        calls.append((query, rows))

    monkeypatch.setattr(sync_module, "execute_values", fake_execute_values)
    return calls


def _run_record_calls(cursor):
    return [
        (query, params)
        for query, params in cursor.executed
        if "catalog_sync_runs" in query
    ]


def test_sync_catalog_writes_a_skipped_run_record_when_manifest_is_empty(
    tmp_path, monkeypatch
):
    manifest_path = _write_manifest(tmp_path, {})

    cursor = FakeCursor()
    fake_connection = FakeConnection(cursor)
    monkeypatch.setattr(sync_module, "get_connection", lambda: fake_connection)
    monkeypatch.setattr(sync_module, "register_vector", lambda conn: None)

    result = sync_module.sync_catalog(
        manifest_path=manifest_path, embed_fn=lambda texts: []
    )

    assert result["tables"] == 0
    assert result["unchanged"] == 0
    assert result["batch_id"].startswith("catalog_sync_gold_marts_")
    assert fake_connection.closed is True

    run_calls = _run_record_calls(cursor)
    assert len(run_calls) == 2  # INSERT running, then UPDATE success
    insert_query, insert_params = run_calls[0]
    assert "INSERT" in insert_query
    assert insert_params[2] == result["batch_id"]  # batch_id position in the insert
    update_query, update_params = run_calls[1]
    assert "UPDATE" in update_query
    assert "no marts descriptions found" in update_params[4]  # comments position


def test_first_sync_treats_every_record_as_changed(tmp_path, monkeypatch):
    manifest_path = _seller_manifest(tmp_path)

    # Empty history table -- the first-ever sync -- so fetchall() for the
    # latest-hashes query returns nothing to compare against.
    cursor = FakeCursor(fetchall_results=[[]], fetchone_results=[(1,)])
    fake_connection = FakeConnection(cursor)
    monkeypatch.setattr(sync_module, "get_connection", lambda: fake_connection)
    monkeypatch.setattr(sync_module, "register_vector", lambda conn: None)
    execute_values_calls = _patch_execute_values(monkeypatch)

    result = sync_module.sync_catalog(
        manifest_path=manifest_path,
        embed_fn=lambda texts: [[0.1, 0.2, 0.3] for _ in texts],
    )

    assert result["tables"] == 1
    assert result["columns"] == 1
    assert result["unchanged"] == 0

    assert len(execute_values_calls) == 2  # upsert, then history insert
    upsert_query, upsert_rows = execute_values_calls[0]
    history_query, history_rows = execute_values_calls[1]
    assert "dataset_embeddings" in upsert_query
    assert "description_sync_history" in history_query
    assert len(upsert_rows) == 2
    assert len(history_rows) == 2
    assert all(row[5] == 1 for row in history_rows)  # catalog_version from RETURNING

    assert fake_connection.closed is True
    version_updates = [
        stmt for stmt, _ in cursor.executed if "catalog_version" in stmt
    ]
    assert len(version_updates) == 1

    run_calls = _run_record_calls(cursor)
    assert len(run_calls) == 2
    _, update_params = run_calls[1]
    assert "2 changed, 0 unchanged" in update_params[4]


def test_unchanged_description_is_skipped_entirely(tmp_path, monkeypatch):
    manifest_path = _seller_manifest(tmp_path, description="One row per seller.")
    table_hash = _hash_description("One row per seller.")
    column_hash = _hash_description("Natural key.")

    # History already has the current hash for both records -- nothing to do.
    cursor = FakeCursor(
        fetchall_results=[[
            ("table", "gold", "dim_seller", None, table_hash),
            ("column", "gold", "dim_seller", "seller_id", column_hash),
        ]]
    )
    fake_connection = FakeConnection(cursor)
    monkeypatch.setattr(sync_module, "get_connection", lambda: fake_connection)
    monkeypatch.setattr(sync_module, "register_vector", lambda conn: None)
    execute_values_calls = _patch_execute_values(monkeypatch)

    def fail_embed(texts):
        raise AssertionError("should not embed anything when nothing changed")

    result = sync_module.sync_catalog(manifest_path=manifest_path, embed_fn=fail_embed)

    assert result["tables"] == 0
    assert result["columns"] == 0
    assert result["unchanged"] == 2
    assert execute_values_calls == []
    version_updates = [
        stmt for stmt, _ in cursor.executed if "catalog_version" in stmt
    ]
    assert version_updates == []  # no bump when nothing changed

    run_calls = _run_record_calls(cursor)
    assert len(run_calls) == 2
    _, update_params = run_calls[1]
    assert "skipped: no description changes detected" in update_params[4]


def test_only_the_changed_description_is_embedded(tmp_path, monkeypatch):
    manifest_path = _seller_manifest(tmp_path, description="Updated seller description.")
    stale_table_hash = _hash_description("One row per seller. (stale)")
    current_column_hash = _hash_description("Natural key.")

    cursor = FakeCursor(
        fetchall_results=[[
            ("table", "gold", "dim_seller", None, stale_table_hash),
            ("column", "gold", "dim_seller", "seller_id", current_column_hash),
        ]],
        fetchone_results=[(2,)],
    )
    fake_connection = FakeConnection(cursor)
    monkeypatch.setattr(sync_module, "get_connection", lambda: fake_connection)
    monkeypatch.setattr(sync_module, "register_vector", lambda conn: None)
    execute_values_calls = _patch_execute_values(monkeypatch)

    embedded_texts = []

    def track_embed(texts):
        embedded_texts.extend(texts)
        return [[0.1, 0.2, 0.3] for _ in texts]

    result = sync_module.sync_catalog(manifest_path=manifest_path, embed_fn=track_embed)

    assert result["tables"] == 1
    assert result["columns"] == 0
    assert result["unchanged"] == 1
    assert embedded_texts == ["Updated seller description."]

    _, upsert_rows = execute_values_calls[0]
    assert len(upsert_rows) == 1
    assert upsert_rows[0][0] == "table"


def test_sync_catalog_rejects_mismatched_embedding_count_and_records_failure(
    tmp_path, monkeypatch
):
    manifest_path = _seller_manifest(tmp_path)

    cursor = FakeCursor(fetchall_results=[[]])
    fake_connection = FakeConnection(cursor)
    monkeypatch.setattr(sync_module, "get_connection", lambda: fake_connection)
    monkeypatch.setattr(sync_module, "register_vector", lambda conn: None)

    with pytest.raises(ValueError, match="changed descriptions"):
        sync_module.sync_catalog(manifest_path=manifest_path, embed_fn=lambda texts: [])

    # Failure path re-creates the run record (the original transaction that
    # inserted it rolled back) then marks it failed -- two INSERTs total
    # (one from the failed attempt, one from the except-block redo) plus
    # one failure UPDATE.
    run_calls = _run_record_calls(cursor)
    assert len(run_calls) == 3
    failed_update_query, failed_update_params = run_calls[-1]
    assert "UPDATE" in failed_update_query
    assert "changed descriptions" in failed_update_params[1]  # error_message position
    assert fake_connection.closed is True
