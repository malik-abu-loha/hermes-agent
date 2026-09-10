"""Run with HERMES_TEST_POSTGRES=1 against docker-compose.postgres.yml."""

import concurrent.futures
import json
import os
import uuid

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("HERMES_TEST_POSTGRES") != "1",
    reason="set HERMES_TEST_POSTGRES=1 to run local PostgreSQL integration tests",
)

_LOCAL_TEST_URL = "postgresql://hermes:hermes-local-only@localhost:55432/hermes_test"


@pytest.fixture
def pg_db(tmp_path, monkeypatch):
    pytest.importorskip("psycopg")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_DATABASE_URL", _LOCAL_TEST_URL)
    (tmp_path / "config.yaml").write_text(
        "database:\n  backend: postgres\n  pool_min_size: 1\n  pool_max_size: 8\n",
        encoding="utf-8",
    )
    from hermes_state import SessionDB
    db = SessionDB()
    yield db
    db.close()


def test_core_session_message_routing_search_and_delete(pg_db):
    sid = f"pg-{uuid.uuid4()}"
    pg_db.create_session(sid, "cli", model="test-model", model_config={"unicode": "مرحبا 🌍"})
    first = pg_db.append_message(sid, "user", "PostgreSQL durable unicode مرحبا 🌍")
    pg_db.append_message(sid, "assistant", "A" * 200_000, display_metadata={"kind": "large"})
    pg_db.set_session_title(sid, "PostgreSQL continuation")
    assert first > 0
    session = pg_db.get_session(sid)
    assert session["model"] == "test-model"
    assert json.loads(session["model_config"]) == {"unicode": "مرحبا 🌍"}
    assert session["title"] == "PostgreSQL continuation"
    assert session["started_at"] > 0
    messages = pg_db.get_messages(sid)
    assert [m["role"] for m in messages] == ["user", "assistant"]
    assert messages[1]["display_metadata"] == {"kind": "large"}
    assert pg_db.get_messages_as_conversation(sid)[-1]["content"] == "A" * 200_000
    assert any(row["id"] == sid for row in pg_db.list_sessions_rich())
    assert pg_db._read_one(
        "SELECT max(version) FROM hermes_schema_migrations"
    )[0] >= 2
    pg_db.save_gateway_routing_entry("agent:main:test", '{"session_id":"x"}', scope="test")
    assert "agent:main:test" in pg_db.load_gateway_routing_entries(scope="test")
    assert any(row["session_id"] == sid for row in pg_db.search_messages("durable unicode"))
    with pytest.raises(Exception):
        pg_db._execute_write(lambda conn: conn.execute(
            "INSERT INTO sessions(id, source, started_at) VALUES (?, ?, ?)",
            (sid, "cli", 1.0),
        ))
    pg_db.delete_session(sid)
    assert pg_db.get_session(sid) is None


def test_transaction_rollback(pg_db):
    sid = f"rollback-{uuid.uuid4()}"
    pg_db.create_session(sid, "cli")
    with pytest.raises(RuntimeError, match="rollback"):
        def fail(conn):
            conn.execute("UPDATE sessions SET title = ? WHERE id = ?", ("must-not-stick", sid))
            raise RuntimeError("rollback")
        pg_db._execute_write(fail)
    assert pg_db.get_session(sid)["title"] is None


def test_concurrent_session_reads_and_message_writes(pg_db):
    sid = f"concurrent-{uuid.uuid4()}"
    pg_db.create_session(sid, "cli")

    def write(index):
        return pg_db.append_message(sid, "user", f"worker {index}")

    def read(_index):
        return pg_db.get_session(sid)["id"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda i: write(i) if i % 2 else read(i), range(40)))
    assert all(result for result in results)
    assert pg_db.message_count(sid) == 20


def test_restart_reconnect_preserves_state(pg_db, monkeypatch):
    sid = f"restart-{uuid.uuid4()}"
    pg_db.create_session(sid, "cli")
    pg_db.close()
    from hermes_state import SessionDB
    reopened = SessionDB()
    try:
        assert reopened.get_session(sid)["id"] == sid
    finally:
        reopened.close()


def test_postgres_maintenance_uses_database_not_local_locks(pg_db):
    result = pg_db.maybe_auto_prune_and_vacuum()
    assert "error" not in result
    assert result["vacuumed"] is False
    assert pg_db.maybe_auto_prune_and_vacuum()["skipped"] is True
    assert pg_db.fts_rebuild_step() is False
    assert not pg_db.db_path.exists()
