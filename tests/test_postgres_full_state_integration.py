"""PostgreSQL coverage for every Hermes-owned durable SQL store."""

from __future__ import annotations

import os
import uuid

import pytest


pytestmark = pytest.mark.skipif(
    os.getenv("HERMES_TEST_POSTGRES") != "1",
    reason="set HERMES_TEST_POSTGRES=1 to run local PostgreSQL integration tests",
)
_URL = "postgresql://hermes:hermes-local-only@localhost:55432/hermes_test"


@pytest.fixture
def pg_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_DATABASE_URL", _URL)
    (tmp_path / "config.yaml").write_text("database:\n  backend: postgres\n", encoding="utf-8")
    return tmp_path


def test_gateway_delivery_and_async_delegation_use_postgres(pg_home):
    from agent import verification_evidence
    from gateway import delivery_ledger
    from tools import async_delegation

    oid = uuid.uuid4().hex
    delivery_ledger.record_obligation(
        obligation_id=oid, session_key="s", platform="test", chat_id="c",
        thread_id=None, content="durable",
    )
    delivery_ledger.mark_delivered(oid)
    with delivery_ledger._transaction() as conn:
        assert conn.execute(
            "SELECT state FROM delivery_obligations WHERE obligation_id=?", (oid,)
        ).fetchone()[0] == "delivered"

    did = uuid.uuid4().hex
    async_delegation._persist_dispatch({
        "delegation_id": did, "session_key": "origin", "origin_session_id": "api-1",
        "origin_ui_session_id": "ui-1", "dispatched_at": 1.0,
    })
    assert async_delegation.get_durable_delegation(did)["state"] == "running"

    evidence = verification_evidence._connect()
    try:
        evidence.execute(
            "INSERT INTO verification_state(session_id, root, changed_paths_json) "
            "VALUES (?, ?, ?)", ("session-1", "/workspace", "[]"))
        evidence.commit()
        assert evidence.execute("SELECT COUNT(*) FROM verification_state").fetchone()[0] == 1
    finally:
        evidence.close()
    assert list(pg_home.rglob("*.db")) == []


def test_api_cron_projects_and_kanban_use_postgres(pg_home):
    from cron import delivery_queue, executions, notepad
    from gateway.platforms.api_server import ResponseStore
    from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc
    from hermes_cli import kanban_transfer
    from hermes_cli import projects_db as projects
    from plugins.platforms.discord.recovery import DiscordRecoveryStore

    responses = ResponseStore()
    responses.put("resp-1", {"value": "kept"})
    assert responses.get("resp-1") == {"value": "kept"}
    responses.close()

    runs = RunIdempotencyStore()
    outcome, record = runs.reserve("tenant", "key", "fingerprint", "run-1", {"status": "running"})
    assert outcome == "created" and record["run_id"] == "run-1"
    assert runs.lookup("tenant", "key", "fingerprint")[0] == "reused"
    runs.close()

    execution = executions.create_execution("job-1", source="integration")
    assert executions.get_execution(execution["id"])["job_id"] == "job-1"
    assert notepad.set_note("job-1", "cursor", "42")["value"] == "42"
    assert delivery_queue.enqueue("exec-1", {"id": "job-1"}, "hello")["status"] == "pending"

    project_conn = projects.connect()
    try:
        project_id = projects.create_project(project_conn, name="Postgres Project", folders=["/tmp/project"])
        assert projects.get_project(project_conn, project_id).name == "Postgres Project"
    finally:
        project_conn.close()

    board_conn = kbc.connect()
    try:
        task_id = kb.create_task(board_conn, title="Postgres task")
        assert kb.get_task(board_conn, task_id).title == "Postgres task"
    finally:
        board_conn.close()

    archive = kanban_transfer.export_board("default", str(pg_home / "board-export"))["archive"]
    imported = kanban_transfer.import_board(archive, "postgres-import")
    assert imported["counts"]["tasks"] == 1
    with kbc.connect_closing(board="postgres-import") as imported_board:
        assert kb.get_task(imported_board, task_id).title == "Postgres task"

    discord = DiscordRecoveryStore(hermes_home=pg_home)
    discord.call(lambda conn: conn.execute(
        "INSERT INTO discord_recovery_cursors(channel_id, last_message_id, updated_at) "
        "VALUES (?, ?, ?)", ("channel-1", "message-1", "2099-01-01T00:00:00+00:00")))
    assert discord.call(lambda conn: conn.execute(
        "SELECT last_message_id FROM discord_recovery_cursors WHERE channel_id=?",
        ("channel-1",)).fetchone()[0]) == "message-1"
    assert list(pg_home.rglob("*.db")) == []


def test_hosted_rooms_topics_and_profile_isolation(pg_home, monkeypatch):
    from agent.insights import InsightsEngine
    from gateway import hosted_rooms
    from hermes_state import SessionDB
    from hermes_cli.web_routers import analytics as analytics_router

    state_path = pg_home / "state.db"
    db = SessionDB(db_path=state_path)
    try:
        db.create_session("topic-session", "telegram")
        db.append_message("topic-session", "user", "Unicode analytics: مرحبا 👋")
        report = InsightsEngine(db).generate(days=30)
        assert report["overview"]["total_sessions"] == 1
        db.enable_telegram_topic_mode(chat_id="1", user_id="2")
        db.bind_telegram_topic(
            chat_id="1", thread_id="3", user_id="2", session_key="key",
            session_id="topic-session",
        )
        assert db.get_telegram_topic_binding(chat_id="1", thread_id="3")["session_id"] == "topic-session"
    finally:
        db.close()

    monkeypatch.setattr(
        analytics_router, "_open_session_db_for_profile",
        lambda _profile, read_only: SessionDB(db_path=state_path, read_only=read_only),
    )
    analytics = analytics_router._get_usage_analytics(days=30)
    assert analytics["totals"]["total_sessions"] >= 1
    assert analytics["daily"]

    room = hosted_rooms.create_room(
        state_path, room_id="room-1", name="Room",
        members=[{"profile": "ops", "handle": "ops"}], authority_gateway_id="gateway-a",
    )
    assert room["room_id"] == "room-1"
    assert hosted_rooms.list_rooms(state_path)[0]["room_id"] == "room-1"

    other = pg_home / "profiles" / "other"
    other.mkdir(parents=True)
    (other / "config.yaml").write_text("database:\n  backend: postgres\n", encoding="utf-8")
    isolated = SessionDB(db_path=other / "state.db")
    try:
        assert isolated.get_session("topic-session") is None
    finally:
        isolated.close()
    assert list(pg_home.rglob("*.db")) == []


def test_non_destructive_sqlite_import(pg_home):
    import sqlite3
    from scripts.migrate_sqlite_to_postgres import import_store
    from hermes_db import connect_database
    from hermes_state import SessionDB

    # The source remains a normal SQLite file even though the profile is now
    # configured to direct live state to PostgreSQL.
    source = pg_home / "state.db"
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT NOT NULL, started_at REAL NOT NULL)")
        conn.execute("INSERT INTO sessions VALUES ('imported-session', 'cli', 1.0)")
    report = import_store(source)
    assert report["imported"] == 1
    assert source.is_file()
    with pytest.raises(RuntimeError, match="already been imported"):
        import_store(source)
    db = SessionDB(db_path=source)
    try:
        assert db.get_session("imported-session")["source"] == "cli"
    finally:
        db.close()

    auxiliary = pg_home / "verification_evidence.db"
    with sqlite3.connect(auxiliary) as source_db:
        source_db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        source_db.execute("CREATE UNIQUE INDEX idx_evidence_value ON evidence(value)")
        source_db.execute("INSERT INTO evidence(id, value) VALUES (1, 'preserved')")
    aux_report = import_store(auxiliary)
    assert aux_report["imported"] == 1
    assert auxiliary.is_file()
    target = connect_database(auxiliary)
    try:
        assert target.execute("SELECT value FROM evidence WHERE id=?", (1,)).fetchone()[0] == "preserved"
        assert target.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname=?",
            ("idx_evidence_value",),
        ).fetchone() is not None
    finally:
        target.close()


def test_auxiliary_workers_serialize_check_then_write(pg_home):
    from concurrent.futures import ThreadPoolExecutor
    from hermes_db import connect_database
    from hermes_cli.kanban_db_connect import _dispatch_tick_lock

    path = pg_home / "counter.db"
    conn = connect_database(path, isolation_level=None)
    try:
        conn.execute("CREATE TABLE counter (value INTEGER NOT NULL)")
        conn.execute("INSERT INTO counter VALUES (0)")
    finally:
        conn.close()

    def increment(_):
        worker = connect_database(path, isolation_level=None)
        try:
            worker.execute("BEGIN IMMEDIATE")
            value = worker.execute("SELECT value FROM counter").fetchone()[0]
            worker.execute("UPDATE counter SET value=?", (value + 1,))
            worker.commit()
        finally:
            worker.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(increment, range(12)))
    conn = connect_database(path)
    try:
        assert conn.execute("SELECT value FROM counter").fetchone()[0] == 12
    finally:
        conn.close()
    with _dispatch_tick_lock(path) as first:
        with _dispatch_tick_lock(path) as second:
            assert first is True and second is False
    with _dispatch_tick_lock(path) as after_close:
        assert after_close is True


def test_namespaced_state_survives_home_relocation(pg_home):
    from hermes_state import SessionDB
    namespace = "relocation-" + uuid.uuid4().hex
    configuration = f"database:\n  backend: postgres\n  namespace: {namespace}\n"
    (pg_home / "config.yaml").write_text(configuration)
    with SessionDB(db_path=pg_home / "state.db") as original:
        original.create_session("relocated", "cli")
        original.append_message("relocated", "user", "persistent")
    moved = pg_home / "moved"
    moved.mkdir()
    (moved / "config.yaml").write_text(configuration)
    with SessionDB(db_path=moved / "state.db") as reopened:
        assert reopened.get_messages("relocated")[0]["content"] == "persistent"
    assert not (moved / "state.db").exists()
