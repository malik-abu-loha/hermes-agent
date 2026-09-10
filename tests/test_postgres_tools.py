"""Credential-safety and discovery tests for PostgreSQL operator utilities."""

from scripts.migrate_sqlite_to_postgres import discover
from scripts.postgres_backup import _pg_env


def test_pg_native_tool_environment_parses_secret_url_without_command_arguments(monkeypatch):
    monkeypatch.setenv("PGDATABASE", "wrong-target")
    monkeypatch.setenv("PGSERVICE", "unrelated-service")
    monkeypatch.setenv("PGPASSWORD", "unrelated-password")
    env = _pg_env(
        "postgresql://hermes:secret%20value@db.example:5433/app?sslmode=require"
    )
    assert env["PGHOST"] == "db.example"
    assert env["PGPORT"] == "5433"
    assert env["PGUSER"] == "hermes"
    assert env["PGPASSWORD"] == "secret value"
    assert env["PGDATABASE"] == "app"
    assert env["PGSSLMODE"] == "require"
    assert "PGSERVICE" not in env


def test_import_discovery_includes_durable_auxiliary_stores(tmp_path):
    expected = {
        tmp_path / "state.db",
        tmp_path / "verification_evidence.db",
        tmp_path / "gateway" / "discord_message_recovery.db",
        tmp_path / "kanban" / "boards" / "ops" / "kanban.db",
    }
    for path in expected:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    assert set(discover(tmp_path)) == expected
