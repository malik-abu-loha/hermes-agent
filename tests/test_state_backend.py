from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from hermes_state_backend import (
    DatabaseConfigurationError, DatabaseSettings, DatabaseUnavailableError,
    configured_database_backend, load_database_settings,
    sanitized_database_url,
)
from hermes_state_postgres import _qmark_sql, _translate_sql
from hermes_db import translate_sql


def test_postgres_row_supports_sqlite_unpacking_and_mapping():
    from hermes_db import Row
    row = Row(["id", "content"], [7, "hello"])
    assert tuple(row) == (7, "hello")
    assert dict(row) == {"id": 7, "content": "hello"}
    assert row[0] == row["id"]


def test_stable_namespace_survives_home_move_and_isolates_stores(tmp_path, monkeypatch):
    from hermes_db import postgres_schema_for_path
    monkeypatch.setenv("HERMES_DATABASE_URL", "postgresql://test:test@localhost/test")
    homes = [tmp_path / "original", tmp_path / "relocated"]
    for home in homes:
        _config(home, "database:\n  backend: postgres\n  namespace: deployment-main\n")
    assert postgres_schema_for_path(homes[0] / "state.db") == postgres_schema_for_path(homes[1] / "state.db")
    assert postgres_schema_for_path(homes[0] / "state.db") != postgres_schema_for_path(homes[0] / "projects.db")
    _config(homes[1], "database:\n  backend: postgres\n  namespace: deployment-other\n")
    assert postgres_schema_for_path(homes[0] / "state.db") != postgres_schema_for_path(homes[1] / "state.db")


def test_postgres_does_not_fake_integrity_or_sqlite_schema_checks():
    from hermes_db import PostgresConnection
    conn = PostgresConnection(None, "test")
    for statement in ("PRAGMA integrity_check", "PRAGMA foreign_key_check",
                      "SELECT sql FROM sqlite_master WHERE type='table' AND name='test'"):
        with pytest.raises(NotImplementedError):
            conn._execute_on(None, statement)


def test_update_checks_leave_legacy_sqlite_untouched_in_postgres_mode(tmp_path, monkeypatch):
    from hermes_cli import update_cmd_maint
    from hermes_cli import backup, config
    _config(tmp_path, "database:\n  backend: postgres\n")
    monkeypatch.setenv("HERMES_DATABASE_URL", "postgresql://test:test@localhost/test")
    monkeypatch.setattr(config, "get_hermes_home", lambda: tmp_path)
    (tmp_path / "state.db").write_bytes(b"broken legacy source")
    def forbidden(*args, **kwargs):
        pytest.fail("PostgreSQL updater must not inspect or restore SQLite")
    monkeypatch.setattr(backup, "verify_sqlite_integrity", forbidden)
    update_cmd_maint._verify_and_restore_one_state_db(tmp_path, label="test")
    update_cmd_maint._verify_state_db_after_snapshot("test")
    assert (tmp_path / "state.db").read_bytes() == b"broken legacy source"


def _config(home: Path, text: str) -> None:
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text(text, encoding="utf-8")


def test_sqlite_is_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    assert configured_database_backend() == "sqlite"


def test_postgres_requires_secret_url(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_DATABASE_URL", raising=False)
    _config(tmp_path, "database:\n  backend: postgres\n")
    with pytest.raises(DatabaseConfigurationError, match="HERMES_DATABASE_URL"):
        load_database_settings()


def test_invalid_backend_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(tmp_path, "database:\n  backend: mysql\n")
    with pytest.raises(DatabaseConfigurationError, match="sqlite, postgres|postgres, sqlite"):
        load_database_settings()


def test_postgres_settings_and_url_redaction(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(tmp_path, "database:\n  backend: postgres\n  pool_max_size: 4\n")
    url = "postgresql://hermes:super-secret@db.example:5432/app?sslmode=require"
    monkeypatch.setenv("HERMES_DATABASE_URL", url)
    settings = load_database_settings()
    assert settings.backend == "postgres"
    assert settings.pool_max_size == 4
    assert sanitized_database_url(url) == "postgresql://hermes@db.example:5432/app"
    assert "super-secret" not in sanitized_database_url(url)


def test_qmark_translation_ignores_quoted_question_marks():
    assert _qmark_sql("SELECT '?', value FROM t WHERE a = ? AND b = \"?\"") == (
        "SELECT '?', value FROM t WHERE a = %s AND b = \"?\""
    )
    assert "ON CONFLICT DO NOTHING" in _translate_sql(
        "INSERT OR IGNORE INTO system_prompts(hash, prompt) VALUES (?, ?)"
    )
    assert "RETURNING id" in _translate_sql("INSERT INTO messages(session_id) VALUES (?)")


def test_shared_sql_translation_handles_core_sqlite_idioms():
    assert translate_sql("SELECT 'INTEGER BLOB grant LIMIT -1'") == "SELECT 'INTEGER BLOB grant LIMIT -1'"
    translated = translate_sql(
        "SELECT CHAR(10), INSTR(content, 'x'), content LIKE '%needle%' "
        "FROM messages WHERE content IS ?"
    )
    assert "CHR(10)" in translated
    assert "STRPOS(content, 'x')" in translated
    assert "LIKE '%%needle%%'" in translated
    assert "content IS NOT DISTINCT FROM %s" in translated


def test_sessiondb_dispatch_does_not_touch_state_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_DATABASE_URL", "postgresql://user:secret@db.example/hermes")
    _config(tmp_path, "database:\n  backend: postgres\n")

    class FakePostgres:
        def __init__(self, *args, **kwargs):
            self.args, self.kwargs = args, kwargs

    import hermes_state_postgres
    monkeypatch.setattr(hermes_state_postgres, "PostgresSessionDB", FakePostgres)
    from hermes_state import SessionDB
    result = SessionDB(db_path=tmp_path / "state.db")
    assert isinstance(result, FakePostgres)
    assert not (tmp_path / "state.db").exists()


def test_database_unavailable_error_redacts_password(monkeypatch):
    class UnavailablePool:
        def __init__(self, *args, **kwargs):
            pass

        def wait(self, timeout):
            raise RuntimeError("postgresql://user:secret@db.example/hermes")

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "psycopg_pool", SimpleNamespace(ConnectionPool=UnavailablePool))
    from hermes_state_postgres import PostgresSessionDB
    settings = DatabaseSettings(
        backend="postgres", url="postgresql://user:secret@db.example/hermes",
    )
    with pytest.raises(DatabaseUnavailableError) as caught:
        PostgresSessionDB(settings=settings)
    assert "secret" not in str(caught.value)
    assert "db.example" in str(caught.value)
