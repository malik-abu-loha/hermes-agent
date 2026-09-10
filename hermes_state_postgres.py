"""PostgreSQL implementation of Hermes' canonical ``SessionDB`` boundary.

The business-level session/message/routing mixins remain shared with SQLite.
This module supplies PostgreSQL connection, transaction, schema and search
semantics while deliberately omitting every SQLite file/WAL/PRAGMA path.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Collection, Iterator, Optional

from hermes_state import SessionDB
from hermes_state_backend import (
    DatabaseSettings, DatabaseUnavailableError, sanitized_database_url,
)
from hermes_state_common import SCHEMA_VERSION
from hermes_constants import get_hermes_home
from hermes_db import postgres_schema_for_path, settings_for_path, translate_sql

logger = logging.getLogger(__name__)

POSTGRES_SCHEMA_VERSION = 2

_POSTGRES_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS hermes_schema_migrations (
    version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS system_prompts (hash TEXT PRIMARY KEY, prompt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY, source TEXT NOT NULL, user_id TEXT, session_key TEXT, chat_id TEXT,
    chat_type TEXT, thread_id TEXT, display_name TEXT, origin_json TEXT, expiry_finalized INTEGER DEFAULT 0,
    model TEXT, model_config TEXT, system_prompt TEXT, system_prompt_hash TEXT REFERENCES system_prompts(hash),
    parent_session_id TEXT REFERENCES sessions(id), started_at DOUBLE PRECISION NOT NULL, ended_at DOUBLE PRECISION,
    end_reason TEXT, message_count INTEGER DEFAULT 0, tool_call_count INTEGER DEFAULT 0,
    input_tokens BIGINT DEFAULT 0, output_tokens BIGINT DEFAULT 0, cache_read_tokens BIGINT DEFAULT 0,
    cache_write_tokens BIGINT DEFAULT 0, reasoning_tokens BIGINT DEFAULT 0, cwd TEXT, git_branch TEXT,
    git_repo_root TEXT, git_metadata_generation INTEGER NOT NULL DEFAULT 0, billing_provider TEXT,
    billing_base_url TEXT, billing_mode TEXT, estimated_cost_usd DOUBLE PRECISION,
    actual_cost_usd DOUBLE PRECISION, cost_status TEXT, cost_source TEXT, pricing_version TEXT,
    title TEXT, title_source TEXT, last_activity_at DOUBLE PRECISION, last_activity_description TEXT,
    last_activity_provenance TEXT, api_call_count INTEGER DEFAULT 0, handoff_state TEXT,
    handoff_platform TEXT, handoff_error TEXT, compression_failure_cooldown_until DOUBLE PRECISION,
    compression_failure_error TEXT, compression_fallback_streak INTEGER NOT NULL DEFAULT 0,
    compression_ineffective_count INTEGER NOT NULL DEFAULT 0, compression_recovery_deadline DOUBLE PRECISION,
    profile_name TEXT, rewind_count INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0, hidden INTEGER NOT NULL DEFAULT 0, last_read_at DOUBLE PRECISION,
    tool_names TEXT
);
CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role TEXT NOT NULL, content TEXT, tool_call_id TEXT, tool_calls TEXT, tool_name TEXT,
    effect_disposition TEXT, timestamp DOUBLE PRECISION NOT NULL, token_count INTEGER, finish_reason TEXT,
    reasoning TEXT, reasoning_content TEXT, reasoning_details TEXT, codex_reasoning_items TEXT,
    codex_message_items TEXT, platform_message_id TEXT, observed INTEGER DEFAULT 0,
    _compressed_summary INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1,
    compacted INTEGER NOT NULL DEFAULT 0, api_content TEXT, display_kind TEXT, display_metadata TEXT,
    display_identity BYTEA, display_order INTEGER,
    search_vector TSVECTOR GENERATED ALWAYS AS
      (to_tsvector('simple', coalesce(content, '') || ' ' || coalesce(tool_name, '') || ' ' || coalesce(tool_calls, ''))) STORED
);
CREATE TABLE IF NOT EXISTS session_model_usage (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, model TEXT NOT NULL,
    billing_provider TEXT NOT NULL DEFAULT '', billing_base_url TEXT NOT NULL DEFAULT '',
    billing_mode TEXT NOT NULL DEFAULT '', task TEXT NOT NULL DEFAULT '', api_call_count INTEGER NOT NULL DEFAULT 0,
    input_tokens BIGINT NOT NULL DEFAULT 0, output_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0, cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    reasoning_tokens BIGINT NOT NULL DEFAULT 0, estimated_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    actual_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0, cost_status TEXT, cost_source TEXT,
    first_seen DOUBLE PRECISION, last_seen DOUBLE PRECISION,
    PRIMARY KEY (session_id, model, billing_provider, billing_base_url, billing_mode, task)
);
CREATE TABLE IF NOT EXISTS state_meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS hermes_change_revision (
    id INTEGER PRIMARY KEY CHECK (id = 1), revision BIGINT NOT NULL DEFAULT 0
);
INSERT INTO hermes_change_revision(id, revision) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS gateway_routing (
    scope TEXT NOT NULL DEFAULT '', session_key TEXT NOT NULL, entry_json TEXT NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY (scope, session_key)
);
CREATE TABLE IF NOT EXISTS gateway_hygiene_state (session_key TEXT PRIMARY KEY, failure_streak INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS conversation_generations (
    source TEXT NOT NULL, session_key TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_key)
);
CREATE TABLE IF NOT EXISTS gateway_heartbeats (
    backend_id TEXT PRIMARY KEY, pid BIGINT NOT NULL, started_at DOUBLE PRECISION NOT NULL,
    last_heartbeat DOUBLE PRECISION NOT NULL, profile TEXT NOT NULL DEFAULT '', host TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS compression_locks (
    session_id TEXT PRIMARY KEY, holder TEXT NOT NULL, acquired_at DOUBLE PRECISION NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS session_turn_leases (
    conversation_id TEXT PRIMARY KEY, holder TEXT NOT NULL, acquired_at DOUBLE PRECISION NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS async_delegations (
    delegation_id TEXT PRIMARY KEY, origin_session TEXT NOT NULL, origin_ui_session_id TEXT NOT NULL DEFAULT '',
    parent_session_id TEXT, state TEXT NOT NULL, dispatched_at DOUBLE PRECISION NOT NULL,
    completed_at DOUBLE PRECISION, updated_at DOUBLE PRECISION NOT NULL, event_json TEXT, result_json TEXT,
    delivery_state TEXT NOT NULL DEFAULT 'pending', delivery_attempts INTEGER NOT NULL DEFAULT 0,
    delivered_at DOUBLE PRECISION, owner_pid BIGINT, owner_started_at BIGINT, task_json TEXT,
    delivery_claim TEXT, delivery_claimed_at DOUBLE PRECISION,
    origin_session_id TEXT NOT NULL DEFAULT ''
);
ALTER TABLE async_delegations ADD COLUMN IF NOT EXISTS origin_session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE async_delegations ALTER COLUMN owner_pid TYPE BIGINT;
ALTER TABLE async_delegations ALTER COLUMN owner_started_at TYPE BIGINT;
CREATE TABLE IF NOT EXISTS telegram_dm_topic_mode (
    profile_name TEXT NOT NULL DEFAULT 'default', chat_id TEXT NOT NULL, user_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1, activated_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL, has_topics_enabled INTEGER,
    allows_users_to_create_topics INTEGER, capability_checked_at DOUBLE PRECISION,
    intro_message_id TEXT, pinned_message_id TEXT,
    PRIMARY KEY (profile_name, chat_id)
);
CREATE TABLE IF NOT EXISTS telegram_dm_topic_bindings (
    profile_name TEXT NOT NULL DEFAULT 'default', chat_id TEXT NOT NULL, thread_id TEXT NOT NULL,
    user_id TEXT NOT NULL, session_key TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    managed_mode TEXT NOT NULL DEFAULT 'auto', linked_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY (profile_name, chat_id, thread_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_dm_topic_bindings_session
  ON telegram_dm_topic_bindings(session_id);
CREATE INDEX IF NOT EXISTS idx_telegram_dm_topic_bindings_user
  ON telegram_dm_topic_bindings(profile_name, user_id, chat_id);
CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_sessions_source_id ON sessions(source, id);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id, id);
CREATE INDEX IF NOT EXISTS idx_messages_search_vector ON messages USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_messages_assistant_calls_by_session ON messages(session_id)
  WHERE role = 'assistant' AND tool_calls IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_compression_locks_expires ON compression_locks(expires_at);
CREATE INDEX IF NOT EXISTS idx_session_turn_leases_expires ON session_turn_leases(expires_at);
CREATE INDEX IF NOT EXISTS idx_async_delegations_delivery ON async_delegations(delivery_state, completed_at);
CREATE OR REPLACE FUNCTION hermes_bump_change_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE hermes_change_revision SET revision = revision + 1 WHERE id = 1;
  RETURN NULL;
END
$$;
DROP TRIGGER IF EXISTS hermes_sessions_changed ON sessions;
CREATE TRIGGER hermes_sessions_changed AFTER INSERT OR UPDATE OR DELETE ON sessions
FOR EACH STATEMENT EXECUTE FUNCTION hermes_bump_change_revision();
DROP TRIGGER IF EXISTS hermes_messages_changed ON messages;
CREATE TRIGGER hermes_messages_changed AFTER INSERT OR UPDATE OR DELETE ON messages
FOR EACH STATEMENT EXECUTE FUNCTION hermes_bump_change_revision();
"""

_COMPAT_FUNCTIONS = r"""
CREATE OR REPLACE FUNCTION json_extract(raw TEXT, path TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN raw IS NULL THEN NULL ELSE raw::jsonb #>> string_to_array(trim(leading '$.' from path), '.') END
$$;
CREATE OR REPLACE FUNCTION json_type(raw TEXT, path TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN raw IS NULL THEN NULL ELSE jsonb_typeof(raw::jsonb #> string_to_array(trim(leading '$.' from path), '.')) END
$$;
CREATE OR REPLACE FUNCTION json_remove(raw TEXT, path TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN raw IS NULL THEN NULL ELSE (raw::jsonb #- string_to_array(trim(leading '$.' from path), '.'))::text END
$$;
CREATE OR REPLACE FUNCTION json_set(raw TEXT, path TEXT, value TEXT) RETURNS TEXT
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT jsonb_set(coalesce(raw::jsonb, '{}'::jsonb), string_to_array(trim(leading '$.' from path), '.'), to_jsonb(value), true)::text
$$;
CREATE OR REPLACE FUNCTION instr(haystack TEXT, needle TEXT) RETURNS INTEGER
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$ SELECT strpos(haystack, needle) $$;
CREATE OR REPLACE FUNCTION json_valid(raw TEXT) RETURNS INTEGER
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN PERFORM raw::jsonb; RETURN 1; EXCEPTION WHEN others THEN RETURN 0; END
$$;
"""


class _HybridRow(dict):
    def __init__(self, columns: list[str], values: tuple[Any, ...]):
        super().__init__(zip(columns, values))
        self._values = values

    def __getitem__(self, key):
        return self._values[key] if isinstance(key, (int, slice)) else super().__getitem__(key)


def _qmark_sql(sql: str) -> str:
    """Convert DB-API qmarks outside quoted strings to psycopg placeholders."""
    out: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote:
            out.append(char)
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    out.append(sql[index + 1]); index += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char; out.append(char)
        elif char == "?":
            out.append("%s")
        else:
            out.append(char)
        index += 1
    return "".join(out)


def _translate_sql(sql: str, *, returning_id: bool = True) -> str:
    # Share the compatibility translation with auxiliary stores so newly
    # introduced SQLite idioms cannot silently diverge between backends.
    translated = translate_sql(sql)
    translated = re.sub(
        r"SELECT\s+last_insert_rowid\(\)",
        "SELECT currval(pg_get_serial_sequence('messages', 'id'))",
        translated, flags=re.I,
    )
    if returning_id and re.match(r"\s*INSERT\s+INTO\s+messages\b", translated, re.I) and "RETURNING" not in translated.upper():
        translated = translated.rstrip().rstrip(";") + " RETURNING id"
    return translated


class _Cursor:
    def __init__(self, raw):
        self.raw = raw
        self._lastrowid = None

    @property
    def rowcount(self):
        return self.raw.rowcount

    @property
    def lastrowid(self):
        if self._lastrowid is None and self.raw.description:
            row = self.raw.fetchone()
            self._lastrowid = None if row is None else row[0]
        return self._lastrowid

    def _row(self, row):
        if row is None:
            return None
        columns = [item.name for item in self.raw.description]
        return _HybridRow(columns, tuple(row))

    def fetchone(self):
        return self._row(self.raw.fetchone())

    def fetchall(self):
        rows = self.raw.fetchall()
        return [self._row(row) for row in rows]

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    def execute(self, sql: str, params=()):
        self.raw.execute(_translate_sql(sql), params)
        return self


class _Connection:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql: str, params=()):
        cursor = _Cursor(self.raw.cursor())
        cursor.execute(sql, params)
        return cursor

    def executemany(self, sql: str, params):
        cursor = _Cursor(self.raw.cursor())
        cursor.raw.executemany(_translate_sql(sql, returning_id=False), params)
        return cursor

    def cursor(self):
        return _Cursor(self.raw.cursor())


class PostgresSessionDB(SessionDB):
    """Canonical Hermes state backed by a bounded psycopg 3 connection pool."""

    backend = "postgres"

    def __init__(self, db_path: Path | None = None, read_only: bool = False, *, settings: DatabaseSettings | None = None):
        resolved_path = Path(db_path) if db_path is not None else get_hermes_home() / "state.db"
        settings = settings or settings_for_path(resolved_path)
        if settings.backend != "postgres" or not settings.url:
            raise ValueError("PostgresSessionDB requires database.backend: postgres")
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as exc:
            raise RuntimeError("PostgreSQL support requires `pip install 'hermes-agent[postgres]'`") from exc
        self.db_path = resolved_path
        self.read_only = read_only
        self._settings = settings
        self._lock = threading.RLock()
        self._shared_registry_owned = False
        self._closed = False
        self._conn = None
        self._token_queue = __import__("collections").deque()
        self._token_queue_cond = threading.Condition(threading.Lock())
        self._token_writer_thread = None
        self._token_writer_stop = self._token_writer_busy = False
        self._token_atexit_hook = None
        conninfo = settings.url
        self.schema = postgres_schema_for_path(self.db_path)
        kwargs = {
            "connect_timeout": settings.connect_timeout,
            "application_name": "hermes-agent",
            "options": f"-c search_path={self.schema} -c lock_timeout={int(settings.pool_timeout * 1000)}",
        }
        self._pool = ConnectionPool(
            conninfo, min_size=settings.pool_min_size, max_size=settings.pool_max_size,
            timeout=settings.pool_timeout, kwargs=kwargs, open=True,
        )
        try:
            self._pool.wait(timeout=settings.connect_timeout)
            if not read_only:
                self._migrate()
        except Exception as exc:
            self._pool.close()
            safe_url = sanitized_database_url(conninfo)
            raise DatabaseUnavailableError(
                f"PostgreSQL session database unavailable at {safe_url} ({type(exc).__name__})"
            ) from None
        logger.info("Database backend: PostgreSQL (%s); connection pool initialized", sanitized_database_url(conninfo))

    def _migrate(self) -> None:
        with self._pool.connection() as raw:
            with raw.transaction():
                raw.execute("SELECT pg_advisory_xact_lock(hashtext('hermes-agent-schema'))")
                raw.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
                raw.execute(f'SET LOCAL search_path TO "{self.schema}"')
                exists = raw.execute("SELECT to_regclass('hermes_schema_migrations')").fetchone()[0]
                current = 0
                if exists is not None:
                    current = int(raw.execute("SELECT COALESCE(max(version), 0) FROM hermes_schema_migrations").fetchone()[0])
                    if current > POSTGRES_SCHEMA_VERSION:
                        raise RuntimeError("PostgreSQL schema is newer than this Hermes build; downgrade refused")
                    if current == POSTGRES_SCHEMA_VERSION:
                        return
                # No bound parameters: psycopg uses PostgreSQL's simple-query
                # protocol and can apply each deterministic migration script
                # atomically, including dollar-quoted function bodies.
                raw.execute(_COMPAT_FUNCTIONS)
                raw.execute(_POSTGRES_SCHEMA)
                if current < POSTGRES_SCHEMA_VERSION:
                    raw.execute("INSERT INTO hermes_schema_migrations(version) VALUES (%s)", (POSTGRES_SCHEMA_VERSION,))
                    logger.info("Applied PostgreSQL state schema migration %d", POSTGRES_SCHEMA_VERSION)
                raw.execute("DELETE FROM schema_version")
                raw.execute("INSERT INTO schema_version(version) VALUES (%s)", (SCHEMA_VERSION,))

    @contextmanager
    def _read_ctx(self) -> Iterator[_Connection]:
        if self._closed:
            raise RuntimeError("PostgreSQL session store is closed")
        with self._pool.connection(timeout=self._settings.pool_timeout) as raw:
            yield _Connection(raw)

    def _execute_write(self, fn, patience_s: Optional[float] = None):
        if self.read_only:
            raise RuntimeError("PostgreSQL session store is read-only")
        if self._closed:
            raise RuntimeError("PostgreSQL session store is closed")
        with self._pool.connection(timeout=self._settings.pool_timeout) as raw:
            with raw.transaction():
                raw.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.schema,))
                return fn(_Connection(raw))

    def get_meta(self, key: str) -> Optional[str]:
        row = self._read_one("SELECT value FROM state_meta WHERE key = ?", (key,))
        return None if row is None else row[0]

    def flush_token_counts(self) -> None:
        # Preserve the shared mixin contract; queued accounting still uses its
        # existing worker when populated, while the common empty case is free.
        if self._token_queue:
            return super().flush_token_counts()

    def _message_column_names(self, conn) -> list[str]:
        return [row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'messages' ORDER BY ordinal_position"
        ).fetchall() if row[0] != "search_vector"]

    def session_count_by_source(
        self, *, include_archived: bool = False, archived_only: bool = False,
        exclude_children: bool = False,
    ) -> dict[str, int]:
        # The shared implementation's ``self._conn is None`` check is a
        # SQLite closed-handle invariant; pooled PostgreSQL has no global conn.
        from hermes_state_sessions import _session_filter_where, _where_sql
        where, params = _session_filter_where(
            exclude_children=exclude_children, archived_only=archived_only,
            include_archived=include_archived,
        )
        rows = self._read_all(
            "SELECT COALESCE(NULLIF(s.source, ''), 'cli') AS source, COUNT(*) AS count "
            f"FROM sessions s{_where_sql(where, ' ')} "
            "GROUP BY COALESCE(NULLIF(s.source, ''), 'cli') ORDER BY count DESC",
            params,
        )
        return {str(row["source"]): int(row["count"] or 0) for row in rows}

    def _write_sql_logged(self, op: str, session_id: str, sql: str, params) -> None:
        try:
            self._write_sql(sql, params)
        except Exception as exc:
            logger.warning("%s(%s) failed: %s", op, session_id, exc)

    def purge_stale_tool_call_markers(self, *, dry_run: bool = False, backup: bool = True):
        # PostgreSQL backup belongs to the database operator. Preserve the
        # cleanup behavior but never synthesize a filesystem snapshot.
        return super().purge_stale_tool_call_markers(dry_run=dry_run, backup=False)

    def logical_size_bytes(self) -> Optional[int]:
        row = self._read_one("SELECT pg_database_size(current_database())")
        return None if row is None else int(row[0])

    def change_revision(self) -> int:
        row = self._read_one("SELECT revision FROM hermes_change_revision WHERE id = 1")
        return int(row[0] or 0) if row else 0

    def vacuum(self) -> int:
        # PostgreSQL autovacuum is the authority; request-path VACUUM would
        # require leaving the transaction boundary and is intentionally absent.
        return 0

    def fts_rebuild_step(self) -> bool:
        return False  # Generated vectors are updated with each message write.

    def fts_cjk_rebuild_step(self) -> bool:
        return False

    def maybe_auto_prune_and_vacuum(
        self, retention_days=90, min_interval_hours=24, vacuum=True,
        sessions_dir=None, min_vacuum_interval_days=30,
        min_vacuum_freelist_ratio=0.2,
    ):
        from hermes_db import connect_database
        result = {"skipped": False, "pruned": 0, "closed": 0, "vacuumed": False}
        conn = None
        try:
            conn = connect_database(self.db_path, isolation_level=None)
            if not conn.raw.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 2))", (self.schema,)
            ).fetchone()[0]:
                result["skipped"] = True
                return result
            now = time.time()
            previous = self.get_meta("last_auto_prune")
            if previous and now - float(previous) < min_interval_hours * 3600:
                result["skipped"] = True
                return result
            result["pruned"] = self.prune_sessions(
                older_than_days=retention_days, sessions_dir=sessions_dir,
                exclude_active_write_guards=True,
            )
            result["closed"] = len(self.sweep_orphaned_sessions(
                max_idle_seconds=float(retention_days) * 86400,
                sources=self._AUTO_PRUNE_STALE_OPEN_SOURCES, exclude_pinned=True,
                respect_gateway_heartbeats=False,
            ))
            self.set_meta("last_auto_prune", str(now))
        except Exception as exc:
            result["error"] = type(exc).__name__
            logger.warning("PostgreSQL auto-maintenance failed (%s)", type(exc).__name__)
        finally:
            if conn is not None:
                conn.close()
        return result

    def close(self):
        if self._shared_registry_owned:
            from hermes_state_registry import release
            release(self)
            return
        if not self._closed:
            self.flush_token_counts()
            self._closed = True
            self._pool.close()

    def __del__(self):
        pool = self.__dict__.get("_pool")
        if pool is not None and not self.__dict__.get("_closed", True):
            try:
                pool.close()
            except Exception:
                pass

    # SQLite FTS maintenance concepts do not apply to PostgreSQL's generated
    # tsvector + GIN index.
    def fts_rebuild_status(self): return None
    def fts_cjk_rebuild_status(self): return None
    def fts_optimize_available(self): return False
    def retry_deferred_fts_recovery(self): return False
    def optimize_fts(self): return 0
    def rebuild_fts(self): return 0

    def search_messages(
        self, query: str, source_filter: list[str] | None = None, exclude_sources: list[str] | None = None,
        role_filter: list[str] | None = None, limit: int = 20, offset: int = 0, sort: str | None = None,
        include_inactive: bool = False, fields: Optional[Collection[str]] = None,
    ) -> list[dict[str, Any]]:
        result_fields = self._search_message_fields(fields)
        if not query or not query.strip():
            return []
        where = ["m.search_vector @@ websearch_to_tsquery('simple', ?)"]
        params: list[Any] = [query]
        if not include_inactive:
            where.append("(m.active = 1 OR m.compacted = 1)")
        if source_filter == []:
            return []
        if source_filter is not None:
            where.append(f"s.source IN ({','.join('?' for _ in source_filter)})"); params.extend(source_filter)
        if exclude_sources is not None:
            where.append(f"s.source NOT IN ({','.join('?' for _ in exclude_sources)})"); params.extend(exclude_sources)
        if role_filter:
            where.append(f"m.role IN ({','.join('?' for _ in role_filter)})"); params.extend(role_filter)
        direction = "ASC" if (sort or "").lower() == "oldest" else "DESC"
        order = f"m.timestamp {direction}, m.id {direction}" if sort else "rank DESC, m.id DESC"
        sql = f"""
            SELECT m.id, m.session_id, m.role,
              ts_headline('simple', coalesce(m.content, ''), websearch_to_tsquery('simple', ?),
                'StartSel=>>>, StopSel=<<<, MaxWords=40') AS snippet,
              m.timestamp, m.tool_name, s.source, s.model, s.started_at AS session_started,
              ts_rank_cd(m.search_vector, websearch_to_tsquery('simple', ?)) AS rank
            FROM messages m JOIN sessions s ON s.id = m.session_id
            WHERE {' AND '.join(where)} ORDER BY {order} LIMIT ? OFFSET ?
        """
        matches = [dict(row) for row in self._read_all(sql, [query, query, *params, limit, offset])]
        return self._finalize_search_matches(matches, result_fields=result_fields)


__all__ = ["POSTGRES_SCHEMA_VERSION", "PostgresSessionDB"]
