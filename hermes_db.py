"""Backend-neutral connections for Hermes-owned durable SQL stores.

SQLite remains the zero-configuration default.  In PostgreSQL mode every
logical SQLite file maps to an isolated PostgreSQL schema; code which already
uses the small DB-API subset can therefore keep its transaction semantics
without ever creating a local ``*.db`` file.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home
from hermes_state_backend import (
    DatabaseSettings, DatabaseUnavailableError, load_database_settings,
    sanitized_database_url,
)


def settings_for_path(path: str | Path | None) -> DatabaseSettings:
    if path is None or str(path) == ":memory:":
        return load_database_settings()
    candidate = Path(path).expanduser().resolve()
    for parent in (candidate.parent, *candidate.parents):
        if (parent / "config.yaml").is_file():
            return load_database_settings(parent)
    return load_database_settings()


def postgres_schema_for_path(path: str | Path | None) -> str:
    """Stable, identifier-safe namespace for one historical SQLite store."""
    candidate = Path(path or (get_hermes_home() / "state.db")).expanduser().resolve()
    identity = str(candidate)
    settings = settings_for_path(candidate)
    if settings.namespace is not None:
        home = next((parent for parent in candidate.parents if (parent / "config.yaml").is_file()),
                    get_hermes_home().resolve())
        try:
            relative = candidate.relative_to(home).as_posix()
        except ValueError:
            raise ValueError("Namespaced PostgreSQL stores must be inside their configured profile home") from None
        identity = settings.namespace + "\0" + relative
    digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
    stem = re.sub(r"[^a-z0-9]+", "_", candidate.stem.lower()).strip("_")[:24] or "state"
    return f"hermes_{stem}_{digest}"


def _qmarks(sql: str) -> str:
    out: list[str] = []
    quote: str | None = None
    i = 0
    while i < len(sql):
        char = sql[i]
        if quote:
            # Psycopg parses percent placeholders even inside SQL string
            # literals when a parameter sequence is supplied.
            out.append("%%" if char == "%" else char)
            if char == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    out.append(sql[i + 1]); i += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char; out.append(char)
        elif char == "?":
            out.append("%s")
        elif char == "%":
            out.append("%%")
        else:
            out.append(char)
        i += 1
    return "".join(out)


def translate_sql(sql: str) -> str:
    translated = _qmarks(sql)
    translated = translated.replace("X'0A'", "E'\\n'").replace("X'0D'", "E'\\r'")
    literals: list[str] = []

    def preserve(match):
        literals.append(match.group())
        return f"__hermes_literal_{len(literals) - 1}__"

    # Dialect rewrites must not mutate user text or quoted identifiers.
    translated = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", preserve, translated)
    translated = re.sub(r"\bBEGIN\s+IMMEDIATE\b", "BEGIN", translated, flags=re.I)
    translated = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", translated, flags=re.I)
    if re.search(r"\bINSERT\s+OR\s+IGNORE\b", sql, flags=re.I) and "ON CONFLICT" not in translated.upper():
        translated = translated.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    translated = re.sub(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", "BIGSERIAL PRIMARY KEY", translated, flags=re.I)
    translated = re.sub(r"\bAUTOINCREMENT\b", "", translated, flags=re.I)
    # SQLite INTEGER is signed 64-bit. PostgreSQL INTEGER is only 32-bit, so
    # preserving the source contract requires BIGINT (notably process stamps).
    translated = re.sub(r"\bINTEGER\b", "BIGINT", translated, flags=re.I)
    translated = re.sub(r"\bBLOB\b", "BYTEA", translated, flags=re.I)
    translated = re.sub(r"\bCHAR\s*\(", "CHR(", translated, flags=re.I)
    translated = re.sub(r"\bINSTR\s*\(", "STRPOS(", translated, flags=re.I)
    translated = re.sub(
        r"CAST\s*\(\s*([\w.]+)\s+AS\s+BYTEA\s*\)",
        r"convert_to(\1, 'UTF8')", translated, flags=re.I)
    translated = re.sub(r"\s+COLLATE\s+NOCASE\b", "", translated, flags=re.I)
    translated = re.sub(r"(?<![\"'])\bgrant\b(?![\"'])", '"grant"', translated, flags=re.I)
    translated = re.sub(r"\bIS\s+(%s)", r"IS NOT DISTINCT FROM \1", translated, flags=re.I)
    translated = re.sub(r"\b(OR|AND)\s+(%s)(?=\s|\))", r"\1 (\2 <> 0)", translated, flags=re.I)
    translated = re.sub(r"\bMAX\s*\(([^,()]+),\s*(%s)\)", r"GREATEST(\1, \2)", translated, flags=re.I)
    translated = translated.replace("LIMIT -1", "LIMIT ALL")
    for index, literal in enumerate(literals):
        translated = translated.replace(f"__hermes_literal_{index}__", literal)
    return translated


class Row(dict):
    def __init__(self, columns: list[str], values):
        values = tuple(values)
        super().__init__(zip(columns, values))
        self._values = values

    def __getitem__(self, key):
        return self._values[key] if isinstance(key, (int, slice)) else super().__getitem__(key)

    def keys(self):
        return super().keys()

    def __iter__(self):
        return iter(self._values)


class Cursor:
    def __init__(self, raw, connection: "PostgresConnection"):
        self.raw = raw
        self.connection = connection
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
        cols = [getattr(item, "name", item[0]) for item in self.raw.description]
        return Row(cols, row)

    def fetchone(self):
        return self._row(self.raw.fetchone())

    def fetchall(self):
        return [self._row(row) for row in self.raw.fetchall()]

    def fetchmany(self, size: int | None = None):
        rows = self.raw.fetchmany(size) if size is not None else self.raw.fetchmany()
        return [self._row(row) for row in rows]

    def __iter__(self):
        while (row := self.fetchone()) is not None:
            yield row

    def execute(self, sql: str, params=()):
        return self.connection._execute_on(self.raw, sql, params)


class PostgresConnection:
    """Small sqlite3-compatible wrapper over one psycopg connection."""

    backend = "postgres"

    def __init__(self, raw, schema: str):
        self.raw = raw
        self.schema = schema
        self.row_factory = Row
        self._closed = False

    def _pragma(self, cursor, sql: str):
        table = re.match(r"\s*PRAGMA\s+table_info\s*\(?['\"]?([\w]+)", sql, re.I)
        if table:
            cursor.execute(
                "SELECT ordinal_position - 1 AS cid, column_name AS name, data_type AS type, "
                "CASE WHEN is_nullable='NO' THEN 1 ELSE 0 END AS notnull, column_default AS dflt_value, "
                "COALESCE((SELECT kcu.ordinal_position FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
                "AND tc.table_schema=kcu.table_schema WHERE tc.constraint_type='PRIMARY KEY' "
                "AND tc.table_schema=current_schema() AND tc.table_name=c.table_name "
                "AND kcu.column_name=c.column_name), 0) AS pk "
                "FROM information_schema.columns c WHERE table_schema=current_schema() AND table_name=%s "
                "ORDER BY ordinal_position", (table.group(1),))
            return Cursor(cursor, self)
        if re.match(r"\s*PRAGMA\s+(integrity_check|quick_check)", sql, re.I):
            raise NotImplementedError("SQLite integrity checks are not PostgreSQL integrity verification")
        if re.match(r"\s*PRAGMA\s+(foreign_key_check|foreign_key_list)", sql, re.I):
            raise NotImplementedError("Use PostgreSQL constraint metadata, not SQLite foreign-key PRAGMAs")
        if re.match(r"\s*PRAGMA\s+(journal_mode|wal_checkpoint)", sql, re.I):
            cursor.execute("SELECT 'postgres' AS journal_mode")
            return Cursor(cursor, self)
        raise NotImplementedError("Unsupported SQLite PRAGMA on PostgreSQL")

    def _replace_sql(self, sql: str) -> str:
        translated = translate_sql(re.sub(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", "INSERT INTO", sql, flags=re.I))
        match = re.match(r"\s*INSERT\s+INTO\s+([\w]+)\s*\(([^)]+)\)", translated, re.I | re.S)
        if not match:
            raise ValueError("unsupported INSERT OR REPLACE statement")
        table = match.group(1)
        columns = [item.strip().strip('"') for item in match.group(2).split(',')]
        with self.raw.cursor() as probe:
            probe.execute(
                "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name "
                "AND tc.table_schema=kcu.table_schema WHERE tc.table_schema=current_schema() "
                "AND tc.table_name=%s AND tc.constraint_type='PRIMARY KEY' ORDER BY kcu.ordinal_position", (table,))
            keys = [row[0] for row in probe.fetchall()]
        if not keys or any(key not in columns for key in keys):
            raise ValueError(f"cannot resolve primary key for INSERT OR REPLACE on {table}")
        updates = [col for col in columns if col not in keys]
        conflict = ", ".join(f'"{key}"' for key in keys)
        if updates:
            action = "DO UPDATE SET " + ", ".join(f'"{col}"=EXCLUDED."{col}"' for col in updates)
        else:
            action = "DO NOTHING"
        return translated.rstrip().rstrip(";") + f" ON CONFLICT ({conflict}) {action}"

    def _execute_on(self, raw_cursor, sql: str, params=()):
        if re.match(r"\s*PRAGMA\b", sql, re.I):
            return self._pragma(raw_cursor, sql)
        if re.match(r"\s*SELECT\s+.*\bsqlite_master\b", sql, re.I | re.S):
            kind = "index" if re.search(r"type\s*=\s*'index'", sql, re.I) else "table"
            catalog = "pg_indexes" if kind == "index" else "information_schema.tables"
            column = "indexname" if kind == "index" else "table_name"
            schema_col = "schemaname" if kind == "index" else "table_schema"
            in_match = re.search(r"name\s+IN\s*\(([^)]+)\)", sql, re.I | re.S)
            literal_names = re.findall(r"'([^']+)'", in_match.group(1)) if in_match else []
            exact = re.search(r"name\s*=\s*'([^']+)'", sql, re.I)
            names = list(params) if params else (literal_names or ([exact.group(1)] if exact else []))
            projection = "1"
            if re.match(r"\s*SELECT\s+name\b", sql, re.I):
                projection = column
            elif re.match(r"\s*SELECT\s+sql\b", sql, re.I):
                raise NotImplementedError("SQLite CREATE TABLE text is unavailable on PostgreSQL")
            placeholders = ",".join("%s" for _ in names)
            raw_cursor.execute(
                f"SELECT {projection} FROM {catalog} WHERE {schema_col}=current_schema() "
                f"AND {column} IN ({placeholders})", names)
            return Cursor(raw_cursor, self)
        translated = self._replace_sql(sql) if re.search(r"\bINSERT\s+OR\s+REPLACE\b", sql, re.I) else translate_sql(sql)
        if re.match(r"\s*BEGIN\s+IMMEDIATE\b", sql, re.I):
            if not self.in_transaction:
                raw_cursor.execute("BEGIN")
            # Preserve SQLite's check-then-write exclusion across processes.
            raw_cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.schema,))
            return Cursor(raw_cursor, self)
        if re.match(r"\s*BEGIN\b", translated, re.I) and self.in_transaction:
            raw_cursor.execute("SELECT NULL WHERE FALSE")
            return Cursor(raw_cursor, self)
        insert = re.match(r"\s*INSERT\s+INTO\s+([\w]+)\b", translated, re.I)
        if insert and "RETURNING" not in translated.upper():
            with self.raw.cursor() as probe:
                probe.execute(
                    "SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() "
                    "AND table_name=%s AND column_name='id' AND column_default LIKE 'nextval(%%'",
                    (insert.group(1),))
                if probe.fetchone() is not None:
                    translated = translated.rstrip().rstrip(";") + " RETURNING id"
        raw_cursor.execute(translated, params)
        return Cursor(raw_cursor, self)

    def execute(self, sql: str, params=()):
        return self._execute_on(self.raw.cursor(), sql, params)

    def executemany(self, sql: str, params):
        cursor = self.raw.cursor()
        cursor.executemany(translate_sql(sql), params)
        return Cursor(cursor, self)

    def executescript(self, sql: str):
        self.raw.execute(translate_sql(sql).replace("%%", "%"))
        return self

    def cursor(self):
        return Cursor(self.raw.cursor(), self)

    @property
    def in_transaction(self):
        return self.raw.info.transaction_status.value != 0

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        if not self._closed:
            self._closed = True
            self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


def connect_database(
    path: str | Path, *, timeout: float = 10, check_same_thread: bool = True,
    isolation_level: str | None = "DEFERRED",
):
    """Connect to a Hermes-owned logical store using the selected backend."""
    settings = settings_for_path(path)
    if settings.backend == "sqlite" or str(path) == ":memory:":
        return sqlite3.connect(
            path, timeout=timeout, check_same_thread=check_same_thread,
            isolation_level=isolation_level,
        )
    if not settings.url:
        raise RuntimeError("PostgreSQL backend requires HERMES_DATABASE_URL")
    import psycopg
    from psycopg import sql as pg_sql

    raw = None
    try:
        raw = psycopg.connect(
            settings.url, connect_timeout=settings.connect_timeout,
            application_name="hermes-agent",
            options=f"-c lock_timeout={int(settings.pool_timeout * 1000)}",
        )
        schema = postgres_schema_for_path(path)
        raw.execute("SELECT pg_advisory_xact_lock(hashtext('hermes-agent-schema'))")
        raw.execute(pg_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(pg_sql.Identifier(schema)))
        raw.execute(pg_sql.SQL("SET search_path TO {}").format(pg_sql.Identifier(schema)))
        raw.commit()
        raw.autocommit = isolation_level is None
        return PostgresConnection(raw, schema)
    except Exception as exc:
        if raw is not None:
            raw.close()
        raise DatabaseUnavailableError(
            f"PostgreSQL durable store unavailable at "
            f"{sanitized_database_url(settings.url)} ({type(exc).__name__})"
        ) from None


def is_postgres_connection(conn: Any) -> bool:
    return getattr(conn, "backend", None) == "postgres"
