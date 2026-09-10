#!/usr/bin/env python3
"""Non-destructively import Hermes SQLite stores into PostgreSQL schemas."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes_db import connect_database, is_postgres_connection  # noqa: E402
from hermes_state_backend import load_database_settings  # noqa: E402


SKIP_TABLE_PREFIXES = ("sqlite_", "messages_fts")
KNOWN_STORES = (
    "state.db", "response_store.db", "runs_idempotency.db", "projects.db",
    "cron/executions.db", "cron/deliveries.db", "cron/notepad.db", "kanban.db",
    "gateway/discord_message_recovery.db", "verification_evidence.db",
)


def discover(home: Path) -> list[Path]:
    paths = [home / item for item in KNOWN_STORES]
    paths.extend(sorted((home / "kanban" / "boards").glob("*/kanban.db")))
    return [path for path in paths if path.is_file()]


def _tables(source: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = source.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY rowid"
    ).fetchall()
    return [
        (str(name), str(ddl)) for name, ddl in rows
        if not str(name).startswith(SKIP_TABLE_PREFIXES)
    ]


def _indexes(source: sqlite3.Connection) -> list[str]:
    rows = source.execute(
        "SELECT name, tbl_name, sql FROM sqlite_master "
        "WHERE type='index' AND sql IS NOT NULL ORDER BY rowid"
    ).fetchall()
    return [
        str(ddl) for name, table, ddl in rows
        if not str(name).startswith(SKIP_TABLE_PREFIXES)
        and not str(table).startswith(SKIP_TABLE_PREFIXES)
    ]


def _portable_ddl(ddl: str) -> str:
    sql = ddl
    sql = re.sub(r"^CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)", "CREATE TABLE IF NOT EXISTS ", sql, flags=re.I)
    sql = sql.replace(" WITHOUT ROWID", "").replace(" STRICT", "")
    return sql


def _portable_index(ddl: str) -> str:
    sql = ddl
    return re.sub(
        r"^CREATE(\s+UNIQUE)?\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)",
        r"CREATE\1 INDEX IF NOT EXISTS ", sql, flags=re.I,
    )


def import_store(path: Path, *, dry_run: bool = False) -> dict:
    report = {"source": str(path), "tables": {}, "imported": 0, "conflicts": 0}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as source:
        source.row_factory = sqlite3.Row
        source.execute("BEGIN")
        if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite source failed integrity_check; import refused")
        if source.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("SQLite source has foreign-key violations; import refused")
        tables = _tables(source)
        indexes = _indexes(source)
        if dry_run:
            report["tables"] = {name: source.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                                for name, _ in tables}
            return report
        if path.name == "state.db":
            # Install the canonical, versioned schema before copying. Generic
            # source DDL must never define an old SQLite layout in PostgreSQL.
            from hermes_state import SessionDB
            canonical = SessionDB(db_path=path)
            canonical.close()
        target = connect_database(path, isolation_level=None)
        if not is_postgres_connection(target):
            target.close()
            raise RuntimeError("database.backend must be postgres")
        try:
            target.execute("BEGIN IMMEDIATE")
            target.execute("CREATE TABLE IF NOT EXISTS hermes_sqlite_import (source TEXT PRIMARY KEY)")
            if target.execute("SELECT 1 FROM hermes_sqlite_import WHERE source=?", (str(path.resolve()),)).fetchone():
                raise RuntimeError("This SQLite source has already been imported; repeat import refused")
            for name, ddl in tables:
                target.execute(_portable_ddl(ddl))
            for ddl in indexes:
                target.execute(_portable_index(ddl))
            for name, _ in tables:
                if path.name == "state.db" and name == "schema_version":
                    continue
                source_columns = [row[1] for row in source.execute(f'PRAGMA table_info("{name}")')]
                target_columns = {row[1] for row in target.execute(f'PRAGMA table_info("{name}")')}
                columns = [column for column in source_columns if column in target_columns]
                if not columns:
                    continue
                rows = source.execute(f'SELECT {", ".join(chr(34)+c+chr(34) for c in columns)} FROM "{name}"').fetchall()
                before = target.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                quoted = ", ".join(f'"{column}"' for column in columns)
                placeholders = ", ".join("?" for _ in columns)
                if rows:
                    target.executemany(
                        f'INSERT INTO "{name}" ({quoted}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                        [tuple(row[column] for column in columns) for row in rows],
                    )
                after = target.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                added = int(after) - int(before)
                report["tables"][name] = {"source": len(rows), "imported": added}
                report["imported"] += added
                report["conflicts"] += len(rows) - added
                if "id" in columns:
                    sequence = target.execute(
                        "SELECT pg_get_serial_sequence(?, 'id')", (name,)
                    ).fetchone()[0]
                    if sequence:
                        target.execute(
                            "SELECT setval(?, GREATEST(COALESCE((SELECT MAX(id) FROM \""
                            + name + "\"), 1), 1), true)", (sequence,))
            target.execute("INSERT INTO hermes_sqlite_import(source) VALUES (?)", (str(path.resolve()),))
            target.commit()
        except Exception:
            target.rollback()
            raise
        finally:
            target.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, required=True, help="Hermes profile home containing SQLite stores")
    parser.add_argument("--store", action="append", type=Path, help="Specific SQLite store (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="Inventory rows without writing PostgreSQL")
    parser.add_argument("--report", type=Path, help="Write the JSON report to this path")
    args = parser.parse_args()
    home = args.home.expanduser().resolve()
    settings = load_database_settings(home)
    if settings.backend != "postgres" or not settings.url:
        parser.error("the selected home must configure database.backend: postgres and HERMES_DATABASE_URL")
    stores = [item.expanduser().resolve() for item in args.store] if args.store else discover(home)
    reports = [import_store(path, dry_run=args.dry_run) for path in stores]
    result = {"home": str(home), "dry_run": args.dry_run, "stores": reports}
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.report:
        args.report.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
