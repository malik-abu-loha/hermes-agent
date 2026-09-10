#!/usr/bin/env python3
"""Back up or restore all Hermes PostgreSQL schemas using native pg tools."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes_state_backend import load_database_settings  # noqa: E402


def _pg_env(url: str) -> dict[str, str]:
    parsed = urlsplit(url)
    # Do not let ambient libpq settings redirect a checked restore target.
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    values = {
        "PGHOST": parsed.hostname,
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": unquote(parsed.username or ""),
        "PGPASSWORD": unquote(parsed.password or ""),
        "PGDATABASE": unquote(parsed.path.lstrip("/")),
    }
    query = parse_qs(parsed.query)
    if query.get("sslmode"):
        values["PGSSLMODE"] = query["sslmode"][-1]
    env.update({key: value for key, value in values.items() if value})
    return env


def _settings(home: Path):
    settings = load_database_settings(home)
    if settings.backend != "postgres" or not settings.url:
        raise SystemExit("database.backend must be postgres and HERMES_DATABASE_URL must be set")
    return settings


def _schemas(url: str) -> list[str]:
    import psycopg
    with psycopg.connect(url, application_name="hermes-backup") as conn:
        return [row[0] for row in conn.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'hermes\\_%' ESCAPE '\\' ORDER BY schema_name"
        )]


def backup(home: Path, output: Path) -> None:
    if output.exists():
        raise SystemExit("backup output already exists; choose a new archive path")
    settings = _settings(home)
    schemas = _schemas(settings.url)
    if not schemas:
        raise SystemExit("no Hermes PostgreSQL schemas found")
    output.parent.mkdir(parents=True, exist_ok=True)
    command = ["pg_dump", "--format=custom", "--no-owner", "--no-privileges", "--file", str(output)]
    for schema in schemas:
        command.extend(("--schema", schema))
    subprocess.run(command, env=_pg_env(settings.url), check=True)
    subprocess.run(["pg_restore", "--list", str(output)], check=True, stdout=subprocess.DEVNULL)
    print(f"Verified PostgreSQL backup: {output} ({len(schemas)} Hermes schemas)")


def restore(home: Path, archive: Path, *, confirmed_empty: bool) -> None:
    if not confirmed_empty:
        raise SystemExit("restore requires --confirm-empty-target")
    settings = _settings(home)
    existing = _schemas(settings.url)
    if existing:
        raise SystemExit("target already contains Hermes schemas; restore is intentionally non-destructive")
    subprocess.run(
        ["pg_restore", "--exit-on-error", "--single-transaction", "--no-owner", "--no-privileges", "--dbname", _pg_env(settings.url)["PGDATABASE"], str(archive)],
        env=_pg_env(settings.url), check=True,
    )
    print(f"Restored PostgreSQL backup: {archive}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", required=True, type=Path)
    sub = parser.add_subparsers(dest="action", required=True)
    backup_parser = sub.add_parser("backup")
    backup_parser.add_argument("output", type=Path)
    restore_parser = sub.add_parser("restore")
    restore_parser.add_argument("archive", type=Path)
    restore_parser.add_argument("--confirm-empty-target", action="store_true")
    args = parser.parse_args()
    if args.action == "backup":
        backup(args.home.resolve(), args.output.resolve())
    else:
        restore(args.home.resolve(), args.archive.resolve(), confirmed_empty=args.confirm_empty_target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
