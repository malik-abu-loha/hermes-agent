"""Backend selection for Hermes' canonical session/application state store.

Behavioral selection belongs in ``config.yaml``.  The PostgreSQL URL is a
secret and is therefore read only from ``HERMES_DATABASE_URL``.
"""

from __future__ import annotations

import os
import math
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import yaml

from hermes_constants import get_hermes_home


SUPPORTED_DATABASE_BACKENDS = frozenset({"sqlite", "postgres"})


class DatabaseConfigurationError(ValueError):
    """The durable-state backend configuration is incomplete or invalid."""


class DatabaseUnavailableError(RuntimeError):
    """The configured external state database could not be reached or initialized."""


@dataclass(frozen=True)
class DatabaseSettings:
    backend: str
    url: str | None = None
    connect_timeout: float = 5.0
    pool_timeout: float = 10.0
    pool_min_size: int = 1
    pool_max_size: int = 8
    namespace: str | None = None


def _database_config(home: Path | None = None) -> dict:
    path = (home or get_hermes_home()) / "config.yaml"
    if not path.is_file():
        return {}
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise DatabaseConfigurationError(f"Cannot read database configuration: {type(exc).__name__}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("database", {}), dict):
        raise DatabaseConfigurationError("config.yaml and database must be mappings")
    return value.get("database", {})


def load_database_settings(home: Path | None = None) -> DatabaseSettings:
    config = _database_config(home)
    backend = str(config.get("backend", "sqlite")).strip().lower()
    if backend not in SUPPORTED_DATABASE_BACKENDS:
        supported = ", ".join(sorted(SUPPORTED_DATABASE_BACKENDS))
        raise DatabaseConfigurationError(f"Unsupported database.backend {backend!r}; expected one of: {supported}")
    if backend == "sqlite":
        return DatabaseSettings(backend="sqlite")

    namespace = config.get("namespace")
    if namespace is not None and (not isinstance(namespace, str) or not namespace.strip()):
        raise DatabaseConfigurationError("database.namespace must be a non-empty string")

    url = os.getenv("HERMES_DATABASE_URL", "").strip()
    if not url:
        raise DatabaseConfigurationError(
            "database.backend is 'postgres' but HERMES_DATABASE_URL is not set"
        )
    scheme = urlsplit(url).scheme.lower()
    if scheme not in {"postgres", "postgresql"}:
        raise DatabaseConfigurationError("HERMES_DATABASE_URL must use the postgres:// or postgresql:// scheme")
    try:
        min_size = int(config.get("pool_min_size", 1))
        max_size = int(config.get("pool_max_size", 8))
        connect_timeout = float(config.get("connect_timeout", 5.0))
        pool_timeout = float(config.get("pool_timeout", 10.0))
    except (TypeError, ValueError) as exc:
        raise DatabaseConfigurationError("PostgreSQL timeout and pool settings must be numeric") from exc
    if min_size < 0 or max_size < 1 or min_size > max_size:
        raise DatabaseConfigurationError("database.pool_min_size must be >= 0 and <= pool_max_size")
    if not all(math.isfinite(v) and v > 0 for v in (connect_timeout, pool_timeout)):
        raise DatabaseConfigurationError("PostgreSQL timeouts must be greater than zero")
    return DatabaseSettings(
        backend=backend, url=url, connect_timeout=connect_timeout, pool_timeout=pool_timeout,
        pool_min_size=min_size, pool_max_size=max_size, namespace=namespace,
    )


def configured_database_backend(home: Path | None = None) -> str:
    return load_database_settings(home).backend


def sanitized_database_url(url: str) -> str:
    """Return a log-safe endpoint; never include password, query parameters, or fragments."""
    parts = urlsplit(url)
    host = parts.hostname or "<unknown>"
    if parts.port:
        host = f"{host}:{parts.port}"
    user = f"{parts.username}@" if parts.username else ""
    return urlunsplit((parts.scheme, user + host, parts.path, "", ""))
