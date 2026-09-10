# PostgreSQL state backend

Hermes uses SQLite by default. PostgreSQL backs the core durable SQL stores
listed below when local transactional files are unsuitable, including Azure
Container Apps deployments. Optional plugin databases have separate limits.

## Configuration

Install the driver and select the backend in `~/.hermes/config.yaml`:

```bash
uv pip install -e '.[postgres]'
```

```yaml
database:
  backend: postgres
  connect_timeout: 5
  pool_timeout: 10
  pool_min_size: 1
  pool_max_size: 8
```

Supply credentials only through the secret environment variable:

```bash
export HERMES_DATABASE_URL='postgresql://hermes:password@localhost:5432/hermes?sslmode=require'
```

Azure PostgreSQL TLS query parameters are supported. Logs contain only a
sanitized endpoint, never the password, query, or fragment. Invalid or missing
configuration fails closed; PostgreSQL mode never falls back to SQLite or
process memory for durable stores.

## Storage and isolation

Historical SQLite paths are retained as logical store identities only. They
map deterministically to identifier-safe PostgreSQL schemas, so every profile
and Kanban board stays isolated without creating a local `*.db`, WAL, SHM, or
journal file.

For a fresh deployment, set a unique, stable `database.namespace` in each
profile's config, for example `namespace: hermes-staging-main`. Schemas then
derive from that namespace plus the store's path relative to the profile home,
so moving the home does not change database identity. Different profiles must
use different namespaces. Reusing a namespace deliberately shares the same
stores; it is not an authorization boundary. Without this setting the legacy
absolute-path mapping is preserved. Do not add/change the namespace on an
existing deployment without a deliberate data migration: it selects different
schemas and does not rename or copy existing data.

| Logical store | Data | PostgreSQL status |
|---|---|---|
| `state.db` | Sessions, messages, usage, routing, heartbeats, leases, handoffs, Telegram topics | PostgreSQL-backed |
| direct `state.db` tables | Delivery obligations, hosted rooms, policy, links, replicas, async delegation | PostgreSQL-backed |
| `response_store.db` | Responses API continuation | PostgreSQL-backed |
| `runs_idempotency.db` | API run idempotency | PostgreSQL-backed |
| `cron/executions.db` | Executions and incidents | PostgreSQL-backed |
| `cron/deliveries.db` | Durable delivery queue | PostgreSQL-backed |
| `cron/notepad.db` | Per-job notepad | PostgreSQL-backed |
| `kanban.db` | Tasks, boards, dispatch, workers | PostgreSQL-backed, schema per board |
| `projects.db` | Project catalog | PostgreSQL-backed |
| `gateway/discord_message_recovery.db` | Discord reconnect replay/cursors | PostgreSQL-backed |
| `verification_evidence.db` | Bounded verification/stop-guard evidence | PostgreSQL-backed |

PostgreSQL schema startup is serialized with an advisory transaction lock.
Canonical migrations are versioned in `hermes_schema_migrations`; auxiliary
stores retain their idempotent first-open migrations. Message search uses a
generated `tsvector`, GIN index, `websearch_to_tsquery`, and `ts_headline`.
PostgreSQL search uses the built-in `simple` text-search configuration, so
ranking/tokenization can differ slightly from SQLite FTS5 while preserving the
same search surface.

## Import existing SQLite data

The importer is explicit, transactional per store, preserves all source files,
checks source integrity, resets PostgreSQL sequences, and emits a credential-free
report. A successful import is recorded transactionally; repeated imports of
the same source are refused (including tables without primary keys):

```bash
python scripts/migrate_sqlite_to_postgres.py --home "$HERMES_HOME" --dry-run
python scripts/migrate_sqlite_to_postgres.py --home "$HERMES_HOME" \
  --report migration-report.json
```

Conflicting primary keys are retained in PostgreSQL and counted, never
overwritten. Keep the SQLite sources until the report and live behavior have
been verified. Run the importer once for the root home and once for each named
profile home; schema identity deliberately follows each profile's historical
path rather than merging profiles.

## Backup and restore

`hermes backup` covers filesystem state and reports that PostgreSQL is separate.
Use the native verified dump workflow for all Hermes schemas:

```bash
python scripts/postgres_backup.py --home "$HERMES_HOME" backup hermes.dump
python scripts/postgres_backup.py --home "$HERMES_HOME" restore hermes.dump \
  --confirm-empty-target
```

Restore refuses a target already containing Hermes schemas. It never runs an
implicit destructive `--clean` operation. The script requires compatible
`pg_dump` and `pg_restore` client binaries in the operator environment.
Restore runs in a single transaction. Large multi-profile archives may require
increasing PostgreSQL `max_locks_per_transaction`; failed restores roll back.
Existing backup paths are refused rather than overwritten.

SQLite repair, WAL, byte-probe, and FTS5 maintenance paths are skipped in
PostgreSQL mode. `hermes doctor` and detailed readiness perform bounded
PostgreSQL health queries. Session change watchers use a database revision
counter instead of file mtimes.

## Local integration tests

```bash
docker compose -f docker-compose.postgres.yml up -d --wait
export HERMES_TEST_POSTGRES=1
scripts/run_tests.sh tests/test_state_backend.py \
  tests/test_state_postgres_integration.py \
  tests/test_postgres_full_state_integration.py \
  tests/test_postgres_tools.py
docker compose -f docker-compose.postgres.yml down
```

## Files, caches, and plugin state

Files such as workspaces, file-based memories, skills, uploads, artifacts,
logs, `SOUL.md`, pairing records, and cron job definitions remain
filesystem-backed; PostgreSQL is not an object store.

SQLite that remains is outside the core durable-state contract:

- observability `metrics.sqlite3` is a rebuildable local metrics cache;
- browser profile/history databases belong to the browser and are copied only
  for browser setup;
- `lost_and_found.db` and recovery scratch databases are explicit offline
  repair artifacts;
- generic plugin storage, Holographic Memory's `memory_store.db`, RetainDB's
  local queue, and Matrix SDK crypto storage remain owned by those optional
  plugins/SDKs.

Do not place an optional plugin's live SQLite database on Azure Files merely
because core Hermes uses PostgreSQL. A plugin that needs durable transactional
state on ACA must provide its own network-safe backend or be kept disabled.

## Known deployment limitations

- Keep ACA at one active replica initially. PostgreSQL removes SQLite writer
  contention, but gateway ownership, filesystem workspaces, process lifecycle,
  and several plugin transports are not a horizontally-scalable control plane.
- Logical schema names include a hash of the historical absolute database
  path unless `database.namespace` is set. Without an explicit namespace, keep
  `HERMES_HOME` stable across revisions.
- Each named profile has an independent `config.yaml` and schema. Configure
  PostgreSQL for every profile that must use it, and import every profile
  separately.
- Azure Files remains appropriate for ordinary files, not any live SQLite
  database selected by a plugin or legacy workflow.

## Implementation and validation report (2026-09-10)

### Architecture and important changes

`SessionDB` remains the public boundary. `hermes_state_backend.py` selects and
validates configuration, `hermes_state_postgres.py` supplies pooled canonical
state operations, and `hermes_db.py` centralizes auxiliary connection and SQL
dialect adaptation. SQLite stays on its existing implementation and does not
require the optional PostgreSQL driver.

Canonical tables cover sessions/messages, model usage, prompts, state metadata,
routing, heartbeats, turn/compression ownership, handoffs and Telegram topics.
Primary/foreign keys, PostgreSQL sequences and native full-text indexes enforce
the new backend's storage contract. The database revision counter drives UI
change detection without a physical state file. Native autovacuum replaces
SQLite VACUUM; retention still runs with a PostgreSQL maintenance lock.

Important changed-file groups:

| Files | Purpose |
|---|---|
| `hermes_state.py`, `hermes_state_registry.py`, `hermes_state_telegram.py` | Backend selection, profile-scoped shared handles, Telegram schema integration |
| `hermes_db.py`, `hermes_state_backend.py`, `hermes_state_postgres.py` | PostgreSQL driver, SQL adaptation, schema, transactions, search and maintenance |
| `gateway/delivery_ledger.py`, `hosted_room*.py`, `lifecycle_ledger.py` | Durable operational and hosted-room state |
| `gateway/platforms/api_server*.py` | Response continuation and run idempotency without memory fallback |
| `cron/ledger.py`, `cron/delivery_queue.py` | Durable execution/notepad/delivery stores |
| `hermes_cli/kanban_*.py`, `projects_db.py` | Board/catalog persistence, dispatcher exclusion and portable archive transfer |
| `agent/insights.py`, `agent/verification_evidence.py`, `tools/async_delegation.py` | Analytics, verification and delegation persistence |
| `web_server*.py`, `web_routers/*.py`, `tui_gateway/*.py`, `mcp_serve.py` | Session listing, analytics and database-backed change detection |
| `gateway/readiness.py`, `doctor_state.py`, `sessions_cmd.py`, `backup.py` | Backend-aware health, diagnostics and backup notices |
| `tools/session_search_tool.py`, `bot_live_delivery.py`, plugin adapters/recovery | Remove physical state-file preconditions on migrated paths |
| `scripts/migrate_sqlite_to_postgres.py`, `scripts/postgres_backup.py` | Explicit source-preserving import and native backup/restore |
| Configuration examples, `pyproject.toml`, `uv.lock`, compose file, test runner | Optional dependency/configuration and local-only test setup |

The shared PostgreSQL write boundary uses a schema-scoped transaction advisory
lock to preserve existing check-then-write contracts. Auxiliary `BEGIN IMMEDIATE`
uses the same database lock namespace. Reads remain concurrent. Kanban dispatch
uses a separate non-blocking session lock; delegated descendants retain read-only
connections. Lock waits are bounded by the configured pool timeout.

### Validation performed

Final focused run: **67 passed, 0 failed**, using PostgreSQL 17, psycopg 3.3.4
and psycopg-pool 3.3.0. Reproduce with the local compose database running:

```bash
HERMES_TEST_POSTGRES=1 scripts/run_tests.sh \
  tests/test_state_backend.py tests/test_postgres_tools.py \
  tests/test_state_postgres_integration.py \
  tests/test_postgres_full_state_integration.py \
  tests/hermes_cli/test_kanban_transfer.py \
  tests/hermes_state/test_shared_session_db_registry.py \
  tests/hermes_cli/test_approvals_suggest.py -j 2
```

Coverage includes session CRUD/continuation, metadata, Unicode, large content,
search, routing, rollback, restart, shared-pool concurrent reads/writes, separate
auxiliary connections performing serialized read-modify-write, dispatcher lock
exclusion/release, retention, profile isolation, durable auxiliary stores,
source-preserving import with repeat refusal, and SQLite archive/registry
compatibility. Targeted lint and `git diff --check` passed.

Earlier regression slices also exercised gateway ledgers, platform base,
projects, journal configuration, insights and verification. They are not summed
here because the slices overlap. Two broader checks remained unsuccessful in
this host environment: a corruption-holder probe encountered macOS `sysctl`
permissions, and a verification/file-tool test hit a lint timeout followed by
the live-process safety guard. Those failures were not waived by weakening the
guard, and were not proven to be baseline failures. The entire repository suite
has **not** been certified green.

A native `pg_dump`/single-transaction `pg_restore` round trip succeeded for one
complete disposable session schema, with matching session/message row counts.
An all-test-schema restore exceeded the default PostgreSQL lock capacity and
rolled back. Native tools ran inside Docker; the Python backup CLI has unit
coverage, not a complete host-client end-to-end test in this environment.

### Remaining qualification work and risks

- Run the full test suite in the supported CI/Linux environment and resolve
  the two broader failures above before treating this as production-certified.
- Exercise a representative real SQLite backup in a disposable target: the
  importer has integration coverage for a small canonical and auxiliary source,
  not every historical schema/data combination. Stop writers for cutover; each
  source has a consistent read transaction, but separate stores are not one
  cross-store snapshot. Reports count conflicts rather than proving conflicting
  rows contain identical data.
- The SQL adapter supports the subset used by migrated stores, not arbitrary
  SQLite SQL. Future upstream SQL/schema changes need PostgreSQL regression
  coverage. Auxiliary schema upgrades reuse existing first-open migrations;
  they are not an independent general migration framework.
- Canonical pools are bounded per instance; auxiliary stores use explicitly
  closed direct connections. Total connection demand still grows with active
  workers/profiles and must be capacity-tested.
- PostgreSQL search is not FTS5/trigram/CJK-tokenizer parity. Validate the query
  languages and Unicode search cases important to your users.
- Optional plugin databases listed above still require their own network-safe
  persistence or must remain disabled. This is not a migration of all possible
  third-party/plugin storage.

### Future ACA implications (no deployment performed)

Build the container with the `postgres` extra, configure `database.backend` in
each profile, inject `HERMES_DATABASE_URL` as a secret with appropriate TLS
settings, and provide outbound access to the external PostgreSQL server. The
database role needs schema/table/index/function creation privileges for startup
migrations. Keep the logical home path stable and retain ordinary files on
appropriate persistent storage. Start with one replica; process ownership,
cron file coordination and plugin transports still constrain multi-replica use.
Plan an operator-native backup/restore test and a deliberate data cutover.
No cloud resources or production state were changed during this work.

## Pre-deployment review follow-up

The image now includes `--extra postgres` in its existing frozen `uv sync`
step; use the repository Dockerfile rather than a wheel-based `pip install .`.
The schema startup checks the recorded version before applying DDL, skips
reapplying current migrations and rejects schemas newer than the build.
SQLite integrity/schema-text checks no longer return fabricated success on
PostgreSQL. Updater checks skip legacy SQLite files in PostgreSQL mode, and
the SQL file-probe helper recognizes network stores. TUI change watchers close
their database handles after each poll, including mixed-backend profile polling.

Follow-up validation: **59 passed, 0 failed** across the four PostgreSQL-focused
files plus SQLite Kanban transfer and shared-session registry regressions.
This includes real PostgreSQL persistence after home relocation with a stable
namespace, explicit unsupported-integrity errors and untouched legacy SQLite
files during updater checks. Targeted lint and whitespace checks passed.

**Migration/runtime role separation is not implemented yet.** Auxiliary stores
still run their existing schema initialization on open, so an application role
restricted to DML is not supported. Do not revoke DDL privileges and expect the
current image to work. A separate migration command must initialize/version all
auxiliary stores, and each runtime path must validate without executing DDL;
that requires its own restricted-role integration test before production use.
The current setup remains appropriate for isolated staging qualification with
the documented schema-management privileges, not least-privilege production.
