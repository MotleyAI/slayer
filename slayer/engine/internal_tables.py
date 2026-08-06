"""Recognition of well-known ELT / migration housekeeping tables (DEV-1759).

An unfiltered ingest models these as first-class semantic models, and the model
list is the menu handed to an AI agent over MCP — so junk entries burn tokens in
every session, invite an agent to aggregate `_dlt_loads` or join `_dlt_version`,
and (for the state/raw tables) dump kilobytes of serialized JSON into context on
a single exploratory query.

Pure and DB-free on purpose: no engine or storage imports, so the rule table can
be unit-tested without a database and reasoned about without tracing a scan.

Two rule shapes. A PREFIX rule is only admissible where the namespace is
reserved by contract, so that a match provably is not user data:

* ``_dlt_``     — dlt namespaces every object it owns this way.
* ``_airbyte_`` — likewise; this is also what covers ``_airbyte_raw_*``, whose
  stream suffix is arbitrary and so cannot be enumerated.
* ``sqlite_``   — SQLite rejects ``CREATE TABLE sqlite_foo`` outright, so the
  prefix cannot belong to anyone but the engine.

Everything else is an exact name. Notably absent, each for a reason:

* ``_fivetran_`` / ``_sdc_`` as PREFIXES. Both vendors' real surface is
  *columns* on real tables (``_fivetran_synced``, ``_sdc_batched_at``, …), not
  tables, so a table-level prefix rule would match nothing and merely imply a
  coverage we do not have. Fivetran's only destination-schema tables are the two
  audit ones, listed exactly.
* PostGIS (``spatial_ref_sys``, ``geometry_columns``, ``geography_columns``,
  ``raster_columns``) and ``pg_stat_statements``. These land in ``public`` and
  are bookkeeping, but the namespaces are not reserved and a geospatial user may
  legitimately want ``spatial_ref_sys``. Holding the line at engine-reserved
  namespaces keeps "which extensions count?" from becoming an open question with
  no principled stopping point.
* ``log`` / ``audit_trail`` / ``changes`` (Fivetran platform, sqitch) — far too
  generic to match on a name alone.

Two accepted risks, recorded so a future reader does not think they were missed:
the ``sqlite_`` prefix is applied on every dialect, so a Postgres table literally
named ``sqlite_backup`` would be hidden; and ``schema_version`` (legacy Flyway
≤4) is the most collision-prone exact entry. Both are recoverable — matching
hides a model, it never omits one, so the table stays queryable by name and one
``edit_model(hidden=false)`` undoes it.
"""

# (prefix, tool). Lower-case; the lookup lower-cases its input once.
_INTERNAL_PREFIXES: tuple[tuple[str, str], ...] = (
    ("_dlt_", "dlt"),
    ("_airbyte_", "airbyte"),
    ("sqlite_", "sqlite"),
)

# Exact table name → owning tool. Lower-case keys: Liquibase upper-cases its
# tables, EF Core writes ``__EFMigrationsHistory`` and Sequelize
# ``SequelizeMeta``, so a case-sensitive match would silently miss all three.
_INTERNAL_EXACT: dict[str, str] = {
    # ELT
    "_fivetran_audit": "fivetran",
    "_fivetran_audit_warning": "fivetran",
    # Schema migration bookkeeping
    "flyway_schema_history": "flyway",
    "schema_version": "flyway",
    "databasechangelog": "liquibase",
    "databasechangeloglock": "liquibase",
    "alembic_version": "alembic",
    "django_migrations": "django",
    "schema_migrations": "rails",
    "ar_internal_metadata": "rails",
    "sequelizemeta": "sequelize",
    "pgmigrations": "node-pg-migrate",
    "__efmigrationshistory": "entity-framework",
    "__migrationhistory": "entity-framework",
    "knex_migrations": "knex",
    "knex_migrations_lock": "knex",
}


def internal_table_rule(table_name: str) -> str | None:
    """Return the tool that owns ``table_name`` as bookkeeping, else ``None``.

    Match on the LIVE object name, never on a derived model name — ingestion
    sanitizes ``__`` runs out of model names, so ``_dlt_loads__x`` becomes the
    model ``_dlt_loads_x`` and matching the sanitized form would be matching a
    string the database never had.

    Exact names are consulted before prefixes. The two sets do not overlap, so
    the order is for determinism rather than precedence.
    """
    if not table_name:
        return None
    lowered = table_name.lower()
    exact = _INTERNAL_EXACT.get(lowered)
    if exact is not None:
        return exact
    for prefix, tool in _INTERNAL_PREFIXES:
        if lowered.startswith(prefix):
            return tool
    return None
