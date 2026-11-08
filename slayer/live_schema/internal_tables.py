"""Recognition of well-known ELT / migration housekeeping tables.

An unfiltered ingest models these as first-class semantic models, so they clog
the model list handed to an AI agent over MCP — burning tokens and inviting
nonsense aggregations/joins. Pure and DB-free so the rule table is unit-testable
without a database.

A prefix rule is admissible only where a vendor reserves the namespace by
contract (``_dlt_``, ``_airbyte_``), so a match cannot be user data; everything
else is an exact name. Rules are dialect-blind — each names something a vendor
writes into any warehouse it targets. Two deliberate omissions worth not
re-litigating: ``_fivetran_`` / ``_sdc_`` as prefixes (their surface is columns
on real tables, not tables), and ``sqlite_`` (SQLAlchemy already filters SQLite
internals out, so the rule could only fire on a non-SQLite DB and hide user data).
"""

# (prefix, tool). Lower-case; the lookup lower-cases its input once.
_INTERNAL_PREFIXES: tuple[tuple[str, str], ...] = (
    ("_dlt_", "dlt"),
    ("_airbyte_", "airbyte"),
)

# Exact table name → owning tool. Lower-case keys because vendors vary the case
# (``__EFMigrationsHistory``, ``SequelizeMeta``); the lookup is case-insensitive.
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

    Match on the live object name, not a derived model name: ``__``-sanitization
    turns ``_dlt_loads__x`` into the model ``_dlt_loads_x``, a string the
    database never had. Exact names are checked before prefixes (the sets don't
    overlap, so it's for determinism, not precedence).
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
