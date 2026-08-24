"""Schema identity and ingest-scope resolution (DEV-1758).

``SchemaRef`` is the single owner of a schema's identity — its catalog, its
name, whether it was explicitly requested, and whether it is the connection
default. Every surface that qualifies an ingested table (``sql_table``), probes
columns / PKs / comments, or lists ingestable objects goes through a
``SchemaRef`` rather than passing a bare schema string around, so the
catalog-qualified token that keeps DuckDB's ``Inspector`` from sweeping every
attached catalog is carried end to end.

The module is a leaf: it depends only on SQLAlchemy. The behavioural contract
(qualify matrix, dialect-gated catalog split, system-schema filter, scope
conflict validation) is pinned by ``tests/test_schema_scope.py``; resolution
precedence / dedup / current-catalog preference is pinned end-to-end on DuckDB
by ``tests/test_ingestion_schema_qualification.py``.
"""

from __future__ import annotations

import logging
from typing import Optional

import sqlalchemy as sa
from pydantic import BaseModel

logger = logging.getLogger(__name__)


# Dialects whose ``get_schema_names()`` emits catalog-qualified
# ``catalog.schema`` tokens (DuckDB lists every attached catalog's schemas), so
# a dotted token there must be split into ``(catalog, name)``. Everywhere else a
# dot inside a schema name is part of the name (Postgres allows
# ``CREATE SCHEMA "foo.bar"``) and must not be read as a catalog.
_CATALOG_QUALIFYING_DIALECTS = frozenset({"duckdb"})

# System / bookkeeping schemas never ingested. Matched on the LAST dotted
# segment (``system.information_schema`` → ``information_schema``) …
_SYSTEM_LAST_SEGMENTS = frozenset(
    {
        "information_schema",
        "pg_catalog",
        "pg_toast",
        "performance_schema",
        "mysql",
        "sys",
        "sys_temp",
    }
)
# … or the FIRST segment for DuckDB's own catalogs (``system.*`` / ``temp.*``).
_SYSTEM_FIRST_SEGMENTS = frozenset({"system", "temp"})


def _dialect_qualifies_tokens(dialect_name: Optional[str]) -> bool:
    return (dialect_name or "").lower() in _CATALOG_QUALIFYING_DIALECTS


class SchemaRef(BaseModel, frozen=True):
    """One schema's identity — the single owner of schema qualification.

    ``catalog`` / ``name`` locate the schema (``catalog`` is populated only on
    catalog-qualifying dialects). ``requested`` holds the exact spelling a user
    asked for, when they asked; ``is_default`` marks the connection default.
    Frozen so a resolved ref is hashable and can be de-duplicated by identity.
    """

    catalog: Optional[str] = None
    name: Optional[str] = None
    requested: Optional[str] = None
    is_default: bool = False

    @property
    def explicit(self) -> bool:
        """True when a user named this schema (so it is emitted verbatim)."""
        return self.requested is not None

    @property
    def token(self) -> Optional[str]:
        """The catalog-qualified token to hand ``Inspector`` methods."""
        if self.catalog and self.name:
            return f"{self.catalog}.{self.name}"
        return self.name

    def qualify(self, obj_name: str) -> str:
        """Prefix ``obj_name`` for a persisted ``sql_table``.

        An explicit request is emitted exactly as typed. An auto-resolved
        default (or a dialect with no schema concept) stays bare; an
        auto-resolved non-default emits its bare schema name (never the
        catalog — the datasource already points at one catalog).
        """
        if self.explicit:
            return f"{self.requested}.{obj_name}"
        if self.is_default or not self.name:
            return obj_name
        return f"{self.name}.{obj_name}"


class SkippedSchema(BaseModel):
    """A visible schema left out of scope, with an actionable reason."""

    token: str
    reason: str


class IngestScope(BaseModel):
    """The resolved set of schemas to ingest, plus hints and skips."""

    schemas: list[SchemaRef]
    #: Own-catalog schemas present but NOT in scope — the source of the
    #: "new schema available" hint. Populated only when exactly one schema is
    #: in scope and ``all_schemas`` is off.
    other_schemas: list[str] = []
    #: Foreign-catalog / system schemas explicitly dropped, never silently.
    skipped: list[SkippedSchema] = []


def split_sql_table(sql_table: str) -> tuple[Optional[str], str]:
    """Split a persisted ``sql_table`` into ``(schema_token, object)``.

    Splits on the FINAL dot so a 3-part ``catalog.schema.table`` keeps its
    catalog with the schema token (``"c.s.t" -> ("c.s", "t")``); a quoted
    object identifier is preserved. Returns ``(None, name)`` when unqualified.
    """
    schema_token, sep, obj = sql_table.rpartition(".")
    if not sep:
        return None, sql_table
    return (schema_token or None), obj


def schema_ref_from_token(
    token: str,
    *,
    dialect_name: Optional[str],
    requested: Optional[str] = None,
    is_default: bool = False,
) -> SchemaRef:
    """Build a ``SchemaRef`` from an enumerated schema token.

    The catalog is peeled off only on catalog-qualifying dialects (DuckDB);
    elsewhere the whole token is the schema name.
    """
    catalog: Optional[str] = None
    name = token
    if _dialect_qualifies_tokens(dialect_name) and "." in token:
        catalog, _, name = token.partition(".")
        catalog = catalog or None
    return SchemaRef(
        catalog=catalog, name=name, requested=requested, is_default=is_default
    )


def is_system_schema(token: str) -> bool:
    """True for catalog/bookkeeping schemas that are never ingested."""
    if not token:
        return False
    parts = token.split(".")
    first = parts[0].lower()
    last = parts[-1].lower()
    if first in _SYSTEM_FIRST_SEGMENTS:
        return True
    if last in _SYSTEM_LAST_SEGMENTS:
        return True
    return last.startswith("pg_temp_") or last.startswith("pg_toast_temp_")


def validate_scope_args(
    *,
    schema: Optional[str] = None,
    schemas: Optional[list[str]] = None,
    all_schemas: bool = False,
) -> None:
    """Reject conflicting scope requests at every entry point.

    ``all_schemas`` is exclusive with any named schema, and the legacy single
    ``schema`` is exclusive with the plural ``schemas``.
    """
    if all_schemas and (schema is not None or schemas is not None):
        raise ValueError(
            "all_schemas cannot be combined with an explicit schema / schemas "
            "request — choose one."
        )
    if schema is not None and schemas is not None:
        raise ValueError(
            "Pass either a single schema or a list of schemas, not both."
        )


# ---------------------------------------------------------------------------
# Live connection helpers
# ---------------------------------------------------------------------------


def _current_catalog(sa_engine: sa.Engine) -> Optional[str]:
    """The connection's current catalog (``current_database()``), or None.

    Only a real string counts — a mock engine's non-str attribute or row value
    is treated as "no catalog" rather than propagated into a ``SchemaRef``.
    """
    try:
        with sa_engine.connect() as conn:
            row = conn.execute(sa.text("SELECT current_database()")).fetchone()
        value = row[0] if row else None
        return value if isinstance(value, str) else None
    except Exception:  # noqa: BLE001 — best-effort; absence just means "no catalog"
        return None


def _default_schema_name(
    inspector: sa.engine.Inspector, sa_engine: Optional[sa.Engine]
) -> Optional[str]:
    """The connection's default schema name (bare, e.g. ``main`` / ``public``).

    Only a real string counts (a mock inspector's attribute is ignored)."""
    try:
        name = inspector.default_schema_name
    except Exception:  # noqa: BLE001
        name = None
    if isinstance(name, str) and name:
        return name
    if sa_engine is not None:
        try:
            with sa_engine.connect() as conn:
                row = conn.execute(sa.text("SELECT current_schema()")).fetchone()
            value = row[0] if row else None
            if isinstance(value, str) and value:
                return value
        except Exception:  # noqa: BLE001 — dialect may lack current_schema()
            pass
    return name if isinstance(name, str) else None


def _dialect_name(engine: Optional[sa.Engine]) -> Optional[str]:
    return getattr(getattr(engine, "dialect", None), "name", None)


def default_schema_ref(
    inspector: sa.engine.Inspector, sa_engine: Optional[sa.Engine] = None
) -> SchemaRef:
    """The catalog-qualified default-schema ref for an inspector.

    Used to resolve a ``ref=None`` discovery / fallback request to the
    connection default BEFORE listing, so a bare lookup never sweeps an
    attached catalog.
    """
    engine = sa_engine if sa_engine is not None else getattr(inspector, "bind", None)
    dialect = _dialect_name(engine)
    catalog = (
        _current_catalog(engine)
        if (engine is not None and _dialect_qualifies_tokens(dialect))
        else None
    )
    return SchemaRef(
        catalog=catalog,
        name=_default_schema_name(inspector, engine),
        is_default=True,
    )


def default_schema_ref_for_engine(sa_engine: sa.Engine) -> SchemaRef:
    """``default_schema_ref`` for callers that hold only an engine."""
    return default_schema_ref(sa.inspect(sa_engine), sa_engine)


# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------


def _dedup_requested(requested: Optional[list[str]]) -> Optional[list[str]]:
    """Dedup a requested list, preserving first-seen order."""
    if not requested:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for name in requested:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _dedup_refs(refs: list[SchemaRef]) -> list[SchemaRef]:
    """Collapse refs that resolve to the same token (the double-scan bug)."""
    seen: set[Optional[str]] = set()
    out: list[SchemaRef] = []
    for ref in refs:
        if ref.token in seen:
            continue
        seen.add(ref.token)
        out.append(ref)
    return out


def _hint_schemas(
    own_refs: list[SchemaRef], in_scope: list[SchemaRef]
) -> list[str]:
    """Own-catalog schema names present but not covered by ``in_scope``."""
    covered = {r.name for r in in_scope}
    out: list[str] = []
    for ref in own_refs:
        if ref.name and ref.name not in covered and ref.name not in out:
            out.append(ref.name)
    return out


def resolve_ingest_scope(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    requested: Optional[list[str]],
    all_schemas: bool,
    datasource_schema: Optional[str],
) -> IngestScope:
    """Resolve which schemas to ingest, as ``SchemaRef``s.

    Precedence: ``all_schemas`` → every own-catalog non-system schema;
    a ``requested`` list → those schemas (a single name is emitted verbatim,
    several are resolved against the enumerated set with the default staying
    bare); else a persisted ``datasource_schema`` (verbatim); else the bare
    connection default. Foreign attached catalogs and system schemas are
    reported in ``skipped``. When exactly one schema is in scope (and
    ``all_schemas`` is off) the remaining own schemas are offered as
    ``other_schemas`` hints.
    """
    dialect = _dialect_name(sa_engine)
    qualifies = _dialect_qualifies_tokens(dialect)
    current_catalog = _current_catalog(sa_engine) if qualifies else None
    default_schema = _default_schema_name(inspector, sa_engine)

    own_refs: list[SchemaRef] = []
    skipped: list[SkippedSchema] = []
    try:
        tokens = list(inspector.get_schema_names() or [])
    except Exception:  # noqa: BLE001 — no schema listing → default-only scope
        tokens = []
    for tok in tokens:
        if is_system_schema(tok):
            continue
        ref = schema_ref_from_token(tok, dialect_name=dialect)
        if (
            qualifies
            and ref.catalog
            and current_catalog
            and ref.catalog != current_catalog
        ):
            skipped.append(
                SkippedSchema(
                    token=tok,
                    reason=(
                        f"schema belongs to attached catalog {ref.catalog!r}; "
                        f"point a separate datasource at {ref.catalog!r} to "
                        f"ingest it"
                    ),
                )
            )
            continue
        own_refs.append(ref)

    own_by_name: dict[Optional[str], SchemaRef] = {}
    for ref in own_refs:
        own_by_name.setdefault(ref.name, ref)

    def _resolve_name(name: str, *, requested_spelling: Optional[str]) -> SchemaRef:
        is_def = name == default_schema
        base = own_by_name.get(name)
        if base is not None:
            return base.model_copy(
                update={"requested": requested_spelling, "is_default": is_def}
            )
        ref = schema_ref_from_token(
            name,
            dialect_name=dialect,
            requested=requested_spelling,
            is_default=is_def,
        )
        if qualifies and ref.catalog is None and current_catalog is not None:
            ref = ref.model_copy(update={"catalog": current_catalog})
        return ref

    def _default_ref() -> SchemaRef:
        base = own_by_name.get(default_schema)
        if base is not None:
            return base.model_copy(update={"is_default": True})
        return SchemaRef(
            catalog=current_catalog, name=default_schema, is_default=True
        )

    req = _dedup_requested(requested)

    if all_schemas:
        schemas = [
            ref.model_copy(
                update={"is_default": ref.name == default_schema, "requested": None}
            )
            for ref in own_refs
        ]
        return IngestScope(schemas=schemas, other_schemas=[], skipped=skipped)

    if req:
        if len(req) == 1:
            schemas = [_resolve_name(req[0], requested_spelling=req[0])]
        else:
            schemas = _dedup_refs(
                [_resolve_name(name, requested_spelling=None) for name in req]
            )
    elif datasource_schema:
        schemas = [
            _resolve_name(datasource_schema, requested_spelling=datasource_schema)
        ]
    else:
        schemas = [_default_ref()]

    other = _hint_schemas(own_refs, schemas) if len(schemas) == 1 else []
    return IngestScope(schemas=schemas, other_schemas=other, skipped=skipped)
