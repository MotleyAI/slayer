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


class SchemaEnumerationError(Exception):
    """``get_schema_names()`` failed, so the schema list is unknown. Raised only
    for ``all_schemas`` — an empty scope there would masquerade as an empty
    datasource; the default / requested branches still work from a bare default.
    """


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
    return last.startswith(("pg_temp_", "pg_toast_temp_"))


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


def _enumerate_own_schemas(
    *,
    inspector: sa.engine.Inspector,
    dialect: Optional[str],
    qualifies: bool,
    current_catalog: Optional[str],
) -> tuple[list[SchemaRef], list[SkippedSchema], bool]:
    """Split the visible schemas into own-catalog non-system refs and skips.

    A schema in an attached catalog other than the connection's own is dropped
    to ``skipped``, never silently ingested. Third value ``enum_ok`` is False
    when ``get_schema_names()`` failed, so ``all_schemas`` can raise rather than
    return a silent empty scope.
    """
    own_refs: list[SchemaRef] = []
    skipped: list[SkippedSchema] = []
    enum_ok = True
    try:
        tokens = list(inspector.get_schema_names() or [])
    except Exception:  # noqa: BLE001 — no schema listing → default-only scope
        tokens = []
        enum_ok = False
    for tok in tokens:
        if is_system_schema(tok):
            continue
        ref = schema_ref_from_token(tok, dialect_name=dialect)
        is_foreign = (
            qualifies
            and ref.catalog
            and current_catalog
            and ref.catalog != current_catalog
        )
        if is_foreign:
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
    return own_refs, skipped, enum_ok


def _resolve_requested_name(
    name: str,
    *,
    own_by_name: dict[Optional[str], SchemaRef],
    default_schema: Optional[str],
    dialect: Optional[str],
    qualifies: bool,
    current_catalog: Optional[str],
    requested_spelling: Optional[str],
) -> SchemaRef:
    """Resolve one requested schema name to a ref, preferring the enumerated
    entry (so its catalog is the connection's own) and stamping default/spelling.
    """
    is_def = name == default_schema
    base = own_by_name.get(name)
    if base is not None:
        return base.model_copy(
            update={"requested": requested_spelling, "is_default": is_def}
        )
    ref = schema_ref_from_token(
        name, dialect_name=dialect, requested=requested_spelling, is_default=is_def
    )
    if qualifies and ref.catalog is None and current_catalog is not None:
        ref = ref.model_copy(update={"catalog": current_catalog})
    return ref


def _drop_out_of_scope_refs(
    schemas: list[SchemaRef],
    *,
    qualifies: bool,
    current_catalog: Optional[str],
    skipped: list[SkippedSchema],
) -> list[SchemaRef]:
    """Drop refs that must never be ingested into ``skipped`` (in place),
    returning the kept refs. Applied AFTER an explicit / persisted name is
    resolved (enumeration's filter only sees the discovered set): a foreign
    attached catalog, or a system schema (``--schema information_schema``).
    """
    kept: list[SchemaRef] = []
    for ref in schemas:
        is_foreign = (
            qualifies
            and ref.catalog
            and current_catalog
            and ref.catalog != current_catalog
        )
        if is_foreign:
            skipped.append(
                SkippedSchema(
                    token=ref.token or "",
                    reason=(
                        f"requested schema belongs to attached catalog "
                        f"{ref.catalog!r}; point a separate datasource at "
                        f"{ref.catalog!r} to ingest it"
                    ),
                )
            )
            continue
        if is_system_schema(ref.token or ref.name or ""):
            skipped.append(
                SkippedSchema(
                    token=ref.token or ref.name or "",
                    reason="system / bookkeeping schema is never ingested",
                )
            )
            continue
        kept.append(ref)
    return kept


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
    connection default. Foreign attached catalogs and system schemas — whether
    discovered, requested, or persisted — are reported in ``skipped``. When
    exactly one schema is in scope (and ``all_schemas`` is off) the remaining
    own schemas are offered as ``other_schemas`` hints.

    Raises :class:`SchemaEnumerationError` for ``all_schemas`` when the schema
    list can't be read (the other branches resolve from a bare default instead).
    """
    dialect = _dialect_name(sa_engine)
    qualifies = _dialect_qualifies_tokens(dialect)
    current_catalog = _current_catalog(sa_engine) if qualifies else None
    default_schema = _default_schema_name(inspector, sa_engine)

    own_refs, skipped, enum_ok = _enumerate_own_schemas(
        inspector=inspector, dialect=dialect, qualifies=qualifies,
        current_catalog=current_catalog,
    )
    own_by_name: dict[Optional[str], SchemaRef] = {}
    for ref in own_refs:
        own_by_name.setdefault(ref.name, ref)

    if all_schemas:
        if not enum_ok:
            raise SchemaEnumerationError(
                "--all-schemas requested but the datasource's schema list could "
                "not be read (revoked metadata permission or a transient error); "
                "refusing to report an empty scope, which would look like an "
                "empty datasource."
            )
        schemas = [
            ref.model_copy(
                update={"is_default": ref.name == default_schema, "requested": None}
            )
            for ref in own_refs
        ]
        return IngestScope(schemas=schemas, other_schemas=[], skipped=skipped)

    def _resolve(name: str, requested_spelling: Optional[str]) -> SchemaRef:
        return _resolve_requested_name(
            name, own_by_name=own_by_name, default_schema=default_schema,
            dialect=dialect, qualifies=qualifies, current_catalog=current_catalog,
            requested_spelling=requested_spelling,
        )

    req = _dedup_requested(requested)
    if req and len(req) == 1:
        schemas = [_resolve(req[0], req[0])]
    elif req:
        schemas = _dedup_refs([_resolve(name, None) for name in req])
    elif datasource_schema:
        schemas = [_resolve(datasource_schema, datasource_schema)]
    else:
        base = own_by_name.get(default_schema)
        schemas = [
            base.model_copy(update={"is_default": True})
            if base is not None
            else SchemaRef(catalog=current_catalog, name=default_schema,
                           is_default=True)
        ]

    # Defence-in-depth (Codex review): a resolved request that names a foreign
    # attached catalog OR a system/bookkeeping schema is dropped to ``skipped``,
    # never ingested — enumeration's filter only covers the discovered set.
    schemas = _drop_out_of_scope_refs(
        schemas, qualifies=qualifies, current_catalog=current_catalog,
        skipped=skipped,
    )

    other = _hint_schemas(own_refs, schemas) if len(schemas) == 1 else []
    return IngestScope(schemas=schemas, other_schemas=other, skipped=skipped)
