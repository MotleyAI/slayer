"""Public error types raised by the SLayer core/engine/storage layers.

Kept in ``slayer.core`` so callers can catch them without importing engine or
storage internals. Each class is defined with a stable name and signature; the
message format is decided by the layer that raises it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from slayer.engine.schema_drift import ToDeleteEntry  # noqa: F401


class SlayerError(Exception):
    """Base class for SLayer-specific errors.

    Catching ``SlayerError`` distinguishes our intentional failure modes from
    unexpected ``Exception`` paths (driver errors, IO errors, etc.).
    """


class AmbiguousModelError(SlayerError):
    """Raised when a bare model name resolves to ≥2 datasources and the
    datasource priority list does not pick a unique winner.

    The constructor stores the ambiguous name and the list of candidate
    datasources. The default message is intentionally surface-neutral: it
    states the fact and a generic remediation, but does not reference any
    Python-, REST-, MCP-, or CLI-specific invocation form. Each surface
    appends its own concrete remediation when it catches the error
    (``data_source=...`` query param for REST, the
    ``set_datasource_priority`` MCP tool, the ``slayer datasources
    priority`` CLI subcommand, etc.).
    """

    def __init__(self, name: str, candidates: list[str]) -> None:
        self.name = name
        self.candidates = list(candidates)
        super().__init__(
            f"Model '{name}' exists in multiple datasources: "
            f"{sorted(self.candidates)}. Specify a data_source or set a "
            f"datasource priority to disambiguate."
        )


class EntityResolutionError(SlayerError):
    """Raised when an entity reference cannot be resolved to a canonical
    ``<datasource>.<model>[.<leaf>]`` form (DEV-1357).

    Wraps the spec's resolution-failure cases: unknown segment, ambiguous
    bare column matching multiple models in the priority-winner
    datasource, ``*:count`` invoked outside a query context, and similar.
    Distinct from ``AmbiguousModelError`` (which fires for the model leg
    of bare-name resolution and is reused by the resolver verbatim).
    """


class MemoryNotFoundError(SlayerError):
    """Raised when a memory id does not exist in storage (DEV-1357 /
    DEV-1428).

    Memory ids are non-empty strings (auto-allocated int-shaped, or
    user-supplied like ``"kb.policy.42"``). The unified
    ``forget_memory`` MCP tool / REST endpoint / CLI subcommand surface
    this error when the requested id is unknown.
    """

    def __init__(self, memory_id: str) -> None:
        self.memory_id = str(memory_id)
        # Back-compat alias for callers that still use ``.identifier``.
        self.identifier = self.memory_id
        super().__init__(f"No memory with id '{self.memory_id}'.")


class SchemaDriftError(SlayerError):
    """Raised by ``SlayerQueryEngine.execute()`` when a query fails and the
    failure was attributed to schema drift via ``validate_models``.

    Carries the touched model names, the structured ``to_delete`` payload
    (filtered to those models), and the original DBAPI exception (set as
    ``__cause__`` for tracebacks).
    """

    def __init__(
        self,
        models: list[str],
        to_delete: list[Any],
        original: BaseException,
    ) -> None:
        self.models = list(models)
        self.to_delete = list(to_delete)
        super().__init__(
            f"Schema drift detected on models {sorted(self.models)}. "
            f"Run validate_models to inspect the {len(self.to_delete)} "
            f"pending delete(s)."
        )
        self.__cause__ = original


class ColumnCycleError(SlayerError, ValueError):
    """Raised when a derived ``Column.sql`` chain contains a cycle (DEV-1410).

    Carries the cycle as an ordered list of ``(model_name, column_name)``
    tuples reflecting the recursion order in which the cycle was discovered.

    Multi-inherits ``ValueError`` so existing call sites that catch
    ``ValueError`` (or use ``pytest.raises(ValueError)`` for the legacy
    compile-time cycle raise) continue to work unchanged.
    """

    def __init__(self, cycle: list[tuple[str, str]]) -> None:
        self.cycle: list[tuple[str, str]] = list(cycle)
        chain = " → ".join(f"{m}.{c}" for m, c in self.cycle)
        super().__init__(f"Circular column reference detected: {chain}")


class IdCollisionError(SlayerError, ValueError):
    """Raised when saving an entity whose id differs from an existing id
    only by letter case — such ids collide as filenames in the YAML
    backend on case-insensitive filesystems, so every backend rejects
    them. ``kind`` is ``"model"`` / ``"datasource"`` / ``"memory"``.
    Multi-inherits ``ValueError`` so existing ``except ValueError`` call
    sites continue to work unchanged.
    """

    _LABELS = {
        "model": "Model name",
        "datasource": "Datasource name",
        "memory": "Memory id",
    }

    def __init__(
        self,
        *,
        kind: str,
        new_id: str,
        existing_id: str,
        data_source: str | None = None,
    ) -> None:
        self.kind = kind
        self.new_id = new_id
        self.existing_id = existing_id
        self.data_source = data_source
        label = self._LABELS.get(kind, "Id")
        scope = f" in datasource '{data_source}'" if data_source else ""
        super().__init__(
            f"{label} '{new_id}' conflicts with existing '{existing_id}'"
            f"{scope} (differs only by case). Rename or delete one."
        )


class ForcedFilterError(SlayerError):
    """Raised by the session-policy forced-filter rewrite (DEV-1578).

    Fired when a configured ``ColumnFilterRule`` cannot be safely applied to a
    physical table referenced by a query:

    - the table **confirms it lacks** the rule's column and the rule's
      ``on_unapplicable`` is ``"block"`` (the default), or
    - the column's presence **cannot be confirmed** (introspection error) —
      a fail-closed security control that blocks regardless of
      ``on_unapplicable``, or
    - the rewrite is asked to operate on a non-SELECT statement root.

    Carries the offending ``table``, ``column``, and ``rule_name`` (the
    rule's optional ``name``) for diagnostics; any may be ``None`` for the
    statement-root guard.
    """

    def __init__(
        self,
        message: str,
        *,
        table: str | None = None,
        column: str | None = None,
        rule_name: str | None = None,
    ) -> None:
        self.table = table
        self.column = column
        self.rule_name = rule_name
        super().__init__(message)


class DistinctDimensionValuesError(SlayerError, ValueError):
    """Raised when ``distinct_dimension_values=False`` conflicts with the
    query shape (DEV-1543).

    ``distinct_dimension_values=False`` asks for raw rows — no top-level
    ``GROUP BY``. It is incompatible with any aggregation: a non-empty
    ``measures`` list, a filter / order item referencing a measure
    (colon-form ``col:agg`` / ``*:count``, a transform call like
    ``rank(...)``, or a bare saved ``ModelMeasure`` name), or a query
    with no projected columns at all (both ``dimensions`` and
    ``time_dimensions`` empty).

    Multi-inherits ``ValueError`` so existing ``except ValueError``
    call sites continue to work unchanged.
    """


class UnresolvableOrderColumnError(SlayerError, ValueError):
    """Raised when an ``order`` item references a column that cannot be bound
    to the query's FROM scope (DEV-1645).

    Fires when the sort key is neither a projected output alias, a base-model
    column, nor a column on a join that the query already pulled into scope
    (via a dimension, measure, or filter). The common case is ordering by an
    *unprojected joined column* — e.g. ``order=[{"column":
    "customers.regions.name"}]`` — whose join was never resolved, so there is
    no in-scope table to qualify against. Emitting a reference anyway would
    produce SQL that fails at the database with UndefinedTable/UndefinedColumn;
    rejecting at compile time surfaces an actionable error instead.

    Multi-inherits ``ValueError`` so existing ``except ValueError`` call sites
    continue to work unchanged.
    """

    def __init__(self, *, column: str, qualifier: str) -> None:
        self.column = column
        self.qualifier = qualifier
        super().__init__(
            f"ORDER BY column '{qualifier}.{column}' cannot be resolved: it is not a "
            f"projected field, a base column, or a column on a join that is in scope. "
            f"Project it (add to dimensions/measures), reference it in a filter, or "
            f"order by a projected field instead."
        )
