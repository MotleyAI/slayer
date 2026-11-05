"""The stored definitions a list re-type breaks: each dependant is planned against the models as stored and as
re-typed, without execution."""

from __future__ import annotations

import warnings
from typing import Any, Literal

from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.warnings import BrokenDependentWarning, SlayerBrokenDependentWarning
from slayer.engine import query_engine, schema_drift
from slayer.sql.client import BorrowedSqlClient, SqlClient
from slayer.storage.base import StorageBackend
from slayer.storage.model_overlay import ModelOverlayStorage

DependentKind = Literal["measure", "column", "filter", "saved_query"]


class _Dependent(BaseModel):
    model: str
    kind: DependentKind
    name: str
    column: str


def retyped_list_columns(*, stored: SlayerModel, final: SlayerModel) -> list[str]:
    """``final``'s columns that ``stored`` keeps as a non-list and ``final`` re-types to a list."""
    before = {c.name: c.type for c in stored.columns}
    return [c.name for c in final.columns
            if c.type is DataType.ARRAY and before.get(c.name, DataType.ARRAY) is not DataType.ARRAY]


def _dependents(*, models: list[SlayerModel], retyped: dict[str, list[str]]) -> list[_Dependent]:
    """Each definition a delete of a re-typed column cascades to (the column itself excluded), once."""
    by_name = {m.name: m for m in models}
    out: dict[tuple[str, str, str], _Dependent] = {}
    for model_name, columns in retyped.items():
        for column in columns:
            ref = f"{model_name}.{column}"
            for entry in schema_drift.column_dependents(models=models, model=by_name[model_name], column=column):
                def add(kind: DependentKind, name: str, owner: str = entry.model_name) -> None:
                    out.setdefault((owner, kind, name), _Dependent(model=owner, kind=kind, name=name, column=ref))

                if isinstance(entry, schema_drift.WholeModelDelete):
                    if by_name[entry.model_name].source_queries:
                        add("saved_query", entry.model_name)
                    continue
                for c in entry.remove.columns:
                    if (entry.model_name, c) != (model_name, column):
                        add("column", c)
                for m in entry.remove.measures:
                    add("measure", m)
                for f in entry.remove_filters:
                    add("filter", f)
    return list(out.values())


def _probe(dependent: _Dependent) -> Any:
    if dependent.kind == "saved_query":
        return dependent.model
    if dependent.kind == "measure":
        return {"source_model": dependent.model, "measures": [dependent.name]}
    if dependent.kind == "column":
        return {"source_model": dependent.model, "dimensions": [dependent.name]}
    return {"source_model": dependent.model, "measures": ["*:count"]}


async def _plan(engine: query_engine.SlayerQueryEngine, dependent: _Dependent) -> None:
    await engine.execute(query=_probe(dependent), dry_run=True)


async def broken_dependents(
    *, datasource: DatasourceConfig, storage: StorageBackend, client: SqlClient, stored: list[SlayerModel],
    retyped: list[SlayerModel],
) -> list[BrokenDependentWarning]:
    """The definitions that plan over ``stored`` and fail over ``retyped`` (unsaved), each also warned; a check
    that cannot run is reported unchecked. ``client`` stays open."""
    by_name = {m.name: m for m in stored}
    columns = {m.name: cols for m in retyped if (cols := retyped_list_columns(stored=by_name[m.name], final=m))}
    if not columns:
        return []
    dependents = _dependents(models=stored, retyped=columns)
    if not dependents:
        return []
    out: list[BrokenDependentWarning] = []

    def lend(_ds: DatasourceConfig) -> SqlClient:
        return BorrowedSqlClient(client)

    overlay = ModelOverlayStorage(storage, models=[m for m in retyped if m.name in columns])
    async with query_engine.short_lived_engine(storage, client_factory=lend) as before, \
            query_engine.short_lived_engine(overlay, client_factory=lend) as after:
        for dep in dependents:
            payload = await _check(dep, before=before, after=after, data_source=datasource.name)
            if payload is not None:
                warnings.warn(SlayerBrokenDependentWarning(payload), stacklevel=2)
                out.append(payload)
    return out


async def _check(
    dep: _Dependent, *, before: query_engine.SlayerQueryEngine, after: query_engine.SlayerQueryEngine,
    data_source: str,
) -> BrokenDependentWarning | None:
    def report(**fields: Any) -> BrokenDependentWarning:
        return BrokenDependentWarning(model=dep.model, data_source=data_source, dependent_kind=dep.kind,
                                      name=dep.name, column=dep.column, **fields)

    try:
        await _plan(before, dep)
    except ValueError:
        return None
    except Exception as exc:  # noqa: BLE001 — a check that cannot run is reported, never dropped
        return report(checked=False, reason=f"{type(exc).__name__}: {exc}")
    try:
        await _plan(after, dep)
    except ValueError as exc:
        return report(error_type=type(exc).__name__, error=str(exc))
    except Exception as exc:  # noqa: BLE001 — a check that cannot run is reported, never dropped
        return report(checked=False, reason=f"{type(exc).__name__}: {exc}")
    return None
