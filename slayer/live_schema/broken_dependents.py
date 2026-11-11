"""The stored definitions a list re-type breaks: each dependant is planned against the models as stored and as
re-typed, without execution."""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any, Literal

from pydantic import BaseModel, Field

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.warnings import BrokenDependentWarning, SlayerBrokenDependentWarning
from slayer.engine import query_engine
from slayer.live_schema import schema_drift
from slayer.database.client import BorrowedSqlClient, SqlClient
from slayer.storage.base import StorageBackend
from slayer.storage.model_overlay import ModelOverlayStorage

DependentKind = Literal["measure", "column", "filter", "saved_query"]


class _Dependent(BaseModel):
    model: str
    kind: DependentKind
    name: str
    causes: list[tuple[str, str]] = Field(default_factory=list)


class FoundBreak(BaseModel):
    """A broken dependant found before the saves, attributed to ``causes`` (re-typed ``(model, column)``), each
    alone or, when ``joint``, only together."""

    causes: list[tuple[str, str]]
    payload: BrokenDependentWarning
    joint: bool = False


def retyped_list_columns(*, stored: SlayerModel, final: SlayerModel) -> list[str]:
    """``final``'s columns that ``stored`` keeps as a non-list and ``final`` re-types to a list."""
    before = {c.name: c.type for c in stored.columns}
    return [c.name for c in final.columns
            if c.type is DataType.ARRAY and before.get(c.name, DataType.ARRAY) is not DataType.ARRAY]


def _entry_dependents(
    entry: schema_drift.ToDeleteEntry, *, by_name: dict[str, SlayerModel], seed: tuple[str, str],
) -> Iterator[tuple[str, DependentKind, str]]:
    owner = entry.model_name
    if isinstance(entry, schema_drift.WholeModelDelete):
        if by_name[owner].source_queries:
            yield owner, "saved_query", owner
        return
    groups: tuple[tuple[DependentKind, list[str]], ...] = (
        ("column", [c for c in entry.remove.columns if (owner, c) != seed]),
        ("measure", entry.remove.measures), ("filter", entry.remove_filters))
    for kind, names in groups:
        for name in names:
            yield owner, kind, name


def _dependents(*, models: list[SlayerModel], retyped: dict[str, list[str]]) -> list[_Dependent]:
    """Each definition a delete of a re-typed column cascades to (the column itself excluded), once."""
    by_name = {m.name: m for m in models}
    out: dict[tuple[str, str, str], _Dependent] = {}
    for seed in ((m, c) for m, columns in retyped.items() for c in columns):
        for entry in schema_drift.column_dependents(models=models, model=by_name[seed[0]], column=seed[1]):
            for owner, kind, name in _entry_dependents(entry, by_name=by_name, seed=seed):
                dep = out.setdefault((owner, kind, name), _Dependent(model=owner, kind=kind, name=name))
                if seed not in dep.causes:
                    dep.causes.append(seed)
    return list(out.values())


def _probe(dependent: _Dependent) -> Any:
    if dependent.kind == "saved_query":
        return dependent.model
    if dependent.kind == "measure":
        return {"source_model": dependent.model, "measures": [dependent.name]}
    if dependent.kind == "column":
        return {"source_model": dependent.model, "dimensions": [dependent.name]}
    return {"source_model": dependent.model, "measures": ["count(*)"]}


async def _plan(engine: query_engine.SlayerQueryEngine, dependent: _Dependent, *, data_source: str) -> None:
    await engine.execute(query=_probe(dependent), dry_run=True, data_source=data_source)


def _one_retype(*, stored: SlayerModel, final: SlayerModel, column: str) -> SlayerModel:
    """``stored`` with only ``column`` taken from ``final``."""
    return stored.model_copy(update={"columns": [
        final.get_column(column) or c if c.name == column else c for c in stored.columns]})


async def broken_dependents(
    *, datasource: DatasourceConfig, storage: StorageBackend, client: SqlClient, stored: list[SlayerModel],
    retyped: list[SlayerModel],
) -> list[FoundBreak]:
    """The definitions that plan over ``stored`` and fail over ``retyped`` (unsaved); a check that cannot run is
    found unchecked. ``client`` stays open; nothing is warned until ``report_broken``."""
    by_name = {m.name: m for m in stored}
    columns = {m.name: cols for m in retyped if (cols := retyped_list_columns(stored=by_name[m.name], final=m))}
    if not columns:
        return []
    dependents = _dependents(models=stored, retyped=columns)
    if not dependents:
        return []
    finals = {m.name: m for m in retyped if m.name in columns}
    out: list[FoundBreak] = []

    def lend(_ds: DatasourceConfig) -> SqlClient:
        return BorrowedSqlClient(client)

    async def breaks_alone(dep: _Dependent, cause: tuple[str, str]) -> bool:
        model = _one_retype(stored=by_name[cause[0]], final=finals[cause[0]], column=cause[1])
        async with query_engine.short_lived_engine(ModelOverlayStorage(storage, models=[model]),
                                                   client_factory=lend) as alone:
            try:
                await _plan(alone, dep, data_source=datasource.name)
            except ValueError:
                return True
            except Exception:  # noqa: BLE001 — a cause that cannot be planned is not blamed
                return False
        return False

    overlay = ModelOverlayStorage(storage, models=finals.values())
    async with query_engine.short_lived_engine(storage, client_factory=lend) as before, \
            query_engine.short_lived_engine(overlay, client_factory=lend) as after:
        for dep in dependents:
            payload = await _check(dep, before=before, after=after, data_source=datasource.name)
            if payload is None:
                continue
            found = FoundBreak(causes=dep.causes, payload=payload)
            if payload.checked and len(dep.causes) > 1:
                alone = [c for c in dep.causes if await breaks_alone(dep, c)]
                found = found.model_copy(update={"causes": alone} if alone else {"joint": True})
            out.append(found)
    return out


def report_broken(found: list[FoundBreak], *, unsaved: set[str]) -> list[BrokenDependentWarning]:
    """The breaks whose cause (all causes, when joint) was saved, each naming its first saved column and warned."""
    out: list[BrokenDependentWarning] = []
    for f in found:
        saved = [(m, c) for m, c in f.causes if m not in unsaved]
        if saved and (not f.joint or len(saved) == len(f.causes)):
            out.append(f.payload.model_copy(update={"column": "{}.{}".format(*saved[0])}))
    for payload in out:
        warnings.warn(SlayerBrokenDependentWarning(payload), stacklevel=2)
    return out


async def _check(
    dep: _Dependent, *, before: query_engine.SlayerQueryEngine, after: query_engine.SlayerQueryEngine,
    data_source: str,
) -> BrokenDependentWarning | None:
    def report(**fields: Any) -> BrokenDependentWarning:
        return BrokenDependentWarning(model=dep.model, data_source=data_source, dependent_kind=dep.kind,
                                      name=dep.name, column="{}.{}".format(*dep.causes[0]), **fields)

    try:
        await _plan(before, dep, data_source=data_source)
    except ValueError:
        return None
    except Exception as exc:  # noqa: BLE001 — an unrunnable check is reported as unchecked
        return report(checked=False, reason=f"{type(exc).__name__}: {exc}")
    try:
        await _plan(after, dep, data_source=data_source)
    except ValueError as exc:
        return report(error_type=type(exc).__name__, error=str(exc))
    except Exception as exc:  # noqa: BLE001 — an unrunnable check is reported as unchecked
        return report(checked=False, reason=f"{type(exc).__name__}: {exc}")
    return None
