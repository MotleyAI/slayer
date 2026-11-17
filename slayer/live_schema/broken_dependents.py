"""The stored definitions a list re-type breaks: each dependant is planned against the datasets as stored and as
re-typed, without execution."""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any, Literal

from pydantic import BaseModel, Field

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.warnings import BrokenDependentWarning, SlayerBrokenDependentWarning
from slayer.engine import query_engine
from slayer.live_schema import schema_drift
from slayer.database.client import BorrowedSqlClient, SqlClient
from slayer.storage.base import StorageBackend
from slayer.storage.dataset_overlay import DatasetOverlayStorage

DependentKind = Literal["metric", "field", "filter", "saved_query"]


class _Dependent(BaseModel):
    dataset: str
    kind: DependentKind
    name: str
    causes: list[tuple[str, str]] = Field(default_factory=list)


class FoundBreak(BaseModel):
    """A broken dependant found before the saves, attributed to ``causes`` (re-typed ``(dataset, field)``), each
    alone or, when ``joint``, only together."""

    causes: list[tuple[str, str]]
    payload: BrokenDependentWarning
    joint: bool = False


def retyped_list_fields(*, stored: SlayerDataset, final: SlayerDataset) -> list[str]:
    """``final``'s fields that ``stored`` keeps as a non-list and ``final`` re-types to a list."""
    before = {c.name: c.datatype for c in stored.fields}
    return [c.name for c in final.fields
            if c.datatype is DataType.ARRAY and before.get(c.name, DataType.ARRAY) is not DataType.ARRAY]


def _entry_dependents(
    entry: schema_drift.ToDeleteEntry, *, by_name: dict[str, SlayerDataset], seed: tuple[str, str],
) -> Iterator[tuple[str, DependentKind, str]]:
    owner = entry.dataset_name
    if isinstance(entry, schema_drift.WholeDatasetDelete):
        if by_name[owner].source_queries:
            yield owner, "saved_query", owner
        return
    groups: tuple[tuple[DependentKind, list[str]], ...] = (
        ("field", [c for c in entry.remove.fields if (owner, c) != seed]),
        ("metric", entry.remove.metrics), ("filter", entry.remove_filters))
    for kind, names in groups:
        for name in names:
            yield owner, kind, name


def _dependents(*, datasets: list[SlayerDataset], retyped: dict[str, list[str]]) -> list[_Dependent]:
    """Each definition a delete of a re-typed field cascades to (the field itself excluded), once."""
    by_name = {m.name: m for m in datasets}
    out: dict[tuple[str, str, str], _Dependent] = {}
    for seed in ((m, c) for m, columns in retyped.items() for c in columns):
        for entry in schema_drift.field_dependents(datasets=datasets, dataset=by_name[seed[0]], field=seed[1]):
            for owner, kind, name in _entry_dependents(entry, by_name=by_name, seed=seed):
                dep = out.setdefault((owner, kind, name), _Dependent(dataset=owner, kind=kind, name=name))
                if seed not in dep.causes:
                    dep.causes.append(seed)
    return list(out.values())


def _probe(dependent: _Dependent) -> Any:
    if dependent.kind == "saved_query":
        return dependent.dataset
    if dependent.kind == "metric":
        return {"population": dependent.dataset, "metrics": [dependent.name]}
    if dependent.kind == "field":
        return {"population": dependent.dataset, "dimensions": [dependent.name]}
    return {"population": dependent.dataset, "metrics": ["count(*)"]}


async def _plan(engine: query_engine.SlayerQueryEngine, dependent: _Dependent, *, data_source: str) -> None:
    await engine.execute(query=_probe(dependent), dry_run=True, data_source=data_source)


def _one_retype(*, stored: SlayerDataset, final: SlayerDataset, field: str) -> SlayerDataset:
    """``stored`` with only ``field`` taken from ``final``."""
    return stored.model_copy(update={"fields": [
        final.get_field(field) or c if c.name == field else c for c in stored.fields]})


async def broken_dependents(
    *, datasource: DatasourceConfig, storage: StorageBackend, client: SqlClient, stored: list[SlayerDataset],
    retyped: list[SlayerDataset],
) -> list[FoundBreak]:
    """The definitions that plan over ``stored`` and fail over ``retyped`` (unsaved); a check that cannot run is
    found unchecked. ``client`` stays open; nothing is warned until ``report_broken``."""
    by_name = {m.name: m for m in stored}
    columns = {m.name: cols for m in retyped if (cols := retyped_list_fields(stored=by_name[m.name], final=m))}
    if not columns:
        return []
    dependents = _dependents(datasets=stored, retyped=columns)
    if not dependents:
        return []
    finals = {m.name: m for m in retyped if m.name in columns}
    out: list[FoundBreak] = []

    def lend(_ds: DatasourceConfig) -> SqlClient:
        return BorrowedSqlClient(client)

    async def breaks_alone(dep: _Dependent, cause: tuple[str, str]) -> bool:
        dataset = _one_retype(stored=by_name[cause[0]], final=finals[cause[0]], field=cause[1])
        async with query_engine.short_lived_engine(DatasetOverlayStorage(storage, datasets=[dataset]),
                                                   client_factory=lend) as alone:
            try:
                await _plan(alone, dep, data_source=datasource.name)
            except ValueError:
                return True
            except Exception:  # noqa: BLE001 — a cause that cannot be planned is not blamed
                return False
        return False

    overlay = DatasetOverlayStorage(storage, datasets=finals.values())
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
    """The breaks whose cause (all causes, when joint) was saved, each naming its first saved field and warned."""
    out: list[BrokenDependentWarning] = []
    for f in found:
        saved = [(m, c) for m, c in f.causes if m not in unsaved]
        if saved and (not f.joint or len(saved) == len(f.causes)):
            out.append(f.payload.model_copy(update={"field": "{}.{}".format(*saved[0])}))
    for payload in out:
        warnings.warn(SlayerBrokenDependentWarning(payload), stacklevel=2)
    return out


async def _check(
    dep: _Dependent, *, before: query_engine.SlayerQueryEngine, after: query_engine.SlayerQueryEngine,
    data_source: str,
) -> BrokenDependentWarning | None:
    def report(**fields: Any) -> BrokenDependentWarning:
        return BrokenDependentWarning(dataset=dep.dataset, data_source=data_source, dependent_kind=dep.kind,
                                      name=dep.name, field="{}.{}".format(*dep.causes[0]), **fields)

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
