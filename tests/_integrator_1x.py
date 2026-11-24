"""A known 1.8.4 integrator's SLayer calls, replayed: its v15 model set, read-only render backend and filter walk."""

from __future__ import annotations

import importlib
import warnings
from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

import yaml
from pydantic import BaseModel

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import DatasourceConfig
from slayer.core.warnings import DeprecatedInputWarning
from slayer.storage.base import StorageBackend

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "integrator_1x"
MODELS_DIR = FIXTURE / "models" / "warehouse"
DATA_SQL = FIXTURE / "data.sql"
MODEL_NAMES = ("orders", "customers")
DATA_SOURCE = "warehouse"

#: The node classes the integrator imports from ``slayer.engine.syntax`` and refuses to walk past.
NODE_CLASSES = (
    "AggCall", "Arith", "ArrayLit", "BoolOp", "Cmp", "DottedRef", "Literal", "Placeholder", "Ref", "ScalarCall",
    "StarSource", "TransformCall", "TupleLit", "UnaryOp",
)
_SCALARS = (str, int, float, Decimal, bool, type(None))


def legacy(module: str, name: str) -> Any:
    """``from <module> import <name>``, as the integrator spells it."""
    return getattr(importlib.import_module(module), name)


def raw_documents() -> dict[str, dict[str, Any]]:
    return {name: yaml.safe_load((MODELS_DIR / f"{name}.yaml").read_text()) for name in MODEL_NAMES}


def v15_models() -> dict[str, Any]:
    """The fixture documents validated as the integrator validates them."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        slayer_model = legacy("slayer.core.models", "SlayerModel")
        return {name: slayer_model.model_validate(raw) for name, raw in raw_documents().items()}


def deprecations(record: list[warnings.WarningMessage]) -> list[DeprecatedInputWarning]:
    return [w.message.payload for w in record if isinstance(w.message, DeprecatedInputFutureWarning)]


@contextmanager
def one_deprecation(*, naming: str) -> Generator[list[warnings.WarningMessage]]:
    """The block emits exactly one deprecated-input warning, removed in 3.0, whose replacement names ``naming``."""
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        yield record
    found = deprecations(record)
    assert len(found) == 1, [p.human_message() for p in found]
    assert found[0].removal == "3.0"
    assert naming in found[0].new, found[0].human_message()


@contextmanager
def quiet() -> Generator[None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


def _read_only(what: str) -> NoReturn:
    raise RuntimeError(f"read-only storage: {what}")


class ReadOnlyStorage(StorageBackend):
    """The integrator's render backend: validated models, 1.8.4 hook names, every write refused."""

    def __init__(self, models: Mapping[str, Any], *, ds_type: str) -> None:
        self._type = ds_type
        self._models = {(m.data_source, m.name): m for m in models.values()}

    async def get_model(self, name: str, data_source: str | None = None) -> Any:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return None
        model = self._models.get(target)
        return model.model_copy(deep=True) if model is not None else None

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        return sorted(self._models)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        if name not in {data_source for data_source, _ in self._models}:
            return None
        return DatasourceConfig(name=name, type=self._type)

    async def list_datasources(self) -> list[str]:
        return sorted({data_source for data_source, _ in self._models})

    async def get_datasource_priority(self) -> list[str]:
        return []

    async def _save_model_impl(self, model: Any) -> None:
        _read_only("saving a model")

    async def _delete_model_row(self, *, data_source: str, name: str) -> bool:
        _read_only("deleting a model")

    async def update_column_sampled(self, *args: Any, **kwargs: Any) -> None:
        _read_only("recording a column sample")

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        _read_only("saving a datasource")

    async def _delete_datasource_row(self, name: str) -> bool:
        _read_only("deleting a datasource")

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        _read_only("setting the datasource priority")

    async def _save_memory_row(self, memory: Any) -> None:
        _read_only("saving a memory")

    async def _get_memory_row(self, memory_id: str) -> Any:
        _read_only("reading a memory")

    async def _list_memories_rows(self, *args: Any, **kwargs: Any) -> Any:
        _read_only("listing memories")

    async def _delete_memory_row(self, memory_id: str) -> bool:
        _read_only("deleting a memory")

    async def _next_memory_seq(self) -> str:
        _read_only("allocating a memory id")

    async def save_embedding(self, row: Any) -> None:
        _read_only("saving an embedding")

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Any:
        _read_only("reading an embedding")

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Any]:
        _read_only("listing embeddings")

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        _read_only("deleting embeddings")


def node_classes() -> tuple[type, ...]:
    return tuple(legacy("slayer.engine.syntax", name) for name in NODE_CLASSES)


def walk(node: object, *, known: tuple[type, ...]) -> Iterator[BaseModel]:
    """Every node of a parsed tree via each node's declared fields, refusing an unknown class (the integrator's walk)."""
    if isinstance(node, BaseModel):
        if not isinstance(node, known):
            raise ValueError(type(node).__name__)
        yield node
        for name in type(node).model_fields:
            yield from walk(getattr(node, name), known=known)
    elif isinstance(node, (tuple, list)):
        for item in node:
            yield from walk(item, known=known)
    elif not isinstance(node, _SCALARS):
        raise ValueError(type(node).__name__)


def filter_tokens(clause: str, *, source: str, models: Mapping[str, Any]) -> list[str]:
    """The ``model.column`` tokens a filter clause reads, resolved as the integrator resolves them."""
    known = node_classes()
    ref, dotted = legacy("slayer.engine.syntax", "Ref"), legacy("slayer.engine.syntax", "DottedRef")
    parse_filter_expr = legacy("slayer.engine.syntax", "parse_filter_expr")
    resolve_ref_target = legacy("slayer.sql.column_expansion", "resolve_ref_target")

    def tokens(node: Any, owner: Any, expanding: frozenset[str]) -> list[str]:
        if isinstance(node, ref):
            target, leaf = owner, node.name
        else:
            *qualifiers, leaf = node.parts
            target = resolve_ref_target(qualifiers=tuple(qualifiers), source_model=owner, models_by_name=dict(models))
            if target is None:
                raise ValueError(".".join(node.parts))
        if target.list_of is not None:
            parent, column = target.list_of
            return [f"{parent}.{column}.{leaf}"]
        token = f"{target.name}.{leaf}"
        measure = next((m for m in target.measures if m.name == leaf), None)
        if measure is None:
            return [token]
        if token in expanding:
            raise ValueError(token)
        return [
            found
            for inner in walk(parse_filter_expr(measure.formula), known=known)
            if isinstance(inner, (ref, dotted))
            for found in tokens(inner, target, expanding | {token})
        ]

    tree = parse_filter_expr(clause)
    return [
        found
        for node in walk(tree, known=known)
        if isinstance(node, (ref, dotted))
        for found in tokens(node, models[source], frozenset())
    ]
