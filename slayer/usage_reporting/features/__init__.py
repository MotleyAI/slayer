"""Turn the queries and datasources SLayer has already loaded into usage tokens."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.enums import ALL_TRANSFORMS, BUILTIN_AGGREGATIONS, GRANULARITY_NAMES, normalize_aggregation_name
from slayer.core.models import DatasourceConfig
from slayer.core.query import ComputedDimension, SlayerQuery
from slayer.core.syntax import AggCall, ArrayLit, Cast, ScalarCall, TransformCall, parse_expr, walk_parsed
from slayer.sql.dialects import dialect_for_ds_type
from slayer.usage_reporting.payload import FUNCTIONS

CUSTOM = "custom"
OTHER = "other"
_DEMO_DB = ("demo", "jaffle_shop.duckdb")


class QueryFeatures(BaseModel):
    """Flags and built-in (or ``custom``) names of one query attempt; repeats count."""

    flags: list[str] = Field(default_factory=list)
    transforms: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    functions: list[str] = Field(default_factory=list)


def _wraps_aggregate(node: AggCall) -> bool:
    return any(isinstance(n, (AggCall, TransformCall)) for n in walk_parsed(node.source))


def _count_agg_call(node: AggCall, out: QueryFeatures) -> None:
    if node.agg.lower() in GRANULARITY_NAMES:
        return
    name = normalize_aggregation_name(node.agg)
    if name in ALL_TRANSFORMS and _wraps_aggregate(node):
        out.transforms.append(name)
    else:
        out.aggregations.append(name if name in BUILTIN_AGGREGATIONS else CUSTOM)
    if node.filter is not None:
        out.flags.append("aggregate_filter")


def _count_node(node: Any, out: QueryFeatures) -> None:
    if isinstance(node, AggCall):
        _count_agg_call(node, out)
    elif isinstance(node, TransformCall):
        out.transforms.append(node.op)
    elif isinstance(node, ScalarCall):
        if node.name in FUNCTIONS:
            out.functions.append(node.name)
    elif isinstance(node, Cast):
        out.flags.append("try_cast" if node.try_ else "cast")
    elif isinstance(node, ArrayLit):
        out.flags.append("list_literal")


def _count_expression(text: str, out: QueryFeatures) -> None:
    try:
        parsed = parse_expr(text)
    except Exception:
        return
    for node in walk_parsed(parsed):
        _count_node(node, out)


def _stage(raw: Any) -> SlayerQuery | None:
    if isinstance(raw, SlayerQuery):
        return raw
    try:
        return SlayerQuery.model_validate(raw)
    except Exception:
        return None


def _stage_flags(stage: SlayerQuery) -> set[str]:
    present = {
        "inline_source": stage.source_model is not None and not isinstance(stage.source_model, str),
        "time_dimensions": bool(stage.time_dimensions),
        "filters": bool(stage.filters),
        "computed_dimensions": any(isinstance(d, ComputedDimension) for d in stage.dimensions or []),
        "to_many_handling": stage.to_many_handling != SlayerQuery.model_fields["to_many_handling"].default,
    }
    return {flag for flag, on in present.items() if on}


def _expressions(stage: SlayerQuery) -> Iterator[str]:
    """Every Mode B text of a stage: measures, dimensions, time dimensions, filters, order, inline measures."""
    yield from (m.formula for m in stage.measures or [] if m.formula)
    yield from (d.expression for d in stage.dimensions or [] if isinstance(d, ComputedDimension))
    yield from (td.dimension for td in stage.time_dimensions or [] if isinstance(td.dimension, str))
    yield from stage.filters or []
    yield from (o.raw_formula for o in stage.order or [] if o.raw_formula)
    inline_measures = getattr(stage.source_model, "measures", None) or []
    yield from (m.formula for m in inline_measures if m.formula)


def query_features(query: Any) -> QueryFeatures:
    """Features of a query as handed to the engine (a query, a stage list, or a saved query's name)."""
    out = QueryFeatures()
    if isinstance(query, str):
        out.flags.append("saved_query")
        return out
    raw_stages = query if isinstance(query, list) else [query]
    structural = {"multistage"} if len(raw_stages) > 1 else set()
    for stage in filter(None, map(_stage, raw_stages)):
        structural |= _stage_flags(stage)
        for text in _expressions(stage):
            _count_expression(text, out)
    out.flags.extend(sorted(structural))
    return out


def dialect(datasource: DatasourceConfig) -> str:
    """The datasource's SQL dialect name, or ``other`` for an unrecognised type."""
    ds_type = (datasource.type or "").lower()
    found = dialect_for_ds_type(ds_type)
    return found.sqlglot_name if ds_type in found.ds_type_aliases else OTHER


def is_demo(datasource: DatasourceConfig) -> bool:
    """The bundled Jaffle Shop demo database."""
    return datasource.type == "duckdb" and Path(datasource.database or "").parts[-2:] == _DEMO_DB
