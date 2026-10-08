"""Turn the queries and datasources SLayer has already loaded into telemetry tokens."""

import io
import tokenize
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.enums import BUILTIN_AGGREGATIONS, normalize_aggregation_name
from slayer.core.formula import ALL_TRANSFORMS
from slayer.core.keys import SCALAR_FUNCTIONS
from slayer.core.models import DatasourceConfig
from slayer.core.query import ComputedDimension, SlayerQuery
from slayer.sql.dialects import dialect_for_ds_type

CUSTOM = "custom"
OTHER = "other"
_DEMO_DB = ("demo", "jaffle_shop.duckdb")
_SIGNIFICANT = (tokenize.NAME, tokenize.OP, tokenize.NUMBER, tokenize.STRING)


class QueryFeatures(BaseModel):
    """Structural flags and built-in (or ``custom``) function names of one query attempt."""

    flags: set[str] = Field(default_factory=set)
    transforms: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)


def _tokens(text: str) -> list[tokenize.TokenInfo]:
    try:
        return [t for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type in _SIGNIFICANT]
    except (tokenize.TokenError, SyntaxError):
        return []


def _close_paren(tokens: list[tokenize.TokenInfo], open_at: int) -> int:
    depth = 0
    for i in range(open_at, len(tokens)):
        if tokens[i].string == "(":
            depth += 1
        elif tokens[i].string == ")":
            depth -= 1
            if depth == 0:
                return i
    return len(tokens)


def _wraps_aggregate(inner: list[tokenize.TokenInfo]) -> bool:
    return any(
        t.string == ":" or (t.type == tokenize.NAME and i + 1 < len(inner) and inner[i + 1].string == "(")
        for i, t in enumerate(inner)
    )


def _count_calls(text: str, out: QueryFeatures) -> None:
    tokens = _tokens(text)
    for i, tok in enumerate(tokens):
        if tok.type != tokenize.NAME:
            continue
        before = tokens[i - 1].string if i else ""
        if before == ":":
            aggregation = normalize_aggregation_name(tok.string)
            out.aggregations.append(aggregation if aggregation in BUILTIN_AGGREGATIONS else CUSTOM)
            continue
        if before == "." or i + 1 >= len(tokens) or tokens[i + 1].string != "(":
            continue
        name = tok.string.lower()
        if name in SCALAR_FUNCTIONS:
            continue
        aggregation = normalize_aggregation_name(tok.string)
        wraps = _wraps_aggregate(tokens[i + 2:_close_paren(tokens, i + 1)])
        if name in ALL_TRANSFORMS and (wraps or aggregation not in BUILTIN_AGGREGATIONS):
            out.transforms.append(name)
        else:
            out.aggregations.append(aggregation if aggregation in BUILTIN_AGGREGATIONS else CUSTOM)


def _stage(raw: Any) -> SlayerQuery | None:
    if isinstance(raw, SlayerQuery):
        return raw
    try:
        return SlayerQuery.model_validate(raw)
    except Exception:
        return None


def query_features(query: Any) -> QueryFeatures:
    """Features of a query as handed to the engine (a query, a stage list, or a saved query's name)."""
    out = QueryFeatures()
    if isinstance(query, str):
        out.flags.add("saved_query")
        return out
    raw_stages = query if isinstance(query, list) else [query]
    if len(raw_stages) > 1:
        out.flags.add("multistage")
    for stage in filter(None, map(_stage, raw_stages)):
        if stage.source_model is not None and not isinstance(stage.source_model, str):
            out.flags.add("inline_source")
        if stage.time_dimensions:
            out.flags.add("time_dimensions")
        if stage.filters:
            out.flags.add("filters")
        if any(isinstance(d, ComputedDimension) for d in stage.dimensions or []):
            out.flags.add("computed_dimensions")
        if stage.to_many_handling != SlayerQuery.model_fields["to_many_handling"].default:
            out.flags.add("to_many_handling")
        for text in [m.formula for m in stage.measures or [] if m.formula] + list(stage.filters or []):
            _count_calls(text, out)
    return out


def dialect(datasource: DatasourceConfig) -> str:
    """The datasource's SQL dialect name, or ``other`` for an unrecognised type."""
    ds_type = (datasource.type or "").lower()
    found = dialect_for_ds_type(ds_type)
    return found.sqlglot_name if ds_type in found.ds_type_aliases else OTHER


def is_demo(datasource: DatasourceConfig) -> bool:
    """The bundled Jaffle Shop demo database."""
    return datasource.type == "duckdb" and Path(datasource.database or "").parts[-2:] == _DEMO_DB
