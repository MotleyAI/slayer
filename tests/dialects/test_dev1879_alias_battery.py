"""Alias validity and result-key uniformity for unnamed formula measures.

A fixed battery of unnamed-measure shapes crossed with every supported
dialect: emitted projection aliases must be valid identifiers for the
dialect after its emission rewrites, and derived result keys must be
identical across dialects.
"""

from __future__ import annotations

import re
import tempfile

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


_ALL_DIALECTS = [
    "postgres",
    "mysql",
    "sqlite",
    "clickhouse",
    "bigquery",
    "snowflake",
    "duckdb",
    "redshift",
    "trino",
    "presto",
    "databricks",
    "spark",
    "tsql",
    "oracle",
]

# Dialects that mangle dotted aliases to bare identifiers on emission.
_DOT_MANGLING = {"bigquery", "tsql"}

_IDENT = r"[A-Za-z_]\w*"
_BARE_ALIAS_RE = re.compile(_IDENT)
_DOTTED_ALIAS_RE = re.compile(rf"{_IDENT}(\.{_IDENT})*")

_SHAPES = {
    "arithmetic": "logo_churn:sum / logo_bop:sum",
    "transform": "time_shift(cmrr_eop:sum, -1, 'year')",
    "transform_of_composite": "time_shift(logo_churn:sum / logo_bop:sum, -1)",
    "mixed_literal": "logo_churn:sum * 100",
    "cross_model_composite": "logo_churn:sum / targets.goal:sum",
    "parametric_agg": "price:percentile(p=0.9)",
}

# Capability gaps, not alias concerns: these dialects raise a loud
# NotImplementedError for native percentile.
_UNSUPPORTED_CELLS = {
    ("parametric_agg", "mysql"),
    ("parametric_agg", "tsql"),
}


def _mart() -> SlayerModel:
    return SlayerModel(
        name="mart",
        data_source="test",
        sql_table="mart",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="target_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="logo_churn", type=DataType.DOUBLE),
            Column(name="logo_bop", type=DataType.DOUBLE),
            Column(name="cmrr_eop", type=DataType.DOUBLE),
            Column(name="price", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="targets", join_pairs=[["target_id", "id"]]),
        ],
    )


def _targets() -> SlayerModel:
    return SlayerModel(
        name="targets",
        data_source="test",
        sql_table="targets",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="goal", type=DataType.DOUBLE),
        ],
    )


def _shape_query(shape: str) -> SlayerQuery:
    return SlayerQuery(
        source_model="mart",
        measures=[ModelMeasure(formula=_SHAPES[shape])],
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )
        ],
    )


async def _dry_response(query: SlayerQuery, *, dialect: str):
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name="test", type=dialect)
        )
        await storage.save_model(_mart())
        await storage.save_model(_targets())
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(query, dry_run=True)


def _projection_aliases(sql: str, dialect: str) -> list[str]:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [node.alias for node in tree.find_all(exp.Alias) if node.alias]


def _measure_key(resp) -> str:
    keys = [c for c in resp.columns if c != "mart.created_at"]
    assert len(keys) == 1, resp.columns
    return keys[0]


def _emitted_alias(key: str, dialect: str) -> str:
    return key.replace(".", "___") if dialect in _DOT_MANGLING else key


@pytest.mark.parametrize("dialect", _ALL_DIALECTS)
@pytest.mark.parametrize("shape", sorted(_SHAPES))
async def test_aliases_valid_per_dialect(shape: str, dialect: str) -> None:
    if (shape, dialect) in _UNSUPPORTED_CELLS:
        pytest.skip(f"{dialect} has no native percentile")
    resp = await _dry_response(_shape_query(shape), dialect=dialect)
    sql = resp.sql
    assert sql is not None
    alias = _emitted_alias(_measure_key(resp), dialect)
    pattern = (
        _BARE_ALIAS_RE if dialect in _DOT_MANGLING else _DOTTED_ALIAS_RE
    )
    assert pattern.fullmatch(alias), (
        f"invalid {dialect} alias {alias!r} in:\n{sql}"
    )
    assert alias in _projection_aliases(sql, dialect), (
        f"measure alias {alias!r} not projected in:\n{sql}"
    )


@pytest.mark.parametrize("shape", sorted(_SHAPES))
async def test_result_keys_uniform_across_dialects(shape: str) -> None:
    keys_by_dialect = {}
    for dialect in _ALL_DIALECTS:
        if (shape, dialect) in _UNSUPPORTED_CELLS:
            continue
        resp = await _dry_response(_shape_query(shape), dialect=dialect)
        keys_by_dialect[dialect] = list(resp.columns)
    baseline = keys_by_dialect["postgres"]
    assert all(k == baseline for k in keys_by_dialect.values()), (
        keys_by_dialect
    )


async def test_bigquery_unnamed_formula_measures() -> None:
    query = SlayerQuery(
        source_model="mart",
        measures=[
            ModelMeasure(formula="logo_churn:sum / logo_bop:sum"),
            ModelMeasure(formula="time_shift(cmrr_eop:sum, -1, 'year')"),
        ],
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )
        ],
    )
    resp = await _dry_response(query, dialect="bigquery")
    sql = resp.sql
    assert sql is not None
    assert "``" not in sql
    for key in (
        "mart.logo_churn_sum_logo_bop_sum",
        "mart.time_shift_cmrr_eop_sum_1_year",
    ):
        assert key in resp.columns
        mangled = key.replace(".", "___")
        assert mangled in _projection_aliases(sql, "bigquery"), (
            f"measure alias {mangled!r} not projected in:\n{sql}"
        )
