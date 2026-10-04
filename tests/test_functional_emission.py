"""SLayer emits only the functional aggregation spelling: importer formulas, tool descriptions, help and docs."""

import json
import re
import warnings
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeJoin,
    CubeMeasure,
    CubeProject,
    CubeView,
    CubeViewCubeRef,
)
from slayer.cube.parser import parse_cube_project
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import (
    DbtDimension,
    DbtEntity,
    DbtMeasure,
    DbtMeasureAggParams,
    DbtMetric,
    DbtMetricInput,
    DbtMetricTypeParams,
    DbtProject,
    DbtSemanticModel,
)
from slayer.engine.syntax import parse_expr
from slayer.mcp.server import create_mcp_server
from slayer.memories.help_seed import HELP_TOPICS
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine
from tests.test_osi_converter import _SCHEMA

_TESTS = Path(__file__).parent
_DOCS = _TESTS.parent / "docs"
_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_AGG_NAMES = sorted(BUILTIN_AGGREGATIONS | {"agg", "sum_sq"}, key=len, reverse=True)
_COLON_AGG_TEXT = re.compile(
    r"[\w*)\]>]:(?:<(?:agg|aggregation)\w*>|(?:" + "|".join(_AGG_NAMES) + r")\b)"
)
# Any `<ref>:<name>` token; too broad for docs (`user:pass`, `host:port`).
_COLON_ANY = re.compile(r"(?<![\w-])(?!memory:|https?:)(?:[A-Za-z_][\w.]*|\*)(?<!\.):[a-z_]\w*\b")


def _assert_functional(models: list[SlayerModel]) -> None:
    formulas = [(m.name, mm.name, mm.formula) for m in models for mm in m.measures]
    assert formulas
    for model, measure, formula in formulas:
        assert ":" not in _LITERAL.sub("''", formula), (model, measure, formula)
        parse_expr(formula)


def _assert_no_spelling_warnings(rec: list[warnings.WarningMessage]) -> None:
    msgs = [str(w.message) for w in rec]
    assert not [m for m in msgs if "colon" in m.lower() or "auto-rewrote" in m.lower()], msgs


# ── importers ─────────────────────────────────────────────────────────────────


def _dbt_project() -> DbtProject:
    return DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders", model="orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id"),
                          DbtEntity(name="customer_id", type="foreign")],
                dimensions=[DbtDimension(name="region", type="categorical")],
                measures=[
                    DbtMeasure(name="revenue", agg="sum", expr="amount"),
                    DbtMeasure(name="order_count", agg="count", expr="id"),
                    DbtMeasure(name="avg_amount", agg="average", expr="amount"),
                    DbtMeasure(name="uniq_customers", agg="count_distinct", expr="customer_id"),
                    DbtMeasure(name="median_latency", agg="median", expr="latency"),
                    DbtMeasure(name="p90_latency", agg="percentile", expr="latency",
                               agg_params=DbtMeasureAggParams(percentile=0.9)),
                    DbtMeasure(name="paid_orders", agg="sum_boolean", expr="is_paid"),
                ],
            ),
        ],
        metrics=[
            DbtMetric(name="us_p90_latency", type="simple",
                      type_params=DbtMetricTypeParams.model_validate({"measure": "p90_latency"}),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
            DbtMetric(name="us_paid_orders", type="simple",
                      type_params=DbtMetricTypeParams.model_validate({"measure": "paid_orders"}),
                      filter="{{ Dimension('orders__region') }} = 'US'"),
            DbtMetric(name="aov", type="ratio",
                      type_params=DbtMetricTypeParams(
                          numerator=DbtMetricInput(name="revenue"),
                          denominator=DbtMetricInput(name="order_count"))),
            DbtMetric(name="rev_per_customer", type="derived",
                      type_params=DbtMetricTypeParams(
                          expr="revenue / uniq_customers",
                          metrics=[DbtMetricInput(name="revenue"),
                                   DbtMetricInput(name="uniq_customers")])),
        ],
    )


def _cube_project() -> CubeProject:
    return CubeProject(
        cubes=[
            CubeCube(
                name="orders", sql_table="public.orders",
                joins=[CubeJoin(name="customers", relationship="many_to_one",
                                sql="{CUBE}.customer_id = {customers.id}")],
                measures=[
                    CubeMeasure(name="count", type="count"),
                    CubeMeasure(name="count_7d", type="count", rolling_window={"trailing": "7 day"}),
                    CubeMeasure(name="revenue", type="sum", sql="{CUBE}.amount"),
                    CubeMeasure(name="revenue_30d", type="sum", sql="{CUBE}.amount",
                                rolling_window={"trailing": "30 day"}),
                    CubeMeasure(name="uniq", type="count_distinct", sql="{CUBE}.customer_id"),
                    CubeMeasure(name="aov", type="number", sql="{revenue} / {count}"),
                ],
                dimensions=[
                    CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
                    CubeDimension(name="status", sql="{CUBE}.status", type="string"),
                ],
            ),
            CubeCube(
                name="customers", sql_table="public.customers",
                measures=[CubeMeasure(name="count", type="count"),
                          CubeMeasure(name="ltv", type="sum", sql="{CUBE}.ltv")],
                dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                          primary_key=True)],
            ),
        ],
        views=[CubeView(name="overview", cubes=[
            CubeViewCubeRef(join_path="orders", includes=["count", "revenue", "status"]),
            CubeViewCubeRef(join_path="orders.customers", prefix=True, includes=["count", "ltv"]),
        ])],
    )


@pytest.fixture
def shop_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    with disposable_engine(f"sqlite:///{tmp_path}/shop.db") as engine:
        with engine.connect() as conn:
            for ddl in _SCHEMA:
                conn.execute(sa.text(ddl))
            conn.commit()
        yield engine


def test_dbt_import_is_functional() -> None:
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = DbtToSlayerConverter(project=_dbt_project(), data_source="ds").convert()
    _assert_functional(result.models)
    _assert_no_spelling_warnings(rec)


@pytest.mark.parametrize("source", ["fixture", "project"])
def test_cube_import_is_functional(source: str) -> None:
    if source == "fixture":
        project, _ = parse_cube_project(str(_TESTS / "fixtures" / "cube_project"))
    else:
        project = _cube_project()
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = CubeToSlayerConverter(project=project, data_source="ds").convert()
    _assert_functional(result.models)
    _assert_no_spelling_warnings(rec)


def test_osi_import_is_functional(shop_engine: sa.Engine) -> None:
    doc = parse_osi_path(_TESTS / "fixtures" / "osi" / "shop.yaml")[0]
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        result = OsiToSlayerConverter(
            documents=[doc], data_source="ds", sa_engine=shop_engine).convert()
    _assert_functional(result.models)
    _assert_no_spelling_warnings(rec)


def test_cube_emitters_render_functional() -> None:
    result = CubeToSlayerConverter(project=_cube_project(), data_source="ds").convert()
    formulas = {(m.name, mm.name): mm.formula for m in result.models for mm in m.measures}
    assert formulas[("orders", "count")] == "count(*)"
    assert formulas[("orders", "count_7d")] == "count(*, window='7d')"
    assert formulas[("orders", "revenue_30d")] == "sum(amount, window='30d')"
    assert formulas[("overview", "count")] == "count(*)"
    assert formulas[("overview", "revenue")] == "sum(amount)"
    assert formulas[("overview", "customers_count")] == "count(customers.*)"
    assert formulas[("overview", "customers_ltv")] == "sum(customers.ltv_col)"


@pytest.mark.parametrize(
    ("measure", "expected"),
    [
        ("revenue", "sum(amount)"),
        ("avg_amount", "avg(amount)"),
        ("uniq_customers", "count_distinct(customer_id)"),
        ("median_latency", "median(latency)"),
        ("p90_latency", "percentile(latency, p=0.9)"),
    ],
)
def test_dbt_mapped_aggs_render_functional(measure: str, expected: str) -> None:
    result = DbtToSlayerConverter(project=_dbt_project(), data_source="ds").convert()
    orders = next(m for m in result.models if m.name == "orders")
    assert {mm.name: mm.formula for mm in orders.measures}[measure] == expected


# ── agent-facing text ─────────────────────────────────────────────────────────


async def test_mcp_tool_descriptions_are_functional(tmp_path: Path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))
    tools = await server.list_tools()
    for tool in tools:
        text = (tool.description or "") + json.dumps(tool.inputSchema)
        assert not _COLON_ANY.search(text), (tool.name, _COLON_ANY.findall(text))
    assert not _COLON_ANY.search(server.instructions or "")
    create_model = next(t for t in tools if t.name == "create_model")
    assert "sum_sq(column)" in (create_model.description or "")


def test_help_content_is_functional() -> None:
    for topic in HELP_TOPICS:
        text = topic.learning + topic.description
        assert not _COLON_ANY.search(text), (topic.id, _COLON_ANY.findall(text))


def test_docs_are_functional() -> None:
    hits = [
        f"{path.relative_to(_DOCS)}:{lineno}: {line.strip()}"
        for path in sorted(_DOCS.rglob("*.md"))
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if _COLON_AGG_TEXT.search(line)
    ]
    assert hits == []
