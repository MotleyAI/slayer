"""Category 8 / WP7: importers must PRESERVE ``__`` dataset names.

The dotted-canonical flip lifts the ban on ``__`` in dataset names. Today every
importer either DROPS or RENAMES an object whose name contains ``__`` (because
``SlayerDataset(name="a__b")`` raises at construction). After the flip they must
carry the ``__`` name through verbatim.

Each FAIL-FIRST test below fails TODAY for a feature reason (the ``__`` name is
rejected → the object is dropped / the conversion aborts). The one INVARIANT
LOCK test guards a shape that must NOT change: dbt's own ``Dimension('e__d')``
input-filter syntax (where ``__`` is dbt's entity/dimension delimiter, not a
SLayer dataset name) must keep lowering to a one-hop dotted ref.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

# ── dbt ──────────────────────────────────────────────────────────────────────
from slayer.core.models import DatasourceConfig
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.entities import EntityRegistry
from slayer.dbt.filters import convert_dbt_filter
from slayer.dbt.models import (
    DbtDimension,
    DbtEntity,
    DbtMeasure,
    DbtProject,
    DbtRegularModel,
    DbtSemanticModel,
)

# ── cube ─────────────────────────────────────────────────────────────────────
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeCube, CubeDimension, CubeProject

from tests._engine_helpers import disposable_engine
from tests._osi_helpers import run_converter

DS = "test"


# ---------------------------------------------------------------------------
# Live-DB fixture (dbt hidden-model import introspects a real table)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    with disposable_engine(f"sqlite:///{tmp_path}/live.db") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text(
                "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, amount REAL)"
            ))
            # A regular dbt staging model whose materialized table name carries ``__``.
            conn.execute(sa.text(
                "CREATE TABLE stg_jaffle_shop__orders "
                "(id INTEGER PRIMARY KEY, amount REAL)"
            ))
            conn.commit()
        yield engine


def _datasource(engine: sa.Engine) -> DatasourceConfig:
    return DatasourceConfig(name=DS, type="sqlite", database=engine.url.database)


# ---------------------------------------------------------------------------
# 1. dbt hidden regular model with a ``__`` name is imported, not dropped
# ---------------------------------------------------------------------------


def test_dbt_hidden_regular_model_dunder_name_is_imported(
    sqlite_engine: sa.Engine,
) -> None:
    """FAIL-FIRST: a regular (orphan) dbt model materialized as
    ``stg_jaffle_shop__orders`` must be introspected and surfaced as a hidden
    SLayer dataset KEEPING the ``__`` name.

    Today ``introspect_table_to_model`` builds ``SlayerDataset(name=<__ name>)``,
    which raises; the converter's per-dataset try/except swallows it and drops the
    dataset with a ConversionWarning — so the dataset never reaches the output.
    """
    project = DbtProject(
        semantic_models=[],
        regular_models=[
            DbtRegularModel(
                name="stg_jaffle_shop__orders",
                schema_name=None,
                alias="stg_jaffle_shop__orders",
            ),
        ],
    )
    result = run_converter(
        DbtToSlayerConverter(project=project, data_source=DS, include_hidden_datasets=True),
        datasource=_datasource(sqlite_engine),
    )

    names = {m.name for m in result.datasets}
    assert "stg_jaffle_shop__orders" in names, (
        f"dunder-named hidden dataset was dropped instead of imported: {names}"
    )


# ---------------------------------------------------------------------------
# 2. dbt semantic model with a ``__`` name imports without aborting conversion
# ---------------------------------------------------------------------------


def test_dbt_semantic_model_dunder_name_imports() -> None:
    """FAIL-FIRST: a semantic model literally named ``stg_jaffle_shop__orders``
    must convert to a SLayer dataset with the exact ``__`` name.

    Today ``convert()`` builds ``SlayerDataset(name=sm.name)`` with no try/except
    around the semantic-model loop, so a ``__`` name raises a ValidationError
    that aborts the whole conversion.
    """
    project = DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="stg_jaffle_shop__orders",
                model="stg_jaffle_shop__orders",
                entities=[DbtEntity(name="order_id", type="primary", expr="id")],
                measures=[DbtMeasure(name="total", agg="sum", expr="amount")],
            ),
        ],
    )
    result = DbtToSlayerConverter(project=project, data_source=DS).convert()
    names = {m.name for m in result.datasets}
    assert "stg_jaffle_shop__orders" in names, (
        f"dunder-named semantic model not converted: {names}"
    )


# ---------------------------------------------------------------------------
# 3. Cube named ``foo__bar`` builds a dataset, not a PARSE_ERROR report entry
# ---------------------------------------------------------------------------


def test_cube_dunder_name_builds_model() -> None:
    """FAIL-FIRST: a cube literally named ``foo__bar`` must become a SLayer dataset
    named ``foo__bar``.

    Today ``_convert_cube`` wraps ``SlayerDataset(name=cube.name, ...)`` in a
    try/except that routes the ``__``-rejection to a PARSE_ERROR issue and drops
    the cube.
    """
    project = CubeProject(cubes=[CubeCube(
        name="foo__bar",
        sql_table="public.foo_bar",
        dimensions=[
            CubeDimension(name="id", sql="{CUBE}.id", type="number", primary_key=True),
        ],
    )])
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    names = {m.name for m in result.datasets}
    assert "foo__bar" in names, (
        f"dunder-named cube was dropped as a PARSE_ERROR instead of built: {names}"
    )


# ---------------------------------------------------------------------------
# INVARIANT LOCK — dbt's own Dimension('entity__dim') filter syntax
# ---------------------------------------------------------------------------


def test_invariant_dbt_dimension_filter_lowers_to_one_hop_dotted_ref() -> None:
    """INVARIANT LOCK: ``{{ Dimension('customer_id__status') }}`` is dbt INPUT
    syntax where ``__`` delimits entity from dimension — NOT a SLayer dataset name.
    It must keep lowering (via ``_DIMENSION_RE`` in slayer/dbt/filters.py) to a
    one-hop dotted ``customers.status`` reference. Unaffected by the ``__`` flip.
    """
    customers = DbtSemanticModel(
        name="customers", model="customers",
        entities=[DbtEntity(name="customer_id", type="primary", expr="id")],
        dimensions=[DbtDimension(name="status", type="categorical")],
    )
    orders = DbtSemanticModel(
        name="orders", model="orders",
        entities=[
            DbtEntity(name="order_id", type="primary", expr="id"),
            DbtEntity(name="customer_id", type="foreign"),
        ],
    )
    registry = EntityRegistry()
    registry.build([orders, customers])

    lowered = convert_dbt_filter(
        filter_str="{{ Dimension('customer_id__status') }} = 'active'",
        population_name="orders",
        entity_registry=registry,
        dataset_entity_names={"order_id": "primary", "customer_id": "foreign"},
        all_semantic_models={"orders": orders, "customers": customers},
    )
    assert lowered == "customers.status = 'active'", lowered
