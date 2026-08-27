"""DEV-1743 (category 8 / WP7): importers must PRESERVE ``__`` model names.

The dotted-canonical flip lifts the ban on ``__`` in model names. Today every
importer either DROPS or RENAMES an object whose name contains ``__`` (because
``SlayerModel(name="a__b")`` raises at construction). After the flip they must
carry the ``__`` name through verbatim.

Each FAIL-FIRST test below fails TODAY for a feature reason (the ``__`` name is
rejected → the object is dropped / the conversion aborts). The one INVARIANT
LOCK test guards a shape that must NOT change: dbt's own ``Dimension('e__d')``
input-filter syntax (where ``__`` is dbt's entity/dimension delimiter, not a
SLayer model name) must keep lowering to a one-hop dotted ref.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa

# ── dbt ──────────────────────────────────────────────────────────────────────
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

# ── osi ──────────────────────────────────────────────────────────────────────
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import (
    OSIDataset,
    OSIDialectExpression,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSISemanticModel,
)

# Shared fixtures (calling a ``__``-named builder raises TODAY — the fail signal).
from tests._dev1743_fixtures import chain_models

DS = "test"


# ---------------------------------------------------------------------------
# Live-DB fixture (dbt hidden-model import + OSI both introspect a real table)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path) -> sa.Engine:
    engine = sa.create_engine(f"sqlite:///{tmp_path}/live.db")
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
    return engine


def _osi_expr(sql: str) -> OSIExpression:
    return OSIExpression(
        dialects=[OSIDialectExpression(dialect="ANSI_SQL", expression=sql)]
    )


def _osi_doc(datasets: list[OSIDataset]) -> OSIDocument:
    return OSIDocument(
        version="0.2.0.dev0",
        semantic_model=[OSISemanticModel(name="s", datasets=datasets)],
    )


# ---------------------------------------------------------------------------
# 1. dbt hidden regular model with a ``__`` name is imported, not dropped
# ---------------------------------------------------------------------------


def test_dbt_hidden_regular_model_dunder_name_is_imported(
    sqlite_engine: sa.Engine,
) -> None:
    """FAIL-FIRST: a regular (orphan) dbt model materialized as
    ``stg_jaffle_shop__orders`` must be introspected and surfaced as a hidden
    SLayer model KEEPING the ``__`` name.

    Today ``introspect_table_to_model`` builds ``SlayerModel(name=<__ name>)``,
    which raises; the converter's per-model try/except swallows it and drops the
    model with a ConversionWarning — so the model never reaches the output.
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
    result = DbtToSlayerConverter(
        project=project,
        data_source=DS,
        include_hidden_models=True,
        sa_engine=sqlite_engine,
    ).convert()

    names = {m.name for m in result.models}
    assert "stg_jaffle_shop__orders" in names, (
        f"dunder-named hidden model was dropped instead of imported: {names}"
    )


# ---------------------------------------------------------------------------
# 2. dbt semantic model with a ``__`` name imports without aborting conversion
# ---------------------------------------------------------------------------


def test_dbt_semantic_model_dunder_name_imports() -> None:
    """FAIL-FIRST: a semantic model literally named ``stg_jaffle_shop__orders``
    must convert to a SLayer model with the exact ``__`` name.

    Today ``convert()`` builds ``SlayerModel(name=sm.name)`` with no try/except
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
    names = {m.name for m in result.models}
    assert "stg_jaffle_shop__orders" in names, (
        f"dunder-named semantic model not converted: {names}"
    )


# ---------------------------------------------------------------------------
# 3. Cube named ``foo__bar`` builds a model, not a PARSE_ERROR report entry
# ---------------------------------------------------------------------------


def test_cube_dunder_name_builds_model() -> None:
    """FAIL-FIRST: a cube literally named ``foo__bar`` must become a SLayer model
    named ``foo__bar``.

    Today ``_convert_cube`` wraps ``SlayerModel(name=cube.name, ...)`` in a
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
    names = {m.name for m in result.models}
    assert "foo__bar" in names, (
        f"dunder-named cube was dropped as a PARSE_ERROR instead of built: {names}"
    )


# ---------------------------------------------------------------------------
# 4. OSI dataset named ``foo__bar`` is accepted, not skipped as illegal_name
# ---------------------------------------------------------------------------


def test_osi_dunder_dataset_name_is_accepted(sqlite_engine: sa.Engine) -> None:
    """FAIL-FIRST: an OSI dataset literally named ``foo__bar`` must become a
    SLayer model named ``foo__bar``.

    Today ``_UNSAFE_MODEL_NAME_CHARS`` lists ``"__"``, so ``_legal_model_name``
    rejects it and ``_build_model`` skips the dataset with an illegal_name
    warning.
    """
    doc = _osi_doc(datasets=[
        OSIDataset(
            name="foo__bar",
            source="orders",
            fields=[OSIField(name="amount", expression=_osi_expr("amount"))],
        ),
    ])
    result = OsiToSlayerConverter(
        documents=[doc], data_source=DS, sa_engine=sqlite_engine
    ).convert()
    names = {m.name for m in result.models}
    assert "foo__bar" in names, (
        f"dunder-named OSI dataset was skipped as illegal instead of built: {names}"
    )


# ---------------------------------------------------------------------------
# 5. OSI strict-D2 alias resolution: a legacy split-alias qualifier
#    ``customers__regions`` (no such model) must NOT silently walk customers→regions
# ---------------------------------------------------------------------------


def test_osi_walk_join_alias_does_not_split_legacy_dunder_chain(
    sqlite_engine: sa.Engine,
) -> None:
    """FAIL-FIRST (strict D2 / P1): under the dotted-canonical flip a ``__``
    qualifier means an EXACT model name, never a join-path chain. So
    ``customers__regions`` — with NO model literally named ``customers__regions``
    — must be UNRESOLVABLE (``None``), even though a naive ``__`` split could
    walk ``orders → customers → regions``.

    Today ``OsiToSlayerConverter._walk_join_alias`` splits on ``__`` and returns
    the terminal ``regions`` model, silently resolving a reference that should
    error/warn as unresolvable.

    Tested directly on ``_walk_join_alias`` (the OSI Column.sql overlay path that
    reaches it is delicate to stage); this is the exact function DEV-1743 must
    make strict (slayer/osi/converter.py ``_walk_join_alias``).
    """
    conv = OsiToSlayerConverter(
        documents=[], data_source=DS, sa_engine=sqlite_engine
    )
    conv._models = {m.name: m for m in chain_models()}  # orders → customers → regions
    orders = conv._models["orders"]

    resolved = conv._walk_join_alias(host=orders, alias="customers__regions")
    assert resolved is None, (
        "legacy '__' split silently resolved customers__regions to a chain; "
        f"strict-D2 must treat it as an (absent) exact model name, got {resolved!r}"
    )


# ---------------------------------------------------------------------------
# INVARIANT LOCK — dbt's own Dimension('entity__dim') filter syntax
# ---------------------------------------------------------------------------


def test_invariant_dbt_dimension_filter_lowers_to_one_hop_dotted_ref() -> None:
    """INVARIANT LOCK: ``{{ Dimension('customer_id__status') }}`` is dbt INPUT
    syntax where ``__`` delimits entity from dimension — NOT a SLayer model name.
    It must keep lowering (via ``_DIMENSION_RE`` in slayer/dbt/filters.py) to a
    one-hop dotted ``customers.status`` reference. Unaffected by DEV-1743.
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
        source_model_name="orders",
        entity_registry=registry,
        model_entity_names={"order_id": "primary", "customer_id": "foreign"},
        all_semantic_models={"orders": orders, "customers": customers},
    )
    assert lowered == "customers.status = 'active'", lowered
