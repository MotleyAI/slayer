"""Producers set/carry join cardinality (DEV-1688).

* dbt: foreign->primary => many_to_one; peer (both unique) => one_to_one.
* dbt in-memory inner-join mirror inverts cardinality (Codex #6).
* OSI: relationship direction is from=many/to=one => many_to_one.
* Facade: FacadeJoin carries a cardinality passthrough.
"""

from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.entities import EntityRegistry
from slayer.dbt.models import DbtEntity, DbtProject, DbtSemanticModel
from slayer.facade.catalog import FacadeJoin, build_catalog, _facade_join_from
from slayer.facade.translator import translate
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path

FIXTURES = Path(__file__).parent / "fixtures" / "osi"

_OSI_SCHEMA = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
    "product_id INTEGER, amount REAL, quantity INTEGER, ordered_at DATE, status TEXT)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER, "
    "name TEXT, segment TEXT)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, category TEXT, price REAL)",
    "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name TEXT, population INTEGER)",
    "CREATE TABLE ckey_parent (k1 INTEGER, k2 INTEGER, label TEXT, PRIMARY KEY (k1, k2))",
    "CREATE TABLE ckey_child (k1 INTEGER, k2 INTEGER, v REAL)",
]


def _foreign_primary_project() -> DbtProject:
    return DbtProject(
        semantic_models=[
            DbtSemanticModel(
                name="orders",
                model="orders",
                entities=[
                    DbtEntity(name="order_id", type="primary", expr="id"),
                    DbtEntity(name="customer_id", type="foreign", expr="customer_id"),
                ],
            ),
            DbtSemanticModel(
                name="customers",
                model="customers",
                entities=[DbtEntity(name="customer_id", type="primary", expr="id")],
            ),
        ]
    )


# ---------------------------------------------------------------------------
# dbt entity-registry level
# ---------------------------------------------------------------------------


class TestDbtEntities:
    def test_foreign_to_primary_is_many_to_one(self) -> None:
        orders = DbtSemanticModel(
            name="orders",
            entities=[
                DbtEntity(name="order_id", type="primary", expr="id"),
                DbtEntity(name="customer_id", type="foreign", expr="customer_id"),
            ],
        )
        customers = DbtSemanticModel(
            name="customers",
            entities=[DbtEntity(name="customer_id", type="primary", expr="id")],
        )
        reg = EntityRegistry()
        reg.build([orders, customers])
        joins = reg.resolve_joins_for_model(orders)
        assert joins[0].cardinality is JoinCardinality.MANY_TO_ONE

    def test_peer_shared_unique_is_one_to_one(self) -> None:
        a = DbtSemanticModel(
            name="a", entities=[DbtEntity(name="shared", type="unique")]
        )
        b = DbtSemanticModel(
            name="b", entities=[DbtEntity(name="shared", type="unique")]
        )
        reg = EntityRegistry()
        reg.build([a, b])
        joins = reg.resolve_joins_for_model(a)
        peer = next(j for j in joins if j.target_model == "b")
        assert peer.cardinality is JoinCardinality.ONE_TO_ONE


# ---------------------------------------------------------------------------
# dbt converter (with the in-memory inner-join mirror)
# ---------------------------------------------------------------------------


class TestDbtConverterMirror:
    def test_forward_many_to_one(self) -> None:
        result = DbtToSlayerConverter(
            project=_foreign_primary_project(), data_source="test_db"
        ).convert()
        orders = next(m for m in result.models if m.name == "orders")
        fwd = next(j for j in orders.joins if j.target_model == "customers")
        assert fwd.cardinality is JoinCardinality.MANY_TO_ONE

    def test_reverse_mirror_inverts_to_one_to_many(self) -> None:
        result = DbtToSlayerConverter(
            project=_foreign_primary_project(), data_source="test_db"
        ).convert()
        customers = next(m for m in result.models if m.name == "customers")
        rev = next(j for j in customers.joins if j.target_model == "orders")
        # Reverse of many_to_one is one_to_many.
        assert rev.cardinality is JoinCardinality.ONE_TO_MANY

    def test_peer_mirror_stays_one_to_one(self) -> None:
        project = DbtProject(
            semantic_models=[
                DbtSemanticModel(
                    name="claim",
                    model="claim",
                    entities=[DbtEntity(name="claim_identifier", type="primary")],
                ),
                DbtSemanticModel(
                    name="claim_coverage",
                    model="claim_coverage",
                    entities=[DbtEntity(name="claim_identifier", type="primary")],
                ),
            ]
        )
        result = DbtToSlayerConverter(project=project, data_source="test").convert()
        claim = next(m for m in result.models if m.name == "claim")
        cov = next(m for m in result.models if m.name == "claim_coverage")
        assert (
            next(j for j in claim.joins if j.target_model == "claim_coverage").cardinality
            is JoinCardinality.ONE_TO_ONE
        )
        assert (
            next(j for j in cov.joins if j.target_model == "claim").cardinality
            is JoinCardinality.ONE_TO_ONE
        )


# ---------------------------------------------------------------------------
# OSI
# ---------------------------------------------------------------------------


@pytest.fixture
def osi_engine(tmp_path: Path) -> sa.Engine:
    engine = sa.create_engine(f"sqlite:///{tmp_path}/shop.db")
    with engine.connect() as conn:
        for ddl in _OSI_SCHEMA:
            conn.execute(sa.text(ddl))
        conn.commit()
    return engine


class TestOsi:
    def test_relationship_is_many_to_one(self, osi_engine: sa.Engine) -> None:
        doc = parse_osi_path(FIXTURES / "shop.yaml")[0]
        result = OsiToSlayerConverter(
            documents=[doc], data_source="testds", sa_engine=osi_engine
        ).convert()
        orders = {m.name: m for m in result.models}["orders"]
        cust_join = next(j for j in orders.joins if j.target_model == "customers")
        assert cust_join.cardinality is JoinCardinality.MANY_TO_ONE


# ---------------------------------------------------------------------------
# Facade
# ---------------------------------------------------------------------------


class TestFacade:
    def test_facade_join_model_accepts_cardinality(self) -> None:
        fj = FacadeJoin(
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
            cardinality=JoinCardinality.ONE_TO_ONE,
        )
        assert fj.cardinality is JoinCardinality.ONE_TO_ONE

    def test_facade_join_from_carries_cardinality(self) -> None:
        j = ModelJoin(
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )
        fj = _facade_join_from(join=j)
        assert fj.cardinality is JoinCardinality.MANY_TO_ONE


def _dyn_catalog():
    """orders (no configured join) + stores — forces the dynamic-join path."""
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
        ],
        joins=[],
    )
    stores = SlayerModel(
        name="stores",
        data_source="jaffle",
        sql_table="stores",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="tax_rate", type=DataType.DOUBLE),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, stores]})


def _metabase_join_sql(on_clause: str) -> str:
    return (
        'SELECT "Stores"."name" AS "Stores__name" '
        'FROM "public"."orders" '
        'LEFT JOIN (SELECT "public"."stores"."id" AS "id", '
        '"public"."stores"."name" AS "name", '
        '"public"."stores"."tax_rate" AS "tax_rate" '
        'FROM "public"."stores") AS "Stores" '
        f"ON {on_clause}"
    )


class TestFacadeDynamicJoin:
    def test_dynamic_join_many_to_one_when_target_unique(self) -> None:
        # ON joins to stores.id (the PK) -> target is unique -> many_to_one.
        sql = _metabase_join_sql('"public"."orders"."store_id" = "Stores"."id"')
        result = translate(sql=sql, catalog=_dyn_catalog(), dialect="postgres")
        ext = result.query.source_model
        assert ext.joins[0].cardinality is JoinCardinality.MANY_TO_ONE

    def test_dynamic_join_none_when_target_not_unique(self) -> None:
        # ON joins to stores.tax_rate (not PK/unique) -> undetermined -> None.
        sql = _metabase_join_sql('"public"."orders"."store_id" = "Stores"."tax_rate"')
        result = translate(sql=sql, catalog=_dyn_catalog(), dialect="postgres")
        ext = result.query.source_model
        assert ext.joins[0].cardinality is None

    def test_dynamic_join_none_for_composite_pk_member(self) -> None:
        """Joining ONE member of a composite PK does not make the target unique.

        Every member of a composite primary key carries ``primary_key=True``,
        but the join only constrains that single column, so the composite
        key's uniqueness does not carry — same subset rule as
        ``is_key_set_unique``.
        """
        orders = SlayerModel(
            name="orders",
            data_source="jaffle",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="store_id", type=DataType.INT),
            ],
            joins=[],
        )
        # Composite PK (id, region) — neither column is unique on its own.
        stores = SlayerModel(
            name="stores",
            data_source="jaffle",
            sql_table="stores",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
            ],
        )
        catalog = build_catalog(models_by_datasource={"jaffle": [orders, stores]})
        sql = (
            'SELECT "Stores"."name" AS "Stores__name" '
            'FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."stores"."id" AS "id", '
            '"public"."stores"."name" AS "name" '
            'FROM "public"."stores") AS "Stores" '
            'ON "public"."orders"."store_id" = "Stores"."id"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        ext = result.query.source_model
        assert ext.joins[0].cardinality is None

    def test_dynamic_join_many_to_one_for_non_pk_unique_target(self) -> None:
        """A non-PK column flagged ``unique`` is a solo uniqueness claim."""
        orders = SlayerModel(
            name="orders",
            data_source="jaffle",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="store_code", type=DataType.TEXT),
            ],
            joins=[],
        )
        stores = SlayerModel(
            name="stores",
            data_source="jaffle",
            sql_table="stores",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="code", type=DataType.TEXT, unique=True),
                Column(name="name", type=DataType.TEXT),
            ],
        )
        catalog = build_catalog(models_by_datasource={"jaffle": [orders, stores]})
        sql = (
            'SELECT "Stores"."name" AS "Stores__name" '
            'FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."stores"."code" AS "code", '
            '"public"."stores"."name" AS "name" '
            'FROM "public"."stores") AS "Stores" '
            'ON "public"."orders"."store_code" = "Stores"."code"'
        )
        result = translate(sql=sql, catalog=catalog, dialect="postgres")
        ext = result.query.source_model
        assert ext.joins[0].cardinality is JoinCardinality.MANY_TO_ONE
