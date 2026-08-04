"""Entity-resolution tests for DEV-1357.

Resolves user-supplied entity strings (the same forms valid inside a
``SlayerQuery``) into the canonical ``<datasource>.<model>[.<leaf>]`` form
described in the spec, §3-4. The resolver also extracts entities from a
whole ``SlayerQuery`` for use by ``save_query`` and ``entity_search``.

The fixture mounts a multi-datasource layout exercising the leaf rule, the
priority list, the model-vs-column collision (Case A), the bare-column
ambiguity (Case B1), the literal-dotted form (Case C), and the
datasource-vs-model collision (Case D). Failing-test diagnostics quote
the offending segment so the resolver's error messages are useful when
they bubble up to MCP / CLI surfaces.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.enums import TimeGranularity
from slayer.memories.resolver import (
    EntityResolution,
    extract_entities_from_query,
    resolve_entity,
)
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    """A two-datasource layout with joins, measures, custom aggs, and the
    name-shape collisions called out in the spec's resolution cases.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)

        await s.save_datasource(
            DatasourceConfig(name="mydb", type="postgres", host="x")
        )
        await s.save_datasource(
            DatasourceConfig(name="other", type="postgres", host="x")
        )
        # ----------------------- mydb -----------------------
        await s.save_model(
            SlayerModel(
                name="orders",
                data_source="mydb",
                sql_table="orders",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                    Column(name="status", sql="status", type=DataType.TEXT),
                    Column(
                        name="customer_id",
                        sql="customer_id",
                        type=DataType.DOUBLE,
                    ),
                    Column(
                        name="ordered_at",
                        sql="ordered_at",
                        type=DataType.TIMESTAMP,
                    ),
                ],
                measures=[
                    ModelMeasure(formula="amount:sum / *:count", name="aov"),
                ],
                aggregations=[
                    Aggregation(
                        name="weighted_score",
                        formula="SUM(amount * amount) / SUM(amount)",
                    ),
                ],
                joins=[
                    ModelJoin(
                        target_model="customers",
                        join_pairs=[["customer_id", "id"]],
                    ),
                ],
            )
        )
        # `customers` carries a column named "orders" — a model+column
        # collision used to exercise Case A.
        await s.save_model(
            SlayerModel(
                name="customers",
                data_source="mydb",
                sql_table="customers",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(name="name", sql="name", type=DataType.TEXT),
                    Column(
                        name="region_id",
                        sql="region_id",
                        type=DataType.DOUBLE,
                    ),
                    Column(
                        name="orders",
                        sql="orders_count",
                        type=DataType.DOUBLE,
                    ),
                ],
                joins=[
                    ModelJoin(
                        target_model="regions",
                        join_pairs=[["region_id", "id"]],
                    ),
                ],
            )
        )
        await s.save_model(
            SlayerModel(
                name="regions",
                data_source="mydb",
                sql_table="regions",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(name="name", sql="name", type=DataType.TEXT),
                ],
            )
        )
        # `invoices` shares the "amount" column name with `orders` — both
        # live in mydb (the priority winner), so bare "amount" is
        # ambiguous (Case B1).
        await s.save_model(
            SlayerModel(
                name="invoices",
                data_source="mydb",
                sql_table="invoices",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        # ---------------------- other -----------------------
        await s.save_model(
            SlayerModel(
                name="users",
                data_source="other",
                sql_table="users",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(name="name", sql="name", type=DataType.TEXT),
                ],
            )
        )
        # A model literally named "mydb" living in datasource "other" —
        # exercises Case D (first-segment is both a datasource and a
        # model).
        await s.save_model(
            SlayerModel(
                name="mydb",
                data_source="other",
                sql_table="mydb_data",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                ],
            )
        )
        # mydb wins all bare-name disambiguations.
        await s.set_datasource_priority(["mydb", "other"])
        yield s


# ---------------------------------------------------------------------------
# resolve_entity — single-token resolution with no source-model context
# ---------------------------------------------------------------------------


class TestResolveEntityShape:
    async def test_returns_entity_resolution(
        self, storage: StorageBackend
    ) -> None:
        # "other" is a plain datasource — no model collides with the
        # name, so this exercises the bare ``EntityResolution`` shape
        # without firing any Case warnings.
        result = await resolve_entity("other", storage=storage)
        assert isinstance(result, EntityResolution)
        assert result.canonical_forms == ["other"]
        assert result.warnings == []


class TestResolveEntityBare:
    async def test_bare_model_name(self, storage: StorageBackend) -> None:
        result = await resolve_entity("orders", storage=storage)
        assert result.canonical_forms == ["mydb.orders"]
        # No collision (no model+column collision warning fires here —
        # the bare "orders" candidate column lives on customers, but
        # the resolver should detect that, see test_case_a).

    async def test_case_a_model_column_collision_warns(
        self, storage: StorageBackend
    ) -> None:
        # "orders" exists as a model AND as a column on `customers`.
        # Model wins, but the warning helps users qualify if they meant
        # the column.
        result = await resolve_entity("orders", storage=storage)
        assert result.canonical_forms == ["mydb.orders"]
        assert any("column" in w.lower() for w in result.warnings), (
            f"expected Case A column-collision warning, got "
            f"{result.warnings!r}"
        )

    async def test_bare_column_unique_in_priority_winner(
        self, storage: StorageBackend
    ) -> None:
        # "status" only exists on mydb.orders → unambiguous.
        result = await resolve_entity("status", storage=storage)
        assert result.canonical_forms == ["mydb.orders.status"]

    async def test_case_b1_bare_column_ambiguous_raises(
        self, storage: StorageBackend
    ) -> None:
        # "amount" is on both mydb.orders and mydb.invoices.
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("amount", storage=storage)
        msg = str(exc_info.value)
        assert "amount" in msg
        # The error must name the candidate models so the caller can
        # qualify.
        assert "orders" in msg or "invoices" in msg

    async def test_bare_named_measure(self, storage: StorageBackend) -> None:
        # "aov" is a named ModelMeasure on mydb.orders, unique in mydb.
        result = await resolve_entity("aov", storage=storage)
        assert result.canonical_forms == ["mydb.orders.aov"]

    async def test_bare_custom_aggregation(
        self, storage: StorageBackend
    ) -> None:
        # "weighted_score" is a custom Aggregation on mydb.orders,
        # unique in mydb.
        result = await resolve_entity("weighted_score", storage=storage)
        assert result.canonical_forms == ["mydb.orders.weighted_score"]

    async def test_bare_unknown_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("nonexistent_thing", storage=storage)
        assert "nonexistent_thing" in str(exc_info.value)


class TestResolveEntityDotted:
    async def test_one_dot_model_column(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity("orders.amount", storage=storage)
        assert result.canonical_forms == ["mydb.orders.amount"]
        # Dotted forms never emit Case A warnings.
        assert result.warnings == []

    async def test_one_dot_named_measure(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity("orders.aov", storage=storage)
        assert result.canonical_forms == ["mydb.orders.aov"]

    async def test_one_dot_custom_aggregation(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "orders.weighted_score", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.weighted_score"]

    async def test_one_dot_datasource_model(
        self, storage: StorageBackend
    ) -> None:
        # "mydb.orders" with mydb as datasource, orders as model.
        result = await resolve_entity("mydb.orders", storage=storage)
        assert result.canonical_forms == ["mydb.orders"]

    async def test_hyphenated_datasource_segment_accepted(
        self, storage: StorageBackend
    ) -> None:
        # A datasource name may embed a UUID (e.g. ``source_019f4323-cb59-...``),
        # so the identifier guard must accept a hyphenated segment. Resolution
        # then proceeds and fails on not-found — never on a rejected segment.
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity(
                "source_019f4323-cb59.nonexistent_model.amount", storage=storage
            )
        assert "invalid identifier segment" not in str(exc_info.value)

    async def test_two_dot_datasource_model_column(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "mydb.orders.amount", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_two_dot_one_hop_join(
        self, storage: StorageBackend
    ) -> None:
        # orders.customers.name → leaf is on customers (mydb),
        # intermediates discarded.
        result = await resolve_entity(
            "orders.customers.name", storage=storage
        )
        assert result.canonical_forms == ["mydb.customers.name"]

    async def test_three_dot_two_hop_join(
        self, storage: StorageBackend
    ) -> None:
        # orders.customers.regions.name → leaf is on regions, all
        # intermediates discarded per the leaf rule (§3.2).
        result = await resolve_entity(
            "orders.customers.regions.name", storage=storage
        )
        assert result.canonical_forms == ["mydb.regions.name"]

    async def test_dot_terminating_at_join_target(
        self, storage: StorageBackend
    ) -> None:
        # orders.customers (no further segment) → leaf is the model
        # itself.
        result = await resolve_entity("orders.customers", storage=storage)
        assert result.canonical_forms == ["mydb.customers"]

    async def test_unknown_column_on_known_model_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("orders.no_such_column", storage=storage)
        assert "no_such_column" in str(exc_info.value)

    async def test_unknown_model_in_known_datasource_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("mydb.no_such_model", storage=storage)
        assert "no_such_model" in str(exc_info.value)

    async def test_case_c_dotted_taken_literally(
        self, storage: StorageBackend
    ) -> None:
        # "customers.orders" — customers has a column "orders". The user
        # qualified, so the resolver takes it literally; no warning.
        result = await resolve_entity(
            "customers.orders", storage=storage
        )
        assert result.canonical_forms == ["mydb.customers.orders"]
        assert result.warnings == []


class TestResolveEntityCaseDDatasourceModelCollision:
    async def test_first_segment_ds_wins_with_warning(
        self, storage: StorageBackend
    ) -> None:
        # "mydb" is both a datasource (mydb) and a model (other.mydb).
        # Bare "mydb" → datasource wins.
        result = await resolve_entity("mydb", storage=storage)
        assert result.canonical_forms == ["mydb"]
        assert any(
            "datasource" in w.lower() and "model" in w.lower()
            for w in result.warnings
        ), f"expected Case D warning, got {result.warnings!r}"

    async def test_two_dot_first_seg_ds_wins(
        self, storage: StorageBackend
    ) -> None:
        # "mydb.orders.amount" — mydb is interpreted as datasource (not
        # the other.mydb model). Walks to mydb.orders → amount.
        result = await resolve_entity(
            "mydb.orders.amount", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]


class TestResolveEntityAggregationStripping:
    async def test_colon_sum_strips_to_column(
        self, storage: StorageBackend
    ) -> None:
        # "orders.amount:sum" canonicalises to mydb.orders.amount.
        result = await resolve_entity(
            "orders.amount:sum", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_weighted_avg_with_args_strips(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "orders.amount:weighted_avg(weight=amount)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_corr_with_args_strips(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "orders.amount:corr(other=amount)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]


class TestResolveEntityStarCount:
    async def test_star_count_with_model_resolves_to_model(
        self, storage: StorageBackend
    ) -> None:
        # "orders.*:count" → the model itself (per §3.1).
        result = await resolve_entity(
            "orders.*:count", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders"]

    async def test_star_count_without_context_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("*:count", storage=storage)
        assert "*:count" in str(exc_info.value)

    async def test_star_count_with_source_model_uses_it(
        self, storage: StorageBackend
    ) -> None:
        orders = await storage.get_model("orders", data_source="mydb")
        result = await resolve_entity(
            "*:count", storage=storage, source_model=orders
        )
        assert result.canonical_forms == ["mydb.orders"]

    async def test_star_with_non_count_aggregation_rejected(
        self, storage: StorageBackend
    ) -> None:
        """``*:sum`` and similar must not silently collapse to the
        source model — only ``count`` is the valid wildcard form. The
        previous code accepted any aggregation, which would corrupt the
        canonical-entity index for memories tagged via a query that
        used a malformed wildcard."""
        orders = await storage.get_model("orders", data_source="mydb")
        for bad in ("*:sum", "*:avg"):
            with pytest.raises(EntityResolutionError) as exc_info:
                await resolve_entity(
                    bad, storage=storage, source_model=orders
                )
            assert bad in str(exc_info.value)

    async def test_model_star_with_non_count_aggregation_rejected(
        self, storage: StorageBackend
    ) -> None:
        for bad in ("orders.*:sum", "orders.*:avg"):
            with pytest.raises(EntityResolutionError) as exc_info:
                await resolve_entity(bad, storage=storage)
            assert bad in str(exc_info.value)


# ---------------------------------------------------------------------------
# resolve_entity with source-model context (used inside save_query /
# entity_search.query)
# ---------------------------------------------------------------------------


class TestResolveEntityWithSourceModel:
    async def test_bare_column_resolves_against_source_model_first(
        self, storage: StorageBackend
    ) -> None:
        # "amount" is ambiguous globally (Case B1) but unambiguous in
        # the context of source_model=orders.
        orders = await storage.get_model("orders", data_source="mydb")
        result = await resolve_entity(
            "amount", storage=storage, source_model=orders
        )
        assert result.canonical_forms == ["mydb.orders.amount"]
        assert result.warnings == []

    async def test_falls_back_to_global_when_not_on_source(
        self, storage: StorageBackend
    ) -> None:
        # "name" isn't on orders, but is on customers, regions, users.
        # In mydb (priority winner), "name" exists on customers and
        # regions → ambiguous.
        orders = await storage.get_model("orders", data_source="mydb")
        with pytest.raises(EntityResolutionError):
            await resolve_entity(
                "name", storage=storage, source_model=orders
            )


# ---------------------------------------------------------------------------
# extract_entities_from_query — walks dimensions / time_dims / measures /
# filters / source_model / joins.
# ---------------------------------------------------------------------------


class TestExtractEntitiesFromQuery:
    async def test_source_model_always_tagged(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders" in result.canonical_forms

    async def test_dimensions_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.status" in result.canonical_forms

    async def test_dotted_dimension_via_join_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.name")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.customers.name" in result.canonical_forms
        # Intermediate "customers" model is not tagged as a separate
        # entity (the leaf rule discards intermediates).
        # The source model tag for orders is independent.
        assert "mydb.customers" not in result.canonical_forms

    async def test_measure_formula_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms

    async def test_time_dimension_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="ordered_at"),
                    granularity=TimeGranularity.DAY,
                )
            ],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.ordered_at" in result.canonical_forms

    async def test_filter_columns_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["status = 'paid'", "amount > 100"],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.status" in result.canonical_forms
        assert "mydb.orders.amount" in result.canonical_forms

    async def test_filter_variable_placeholders_ignored(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["status = '{status_val}'"],
            measures=[ModelMeasure(formula="*:count")],
            variables={"status_val": "paid"},
        )
        result = await extract_entities_from_query(q, storage=storage)
        # `status` extracted; `{status_val}` ignored as a literal.
        assert "mydb.orders.status" in result.canonical_forms
        assert not any(
            "status_val" in c for c in result.canonical_forms
        )

    async def test_dedup(self, storage: StorageBackend) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="amount")],
            filters=["amount > 100"],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert (
            result.canonical_forms.count("mydb.orders.amount") == 1
        ), result.canonical_forms

    async def test_star_count_uses_source_model(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        # Pin the exact list — a regression that adds an extra entity
        # for ``*:count`` (e.g. tagging ``*`` as its own thing) would
        # silently pass a membership check.
        assert result.canonical_forms == ["mydb.orders"]

    async def test_unknown_column_in_filter_raises(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            source_model="orders",
            filters=["nonexistent_col > 100"],
            measures=[ModelMeasure(formula="*:count")],
        )
        with pytest.raises(EntityResolutionError):
            await extract_entities_from_query(q, storage=storage)


class TestResolveEntityAmbiguousModel:
    async def test_ambiguous_model_propagates_when_no_priority(
        self,
    ) -> None:
        # Build a fixture without a priority list and with the SAME
        # model name in two datasources.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(
                DatasourceConfig(name="dsa", type="postgres", host="x")
            )
            await s.save_datasource(
                DatasourceConfig(name="dsb", type="postgres", host="x")
            )
            await s.save_model(
                SlayerModel(
                    name="orders",
                    data_source="dsa",
                    sql_table="o",
                    columns=[
                        Column(
                            name="id",
                            sql="id",
                            type=DataType.DOUBLE,
                            primary_key=True,
                        )
                    ],
                )
            )
            await s.save_model(
                SlayerModel(
                    name="orders",
                    data_source="dsb",
                    sql_table="o",
                    columns=[
                        Column(
                            name="id",
                            sql="id",
                            type=DataType.DOUBLE,
                            primary_key=True,
                        )
                    ],
                )
            )
            with pytest.raises(AmbiguousModelError):
                await resolve_entity("orders", storage=s)


class TestResolveEntityUnknownDatasource:
    async def test_first_segment_unknown_treated_as_model(
        self, storage: StorageBackend
    ) -> None:
        # "no_such_ds.orders.amount" — first segment isn't a known
        # datasource, so resolve "no_such_ds" as a model in some
        # datasource. None has it → error.
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity(
                "no_such_ds.orders.amount", storage=storage
            )
        assert "no_such_ds" in str(exc_info.value)
