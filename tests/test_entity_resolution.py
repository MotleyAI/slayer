"""Entity-resolution tests.

Resolves user-supplied entity strings (the same forms valid inside a
``SlayerQuery``) into the canonical ``<datasource>.<dataset>[.<leaf>]`` form
described in the spec, §3-4. The resolver also extracts entities from a
whole ``SlayerQuery`` for use by ``save_query`` and ``entity_search``.

The fixture mounts a multi-datasource layout exercising the leaf rule, the
priority list, the dataset-vs-field collision (Case A), the bare-field
ambiguity (Case B1), the literal-dotted form (Case C), and the
datasource-vs-dataset collision (Case D). Failing-test diagnostics quote
the offending segment so the resolver's error messages are useful when
they bubble up to MCP / CLI surfaces.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousDatasetError, EntityResolutionError
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, TimeDimension
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
    """A two-datasource layout with relationships, metrics, custom aggs, and the
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
        await s.save_dataset(
            SlayerDataset(
                name="orders",
                data_source="mydb",
                sql_table="orders",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                    DatasetField(name="status", sql="status", datatype=DataType.STRING),
                    DatasetField(
                        name="customer_id",
                        sql="customer_id",
                        datatype=DataType.FLOAT,
                    ),
                    DatasetField(
                        name="ordered_at",
                        sql="ordered_at",
                        datatype=DataType.DATETIME,
                    ),
                ],
                metrics=[
                    DatasetMetric(expression="sum(amount) / count(*)", name="aov"),
                ],
                aggregations=[
                    Aggregation(
                        name="weighted_score",
                        expression="SUM(amount * amount) / SUM(amount)",
                    ),
                ],
                relationships=[
                    DatasetRelationship(
                        to="customers",
                        from_columns=["customer_id"], to_columns=["id"],
                    ),
                ],
            )
        )
        # `customers` carries a field named "orders" — a dataset+field
        # collision used to exercise Case A.
        await s.save_dataset(
            SlayerDataset(
                name="customers",
                data_source="mydb",
                sql_table="customers",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                    DatasetField(name="name", sql="name", datatype=DataType.STRING),
                    DatasetField(
                        name="region_id",
                        sql="region_id",
                        datatype=DataType.FLOAT,
                    ),
                    DatasetField(
                        name="orders",
                        sql="orders_count",
                        datatype=DataType.FLOAT,
                    ),
                ],
                relationships=[
                    DatasetRelationship(
                        to="regions",
                        from_columns=["region_id"], to_columns=["id"],
                    ),
                ],
            )
        )
        await s.save_dataset(
            SlayerDataset(
                name="regions",
                data_source="mydb",
                sql_table="regions",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                    DatasetField(name="name", sql="name", datatype=DataType.STRING),
                ],
            )
        )
        # `invoices` shares the "amount" field name with `orders` — both
        # live in mydb (the priority winner), so bare "amount" is
        # ambiguous (Case B1).
        await s.save_dataset(
            SlayerDataset(
                name="invoices",
                data_source="mydb",
                sql_table="invoices",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        # ---------------------- other -----------------------
        await s.save_dataset(
            SlayerDataset(
                name="users",
                data_source="other",
                sql_table="users",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                    DatasetField(name="name", sql="name", datatype=DataType.STRING),
                ],
            )
        )
        # A dataset literally named "mydb" living in datasource "other" —
        # exercises Case D (first-segment is both a datasource and a
        # dataset).
        await s.save_dataset(
            SlayerDataset(
                name="mydb",
                data_source="other",
                sql_table="mydb_data",
                fields=[
                    DatasetField(
                        name="id",
                        sql="id",
                        datatype=DataType.FLOAT,
                        primary_key=True,
                    ),
                ],
            )
        )
        # mydb wins all bare-name disambiguations.
        await s.set_datasource_priority(["mydb", "other"])
        yield s


# ---------------------------------------------------------------------------
# resolve_entity — single-token resolution with no population context
# ---------------------------------------------------------------------------


class TestResolveEntityShape:
    async def test_returns_entity_resolution(
        self, storage: StorageBackend
    ) -> None:
        # "other" is a plain datasource — no dataset collides with the
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
        # No collision (no dataset+field collision warning fires here —
        # the bare "orders" candidate field lives on customers, but
        # the resolver should detect that, see test_case_a).

    async def test_case_a_model_column_collision_warns(
        self, storage: StorageBackend
    ) -> None:
        # "orders" exists as a dataset AND as a field on `customers`.
        # Dataset wins, but the warning helps users qualify if they meant
        # the field.
        result = await resolve_entity("orders", storage=storage)
        assert result.canonical_forms == ["mydb.orders"]
        assert any("field" in w.lower() for w in result.warnings), (
            f"expected Case A field-collision warning, got "
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
        # The error must name the candidate datasets so the caller can
        # qualify.
        assert "orders" in msg or "invoices" in msg

    async def test_bare_named_measure(self, storage: StorageBackend) -> None:
        # "aov" is a named DatasetMetric on mydb.orders, unique in mydb.
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
        # "mydb.orders" with mydb as datasource, orders as dataset.
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

    async def test_reverse_hop_join(self, storage: StorageBackend) -> None:
        # customers.orders.amount — orders declares the only edge; the
        # reverse hop resolves without a declared reverse relationship.
        result = await resolve_entity(
            "customers.orders.amount", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_dot_terminating_at_join_target(
        self, storage: StorageBackend
    ) -> None:
        # orders.customers (no further segment) → leaf is the dataset
        # itself.
        result = await resolve_entity("orders.customers", storage=storage)
        assert result.canonical_forms == ["mydb.customers"]

    async def test_unknown_column_on_known_model_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("orders.no_such_field", storage=storage)
        assert "no_such_field" in str(exc_info.value)

    async def test_unknown_model_in_known_datasource_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("mydb.no_such_dataset", storage=storage)
        assert "no_such_dataset" in str(exc_info.value)

    async def test_case_c_dotted_taken_literally(
        self, storage: StorageBackend
    ) -> None:
        # "customers.orders" — customers has a field "orders". The user
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
        # "mydb" is both a datasource (mydb) and a dataset (other.mydb).
        # Bare "mydb" → datasource wins.
        result = await resolve_entity("mydb", storage=storage)
        assert result.canonical_forms == ["mydb"]
        assert any(
            "datasource" in w.lower() and "dataset" in w.lower()
            for w in result.warnings
        ), f"expected Case D warning, got {result.warnings!r}"

    async def test_two_dot_first_seg_ds_wins(
        self, storage: StorageBackend
    ) -> None:
        # "mydb.orders.amount" — mydb is interpreted as datasource (not
        # the other.mydb dataset). Walks to mydb.orders → amount.
        result = await resolve_entity(
            "mydb.orders.amount", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]


class TestResolveEntityAggregationStripping:
    async def test_sum_strips_to_column(
        self, storage: StorageBackend
    ) -> None:
        # "sum(orders.amount)" canonicalises to mydb.orders.amount.
        result = await resolve_entity(
            "sum(orders.amount)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_weighted_avg_with_args_strips(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "weighted_avg(orders.amount, weight=amount)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]

    async def test_corr_with_args_strips(
        self, storage: StorageBackend
    ) -> None:
        result = await resolve_entity(
            "corr(orders.amount, other=amount)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders.amount"]


class TestResolveEntityStarCount:
    async def test_star_count_with_model_resolves_to_model(
        self, storage: StorageBackend
    ) -> None:
        # "count(orders.*)" → the dataset itself (per §3.1).
        result = await resolve_entity(
            "count(orders.*)", storage=storage
        )
        assert result.canonical_forms == ["mydb.orders"]

    async def test_star_count_without_context_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity("count(*)", storage=storage)
        assert "count(*)" in str(exc_info.value)

    async def test_star_count_with_source_model_uses_it(
        self, storage: StorageBackend
    ) -> None:
        orders = await storage.get_dataset("orders", data_source="mydb")
        result = await resolve_entity(
            "count(*)", storage=storage, source_dataset=orders
        )
        assert result.canonical_forms == ["mydb.orders"]

    async def test_star_with_non_count_aggregation_rejected(
        self, storage: StorageBackend
    ) -> None:
        """``sum(*)`` and similar must not silently collapse to the
        population dataset — only ``count`` is the valid wildcard form. The
        previous code accepted any aggregation, which would corrupt the
        canonical-entity index for memories tagged via a query that
        used a malformed wildcard."""
        orders = await storage.get_dataset("orders", data_source="mydb")
        for bad in ("sum(*)", "avg(*)"):
            with pytest.raises(EntityResolutionError) as exc_info:
                await resolve_entity(
                    bad, storage=storage, source_dataset=orders
                )
            assert bad in str(exc_info.value)

    async def test_model_star_with_non_count_aggregation_rejected(
        self, storage: StorageBackend
    ) -> None:
        for bad in ("sum(orders.*)", "avg(orders.*)"):
            with pytest.raises(EntityResolutionError) as exc_info:
                await resolve_entity(bad, storage=storage)
            assert bad in str(exc_info.value)


# ---------------------------------------------------------------------------
# resolve_entity with population context (used inside save_query /
# entity_search.query)
# ---------------------------------------------------------------------------


class TestResolveEntityWithSourceModel:
    async def test_bare_column_resolves_against_source_model_first(
        self, storage: StorageBackend
    ) -> None:
        # "amount" is ambiguous globally (Case B1) but unambiguous in
        # the context of population=orders.
        orders = await storage.get_dataset("orders", data_source="mydb")
        result = await resolve_entity(
            "amount", storage=storage, source_dataset=orders
        )
        assert result.canonical_forms == ["mydb.orders.amount"]
        assert result.warnings == []

    async def test_falls_back_to_global_when_not_on_source(
        self, storage: StorageBackend
    ) -> None:
        # "name" isn't on orders, but is on customers, regions, users.
        # In mydb (priority winner), "name" exists on customers and
        # regions → ambiguous.
        orders = await storage.get_dataset("orders", data_source="mydb")
        with pytest.raises(EntityResolutionError):
            await resolve_entity(
                "name", storage=storage, source_dataset=orders
            )


# ---------------------------------------------------------------------------
# extract_entities_from_query — walks dimensions / time_dims / metrics /
# filters / population / relationships.
# ---------------------------------------------------------------------------


class TestExtractEntitiesFromQuery:
    async def test_source_model_always_tagged(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders" in result.canonical_forms

    async def test_dimensions_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.status" in result.canonical_forms

    async def test_dotted_dimension_via_join_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="customers.name")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.customers.name" in result.canonical_forms
        # Intermediate "customers" dataset is not tagged as a separate
        # entity (the leaf rule discards intermediates).
        # The population tag for orders is independent.
        assert "mydb.customers" not in result.canonical_forms

    async def test_measure_formula_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="sum(orders.amount)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.amount" in result.canonical_forms

    async def test_time_dimension_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(
                    field="orders.ordered_at",
                    granularity=TimeGranularity.DAY,
                )
            ],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.ordered_at" in result.canonical_forms

    async def test_filter_columns_extracted(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["orders.status = 'paid'", "orders.amount > 100"],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert "mydb.orders.status" in result.canonical_forms
        assert "mydb.orders.amount" in result.canonical_forms

    async def test_filter_variable_placeholders_ignored(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["orders.status = '{status_val}'"],
            metrics=[QueryMetric(metric="count(orders.*)")],
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
            population="orders",
            dimensions=[QueryDimension(field="orders.amount")],
            filter=["orders.amount > 100"],
            metrics=[QueryMetric(metric="sum(orders.amount)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        assert (
            result.canonical_forms.count("mydb.orders.amount") == 1
        ), result.canonical_forms

    async def test_star_count_uses_source_model(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await extract_entities_from_query(q, storage=storage)
        # Pin the exact list — a regression that adds an extra entity
        # for ``count(*)`` (e.g. tagging ``*`` as its own thing) would
        # silently pass a membership check.
        assert result.canonical_forms == ["mydb.orders"]

    async def test_unknown_column_in_filter_raises(
        self, storage: StorageBackend
    ) -> None:
        q = SlayerQuery(
            population="orders",
            filter=["orders.nonexistent_col > 100"],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        with pytest.raises(EntityResolutionError):
            await extract_entities_from_query(q, storage=storage)

    async def test_funcstyle_joined_custom_agg_extracts_joined_column(
        self,
    ) -> None:
        # A functional custom aggregation defined on a
        # joined dataset (``rolling_avg(customers.score)`` where ``rolling_avg``
        # lives on ``customers``) parses natively to an AggCall candidate, so
        # the joined field (``customers.score``) ends up tagged — no
        # custom-aggregation registry walk involved.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(
                DatasourceConfig(name="dev1500", type="postgres", host="x")
            )
            await s.save_dataset(SlayerDataset(
                name="customers",
                data_source="dev1500",
                sql_table="customers",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="score", sql="score", datatype=DataType.FLOAT),
                ],
                aggregations=[
                    Aggregation(name="rolling_avg", expression="AVG({value})"),
                ],
            ))
            await s.save_dataset(SlayerDataset(
                name="orders",
                data_source="dev1500",
                sql_table="orders",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
                ],
                relationships=[
                    DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
                ],
            ))
            q = SlayerQuery(
                population="orders",
                metrics=[QueryMetric(metric="rolling_avg(customers.score)")],
            )
            result = await extract_entities_from_query(q, storage=s)
            # The joined field inside the transform surfaces as an entity
            # (parsed natively — no funcstyle rewrite).
            assert "dev1500.customers.score" in result.canonical_forms, (
                result.canonical_forms
            )


class TestResolveEntityAmbiguousModel:
    async def test_ambiguous_model_propagates_when_no_priority(
        self,
    ) -> None:
        # Build a fixture without a priority list and with the SAME
        # dataset name in two datasources.
        with tempfile.TemporaryDirectory() as tmpdir:
            s = YAMLStorage(base_dir=tmpdir)
            await s.save_datasource(
                DatasourceConfig(name="dsa", type="postgres", host="x")
            )
            await s.save_datasource(
                DatasourceConfig(name="dsb", type="postgres", host="x")
            )
            await s.save_dataset(
                SlayerDataset(
                    name="orders",
                    data_source="dsa",
                    sql_table="o",
                    fields=[
                        DatasetField(
                            name="id",
                            sql="id",
                            datatype=DataType.FLOAT,
                            primary_key=True,
                        )
                    ],
                )
            )
            await s.save_dataset(
                SlayerDataset(
                    name="orders",
                    data_source="dsb",
                    sql_table="o",
                    fields=[
                        DatasetField(
                            name="id",
                            sql="id",
                            datatype=DataType.FLOAT,
                            primary_key=True,
                        )
                    ],
                )
            )
            with pytest.raises(AmbiguousDatasetError):
                await resolve_entity("orders", storage=s)


class TestResolveEntityUnknownDatasource:
    async def test_first_segment_unknown_treated_as_model(
        self, storage: StorageBackend
    ) -> None:
        # "no_such_ds.orders.amount" — first segment isn't a known
        # datasource, so resolve "no_such_ds" as a dataset in some
        # datasource. None has it → error.
        with pytest.raises(EntityResolutionError) as exc_info:
            await resolve_entity(
                "no_such_ds.orders.amount", storage=storage
            )
        assert "no_such_ds" in str(exc_info.value)
