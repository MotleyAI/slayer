"""DEV-1443 — filter / ORDER BY on raw colon-syntax measure formula must
resolve to the user-assigned alias when the same node renames the measure.

Bug: filter ``"operbotdetref:count_distinct >= 5"`` paired with a measure
``{"formula": "operbotdetref:count_distinct", "name": "robot_count"}`` was
rendered as ``WHERE per_robot_stats.operbotdetref_count_distinct >= 5``
(canonical alias) — but the inner SELECT exposes only
``per_robot_stats.robot_count`` (user alias), so the query failed at
execution. Expected: filter resolves to the user alias and surfaces as
``HAVING COUNT(DISTINCT operbotdetref) >= 5``.

Companion tickets surfaced during spec'ing:
* DEV-1445 — cross-model agg refs in filters with rename (deferred scope).
* DEV-1446 — transform-wrapped agg refs of a renamed measure produce a
  duplicate ``EnrichedMeasure`` (pre-existing dedup bug).
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator


async def _noop_async(**kw):
    return None


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


async def _generate(query: SlayerQuery, model: SlayerModel) -> str:
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    return SQLGenerator(dialect="postgres").generate(enriched=enriched)


async def _enrich(query: SlayerQuery, model: SlayerModel):
    return await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )


class TestFilterRenamedMeasureRemap:
    """The fix: filter `col:agg` with a same-node rename resolves to user alias."""

    async def test_filter_renamed_measure_resolves_to_alias(self) -> None:
        """Exact bug shape: filter uses the raw colon formula; same node
        renames the measure. The rendered SQL must apply the predicate to the
        actual aggregate (HAVING COUNT(DISTINCT customer_id) >= 5), NOT to a
        non-existent canonical alias `customer_id_count_distinct`.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
            filters=["customer_id:count_distinct >= 5"],
        )
        sql = await _generate(query, model)

        # Must surface the renamed alias in the projection.
        assert '"orders.num_customers"' in sql, (
            f"renamed alias must appear in projection:\n{sql}"
        )
        # Must NOT reference the canonical alias anywhere — that column is
        # never selected once the measure is renamed.
        assert "customer_id_count_distinct" not in sql, (
            f"canonical alias must not leak when measure is renamed:\n{sql}"
        )
        # Predicate must surface as HAVING against the actual aggregate.
        assert "HAVING" in sql, f"filter on aggregate must be HAVING:\n{sql}"
        assert "COUNT(DISTINCT" in sql

    async def test_filter_uses_alias_directly_still_works(self) -> None:
        """Regression: the documented workaround (`filter` references the
        user alias directly) keeps producing the same correct SQL.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
            filters=["num_customers >= 5"],
        )
        sql = await _generate(query, model)

        assert '"orders.num_customers"' in sql
        assert "HAVING" in sql
        assert "COUNT(DISTINCT" in sql
        assert "customer_id_count_distinct" not in sql

    async def test_filter_unrenamed_measure_uses_canonical(self) -> None:
        """Regression: when the measure is NOT renamed, filter on the colon
        form resolves to the canonical alias as before.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customer_id:count_distinct")],
            filters=["customer_id:count_distinct >= 5"],
        )
        sql = await _generate(query, model)

        assert "HAVING" in sql
        assert "COUNT(DISTINCT" in sql
        # Canonical alias surfaces in the projection (no rename).
        assert "customer_id_count_distinct" in sql

    async def test_filter_mixed_renamed_and_canonical(self) -> None:
        """Arithmetic filter combining a renamed colon ref and an unrenamed
        colon ref. The renamed half remaps; the unrenamed half stays
        canonical.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="amount:sum", name="revenue"),
                ModelMeasure(formula="customer_id:count_distinct"),
            ],
            filters=["amount:sum / customer_id:count_distinct > 100"],
        )
        sql = await _generate(query, model)

        # Both halves end up in the HAVING expression, but the renamed half
        # references the user alias (and the unrenamed half references the
        # canonical alias). The SQL must not contain ``amount_sum`` — the
        # rename consumed that slot.
        assert "HAVING" in sql, sql
        assert '"orders.revenue"' in sql
        # Canonical for `amount:sum` must not appear (it's renamed to revenue).
        assert "orders.amount_sum" not in sql, (
            f"renamed canonical leaked:\n{sql}"
        )
        # Canonical for the unrenamed measure surfaces.
        assert "customer_id_count_distinct" in sql

    async def test_filter_renamed_measure_classified_as_having_not_where(
        self,
    ) -> None:
        """Enrichment-level check: after the remap the ParsedFilter carries
        ``is_having=True``, not WHERE.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
            filters=["customer_id:count_distinct >= 5"],
        )
        enriched = await _enrich(query, model)
        # Exactly one parsed filter — and it must be HAVING.
        relevant = [f for f in enriched.filters]
        assert len(relevant) == 1
        f = relevant[0]
        assert f.is_having, (
            f"filter on a renamed aggregated measure must classify as HAVING, "
            f"got is_having={f.is_having!r}, is_post_filter={f.is_post_filter!r}, "
            f"columns={f.columns!r}, sql={f.sql!r}"
        )


class TestOrderByRenamedMeasureRemap:
    """ORDER BY on the raw colon form must remap to the user alias too."""

    async def test_order_by_renamed_measure_canonical_form(self) -> None:
        """``order=["customer_id:count_distinct desc"]`` with a renamed
        measure must resolve to the user alias, not to a non-existent
        canonical column.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
            order=[OrderItem(column="customer_id:count_distinct", direction="desc")],
        )
        sql = await _generate(query, model)

        assert "ORDER BY" in sql
        order_clause = sql.split("ORDER BY", 1)[1]
        assert '"orders.num_customers"' in order_clause, (
            f"renamed alias must surface in ORDER BY:\n{sql}"
        )
        assert "customer_id_count_distinct" not in order_clause, (
            f"canonical must not appear in ORDER BY when renamed:\n{sql}"
        )

    async def test_order_by_renamed_measure_alias_form(self) -> None:
        """Regression: ORDER BY referencing the user alias directly still works."""
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
            order=[OrderItem(column=ColumnRef(name="num_customers"), direction="desc")],
        )
        sql = await _generate(query, model)
        assert "ORDER BY" in sql
        assert '"orders.num_customers"' in sql.split("ORDER BY", 1)[1]


class TestRemapEdgeCases:
    """Codex F1 + F2 — corner cases around the remap."""

    async def test_filter_canonical_name_collides_with_source_column_skips_remap(
        self,
    ) -> None:
        """Codex F1: if a source column literally shares the canonical alias
        AND the filter uses both forms, the remap must NOT fire — that would
        clobber the literal source-column reference. Eligibility: remap only
        when the canonical is in ``pf.synthesized_aliases`` AND not in
        source-column names.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # Pathological: a real column whose name happens to equal
                # the canonical alias of a hypothetical ``amount:sum``.
                Column(name="amount_sum", sql="amount_sum", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum", name="revenue")],
            # Filter combines:
            #  - the colon form (would canonicalise to ``amount_sum``), AND
            #  - a literal reference to the source column also named
            #    ``amount_sum``.
            # The remap must skip the canonical because of the source-column
            # collision; the predicate retains ``amount_sum`` (or the
            # resolved qualified form), NOT the renamed alias ``revenue``.
            filters=["amount:sum > 100 or amount_sum > 0"],
        )
        # Stronger assertion (per Codex test-review): the parsed-filter SQL
        # must NOT have been remapped to the user alias. Inspect at the
        # enrichment level to avoid being fooled by generator-side qualifier
        # rewrites.
        enriched = await _enrich(query, model)
        predicate_sqls = [f.sql for f in enriched.filters]
        for predicate_sql in predicate_sqls:
            assert "revenue" not in predicate_sql, (
                f"remap fired despite source-column collision: predicate="
                f"{predicate_sql!r}, filters={predicate_sqls!r}"
            )
        # And the full rendered SQL still references the literal source
        # column.
        sql = await _generate(query, model)
        assert "amount_sum" in sql, sql

    async def test_model_level_filter_unaffected_by_query_measure_rename(
        self,
    ) -> None:
        """Codex F3 (test-review): pin the Mode B/DSL-only boundary. A
        ``SlayerModel.filters`` entry (Mode A SQL) referencing a token that
        happens to be the canonical-shape of a renamed query measure must
        NOT be remapped — model-side filters never carry synthesized
        canonical aliases and the remap pre-pass must not touch them.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # A real source column whose name happens to equal the
                # canonical of an unrelated query rename. Mode A model
                # filters reference this directly.
                Column(name="customer_id_count_distinct", sql="customer_id_count_distinct", type=DataType.DOUBLE),
            ],
            filters=["customer_id_count_distinct > 0"],  # Mode A SQL filter.
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                # Renames `customer_id:count_distinct` → `num_customers`.
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
        )
        enriched = await _enrich(query, model)
        # The model-level filter must surface untouched in the enriched
        # filter list — neither the SQL text nor the columns list should
        # have been rewritten to the user alias.
        model_filters = [
            f for f in enriched.filters if "customer_id_count_distinct" in f.sql
        ]
        assert model_filters, (
            f"model filter lost from enriched output: "
            f"{[f.sql for f in enriched.filters]!r}"
        )
        for f in model_filters:
            assert "num_customers" not in f.sql, (
                f"model-level (Mode A) filter must not be remapped to the "
                f"query measure alias: {f.sql!r}"
            )

    async def test_query_measure_name_collides_with_source_column_raises(
        self,
    ) -> None:
        """Codex F2: ``{"formula": "amount:sum", "name": "status"}`` where
        ``status`` is a source column must raise at enrichment with a clear
        message — otherwise alias-form filters silently bind to the source
        column instead of the renamed measure.
        """
        model = _orders_model()
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="id")],
            measures=[ModelMeasure(formula="amount:sum", name="status")],
        )
        with pytest.raises(ValueError, match=r"collides with a source column"):
            await _enrich(query, model)


class TestDeferredScopeGuards:
    """Guard tests for the deferred scope (DEV-1445, DEV-1446)."""

    @pytest.mark.skip(
        reason=(
            "DEV-1445: cross-model colon filter + rename is deferred scope. "
            "The DEV-1443 plan does not promise a specific failure mode "
            "(raise vs. broken SQL), only that the local case is fixed. "
            "Flip into a real coverage test when DEV-1445 lands."
        )
    )
    async def test_filter_cross_model_colon_syntax_deferred(self) -> None:
        """DEV-1445 placeholder. When DEV-1445 lands, assert that the
        cross-model filter resolves to the renamed alias and emits correct
        SQL (HAVING on the cross-model CTE's alias, no broken column ref).
        """
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="customers.revenue:sum", name="rev"),
            ],
            filters=["customers.revenue:sum >= 100"],
        )
        await _enrich(query, orders)

    @pytest.mark.skip(
        reason=(
            "DEV-1446: transform-wrapped agg ref of a renamed measure currently "
            "produces a duplicate EnrichedMeasure. Documenting current behaviour "
            "until DEV-1446 is fixed. Flip to a real assertion when DEV-1446 "
            "lands."
        )
    )
    async def test_transform_wrapped_inner_ref_of_renamed_measure_currently_duplicates(
        self,
    ) -> None:
        """DEV-1446 documenting test: filter ``change(col:sum) > 0`` with
        ``{"formula": "col:sum", "name": "user_alias"}`` should resolve the
        inner ref to the renamed measure (one aggregate in the CTE).
        Today it creates a second canonical EnrichedMeasure. When DEV-1446
        is fixed, flip this to an assertion that only one aggregate column
        for ``col:sum`` appears in the base CTE.
        """
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[ModelMeasure(formula="amount:sum", name="revenue")],
            filters=["change(amount:sum) > 0"],
        )
        sql = await _generate(query, model)
        # Should be exactly one SUM(amount) aggregation in the base CTE.
        sum_count = sql.upper().count("SUM(AMOUNT)")
        assert sum_count == 1, (
            f"DEV-1446: expected one SUM(amount) aggregation, got "
            f"{sum_count}:\n{sql}"
        )
