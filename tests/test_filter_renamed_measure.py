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
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery

from tests._engine_helpers import _engine_generate, _having_text, _where_text


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
    return await _engine_generate(query, model)


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
        """After the remap, the aggregate predicate must route to HAVING,
        not WHERE. Asserted on the emitted SQL: the ``>= 5`` predicate over
        ``COUNT(DISTINCT ...)`` lands in the HAVING clause and the WHERE
        clause carries no such predicate.
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
        having = _having_text(sql)
        where = _where_text(sql)
        assert "COUNT(DISTINCT" in having.upper(), (
            f"aggregate predicate must be in HAVING:\nHAVING={having!r}\nSQL:\n{sql}"
        )
        assert ">= 5" in having, f"predicate value missing from HAVING:\n{sql}"
        # There is no row-level predicate in this query, so WHERE must be
        # empty — the aggregate filter routes entirely to HAVING.
        assert where == "", (
            f"aggregate predicate must not produce a WHERE clause:\nWHERE={where!r}\nSQL:\n{sql}"
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

    async def test_remap_does_not_rewrite_quoted_string_literal(
        self,
    ) -> None:
        """DEV-1443 (CodeRabbit thread 2): the filter remap is identifier-
        level only — canonical-name matches inside quoted string literals
        must NOT be rewritten. Example: filter ``country:first = 'country_first'``
        with the measure renamed to ``primary_country`` must remap the
        first occurrence (the alias) but leave the literal
        ``'country_first'`` alone, otherwise the filter compares the
        renamed-alias column to itself instead of the original constant.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="country", sql="country", type=DataType.TEXT),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="country:first", name="primary_country"),
            ],
            filters=["country:first = 'country_first'"],
        )
        sql = await _generate(query, model)
        # The literal 'country_first' must still appear in the emitted
        # predicate — the alias remap must not have rewritten it. (The
        # ``first`` aggregate routes the predicate to HAVING.)
        assert "'country_first'" in sql, (
            f"remap clobbered the literal 'country_first':\n{sql}"
        )

    @pytest.mark.parametrize(
        ("formula_a", "formula_b"),
        [
            # Two plain colon-syntax aggregates — Codex round 3 finding.
            ("amount:sum", "profit:avg"),
            # Two arithmetic / non-aggregate measures — CodeRabbit + Codex
            # round 4: the duplicate-name check must cover all measure
            # kinds, not just the local AggregatedMeasureRef rename branch.
            ("amount:sum / 100", "profit:avg * 2"),
        ],
        ids=["plain_agg", "arithmetic"],
    )
    async def test_duplicate_explicit_name_across_measures_raises(
        self,
        formula_a: str,
        formula_b: str,
    ) -> None:
        """DEV-1443 (CodeRabbit + Codex review rounds 3-4 on PR #133): two
        query measures sharing the same explicit ``name`` would otherwise
        silently collapse to one alias, making filter/ORDER BY refs bind
        to whichever measure was processed first. The pre-pass validator
        must reject the collision regardless of measure shape — plain
        colon aggregates, cross-model aggregates, or arithmetic /
        transform expressions.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="profit", sql="profit", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula=formula_a, name="metric"),
                ModelMeasure(formula=formula_b, name="metric"),
            ],
        )
        with pytest.raises(ValueError, match=r"declared more than once"):
            await _generate(query, model)

    async def test_rename_collides_with_other_measure_canonical_raises(
        self,
    ) -> None:
        """DEV-1443 (CodeRabbit review on PR #133): if a query measure's
        ``name`` equals the canonical alias of another measure in the same
        query, ``_ensure_aggregated_measure``'s alias-based dedup would
        silently merge the two aggregates — both filter refs would resolve
        to the SAME underlying aggregate even though the user declared two
        distinct measures. Refuse the query at enrichment time.

        Example: ``revenue:sum`` renamed to ``profit_avg`` AND a separate
        ``profit:avg`` measure both end up at ``<model>.profit_avg``.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
                Column(name="profit", sql="profit", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                # Rename to a name that collides with the second measure's
                # auto-canonical (``profit_avg``).
                ModelMeasure(formula="revenue:sum", name="profit_avg"),
                ModelMeasure(formula="profit:avg"),  # NOSONAR(S125) — canonical = "profit_avg" (explanatory note, not commented-out code)
            ],
        )
        with pytest.raises(ValueError, match=r"declared more than once"):
            await _generate(query, model)

    async def test_filter_canonical_name_collides_with_source_column_raises(
        self,
    ) -> None:
        """DEV-1443 (Codex review on PR #133): if the canonical agg alias
        of a renamed measure literally shadows a source ``Column`` on the
        model, the colon-form filter ``col:agg <op> N`` is ambiguous (it
        could mean the renamed aggregate or the source column). Refuse
        the query at enrichment time rather than silently picking one and
        producing surprising SQL.
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
                # the canonical alias of ``amount:sum``.
                Column(name="amount_sum", sql="amount_sum", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            # Rename triggers the collision check: canonical
            # ``amount_sum`` already names a source column.
            measures=[ModelMeasure(formula="amount:sum", name="revenue")],
        )
        with pytest.raises(ValueError, match=r"shadows a source column"):
            await _generate(query, model)

    async def test_model_level_filter_unaffected_by_query_measure_rename(
        self,
    ) -> None:
        """Codex F3 (test-review): pin the Mode B/DSL-only boundary. A
        ``SlayerModel.filters`` entry (Mode A SQL) must survive query
        enrichment untouched even when the query renames a measure that
        would otherwise feed into ``canonical_to_user_name``. The remap
        helper is only invoked for query filters; model filters go through
        ``parse_sql_predicate`` and never see canonical-agg aliases.
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
                Column(name="deleted_at", sql="deleted_at", type=DataType.TIMESTAMP),
            ],
            # Plain Mode A model filter.
            filters=["deleted_at IS NULL"],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                # Triggers the rename branch — populates canonical_to_user_name.
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
            ],
        )
        sql = await _generate(query, model)
        # The model-level (Mode A) filter must surface in the emitted WHERE
        # clause, unchanged by the query-side measure rename.
        where = _where_text(sql)
        assert "deleted_at" in where, (
            f"model filter dropped from WHERE clause:\nWHERE={where!r}\nSQL:\n{sql}"
        )
        assert "num_customers" not in where, (
            f"model-level (Mode A) filter must not be remapped to the "
            f"query measure alias:\nWHERE={where!r}"
        )
        assert "IS NULL" in where.upper(), (
            f"model filter shape altered:\nWHERE={where!r}"
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
        with pytest.raises(ValueError, match=r"matches a source column"):
            await _generate(query, model)
