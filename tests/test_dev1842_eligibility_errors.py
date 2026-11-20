"""Position eligibility, resolution order, and the error
contract for dotted saved-metric references.

Legal in exactly two positions: metric expressions and computed-dimension
expressions. Everywhere else — an aggregation suffix, a plain dimension, an
unselected filter/ORDER BY, a ``partition_by`` member, a raw-row query — a
dotted reference to a saved metric fails with a message naming it as a saved
metric and where it may be referenced (never suggesting an aggregation suffix
for a non-field). Resolution order is declared-alias → field → saved metric,
so a *selected* dotted metric stays addressable by its name in filters/ORDER BY.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import DistinctDimensionValuesError

from tests._dev1842_fixtures import gen, make_exec_engine, q


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _assert_names_saved_metric(message: str) -> None:
    low = message.lower()
    assert "saved metric" in low, message
    assert "aov" in message, message


class TestIneligiblePositions:
    """Each ineligible position must reject with a saved-metric-aware message."""

    async def test_aggregation_suffix_errors(self) -> None:
        """``sum(customers.aov)`` — a saved metric takes no aggregation; the
        message mirrors the bare-name form and points at ``customers.aov``."""
        query = q(metrics=[{"metric": "sum(customers.aov)", "alias": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value)
        _assert_names_saved_metric(message)
        assert "takes no aggregation" in message
        assert "customers.aov" in message

    async def test_plain_dimension_entry_errors(self) -> None:
        query = q(dimensions=["customers.aov"],
                  metrics=[{"metric": "sum(orders.amount)", "alias": "a"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        _assert_names_saved_metric(str(ei.value))

    async def test_unselected_filter_errors(self) -> None:
        query = q(dimensions=["customers.tier"],
                  metrics=[{"metric": "sum(orders.amount)", "alias": "a"}],
                  filter=["customers.aov > 100"])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        _assert_names_saved_metric(str(ei.value))

    async def test_order_by_formula_errors(self) -> None:
        query = q(dimensions=["customers.tier"],
                  metrics=[{"metric": "sum(orders.amount)", "alias": "a"}],
                  order=[{"field": "customers.aov", "direction": "desc"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        _assert_names_saved_metric(str(ei.value))

    async def test_partition_by_member_errors(self) -> None:
        query = q(dimensions=["customers.tier"],
                  metrics=[{"metric": "sum(orders.amount, partition_by=customers.aov)",
                             "alias": "a"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        _assert_names_saved_metric(str(ei.value))

    async def test_legal_positions_are_named(self) -> None:
        """At least one ineligible-position error must say WHERE the reference is
        legal (metric expressions / computed dimensions)."""
        query = q(dimensions=["customers.aov"],
                  metrics=[{"metric": "sum(orders.amount)", "alias": "a"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        low = str(ei.value).lower()
        assert "metric expression" in low or "computed dimension" in low, ei.value


class TestRawRowQueries:
    async def test_raw_row_filter_dotted_measure_targeted_error(self) -> None:
        """``distinct_dimension_values=False`` rejects a dotted saved-metric
        reference in a filter — the raw-row detector sees through the ``DottedRef``
        and names the specific metric (the filter branch quotes the predicate)."""
        query = q(distinct_dimension_values=False,
                  dimensions=["customers.tier"],
                  filter=["customers.aov > 0"])
        with pytest.raises(DistinctDimensionValuesError) as ei:
            await gen(query)
        message = str(ei.value)
        assert "customers.aov" in message
        assert "metric" in message.lower()

    async def test_raw_row_order_dotted_measure_targeted_error(self) -> None:
        query = q(distinct_dimension_values=False,
                  dimensions=["customers.tier"],
                  order=[{"field": "customers.aov", "direction": "desc"}])
        with pytest.raises(DistinctDimensionValuesError) as ei:
            await gen(query)
        _assert_names_saved_metric(str(ei.value))


class TestUnresolvableDottedLeaf:
    async def test_neither_column_nor_measure_names_both_namespaces(self) -> None:
        """``customers.zzz`` matches nothing: the error names ``customers`` and
        mentions BOTH namespaces (a real field AND the metric namespace), and
        never suggests an aggregation suffix for a non-field."""
        query = q(metrics=[{"metric": "customers.zzz", "alias": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value)
        low = message.lower()
        assert "customers" in message
        assert "metric" in low, "must mention the saved-metric namespace"
        # The field namespace is present too — either the word or a real field.
        assert "field" in low or "spend" in message or "tier" in message
        assert ":sum" not in message, "must not suggest an aggregation suffix"

    async def test_close_measure_name_is_suggested(self) -> None:
        query = q(metrics=[{"metric": "customers.aovv", "alias": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value)
        assert "Did you mean" in message
        assert "aov" in message

    async def test_close_column_name_is_suggested(self) -> None:
        """A near-miss to a FIELD suggests it — both namespaces feed the
        suggester, not just metrics."""
        query = q(metrics=[{"metric": "customers.tierr", "alias": "x"}])
        with pytest.raises(ValueError) as ei:
            await gen(query)
        message = str(ei.value)
        assert "Did you mean" in message
        assert "tier" in message


class TestSelectedMeasureAddressable:
    """A selected dotted metric is addressable by its name (declared-alias
    precedence). The tests verify the filter/ORDER actually binds to the
    SELECTED slot by asserting the observed effect, not just that SQL compiled."""

    async def test_dotted_name_filter_applies_to_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.aov"}],
            filter=["customers.aov > 100"]))
        rows = [r for r in resp.data if r.get("customers.aov") is not None]
        assert rows, "filter removed everything — it did not bind to the aov slot"
        for r in rows:
            assert float(r["customers.aov"]) > 100

    async def test_dotted_name_order_uses_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.aov"}],
            order=[{"field": "customers.aov", "direction": "desc"}]))
        vals = [float(r["customers.aov"]) for r in resp.data
                if r.get("customers.aov") is not None]
        assert vals == sorted(vals, reverse=True)
        assert len(vals) > 1

    async def test_explicit_alias_filter_applies_to_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.aov", "alias": "myaov"}],
            filter=["myaov > 100"]))
        rows = [r for r in resp.data if r.get("myaov") is not None]
        assert rows
        for r in rows:
            assert float(r["myaov"]) > 100
