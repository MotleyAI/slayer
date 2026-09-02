"""DEV-1842 task 1.5 — position eligibility, resolution order, and the error
contract for dotted saved-measure references.

Legal in exactly two positions: measure formulas and computed-dimension
expressions. Everywhere else — an aggregation suffix, a plain dimension, an
unselected filter/ORDER BY, a ``partition_by`` member, a raw-row query — a
dotted reference that resolves to a saved measure fails with a message naming it
as a saved measure and where it may be referenced (never suggesting an
aggregation suffix for something that is not a column). Resolution order is
declared-alias → column → saved measure, so a *selected* dotted measure stays
addressable by its name in filters and ORDER BY.

Today the dotted forms raise a generic ``UnknownReferenceError`` (or bind as an
unknown column), so these fail for the right reason.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import DistinctDimensionValuesError

from tests._dev1842_fixtures import gen, make_exec_engine, q


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _assert_names_saved_measure(message: str) -> None:
    low = message.lower()
    assert "saved measure" in low, message
    assert "aov" in message, message


# --------------------------------------------------------------------------- #
# Ineligible positions — each must reject with a saved-measure-aware message.
# --------------------------------------------------------------------------- #
class TestIneligiblePositions:
    async def test_aggregation_suffix_errors(self) -> None:
        """``customers.aov:sum`` — a saved measure takes no aggregation; the
        message mirrors the bare-name form and points at ``customers.aov``."""
        with pytest.raises(ValueError) as ei:
            await gen(q(measures=[{"formula": "customers.aov:sum", "name": "x"}]))
        message = str(ei.value)
        _assert_names_saved_measure(message)
        assert "takes no aggregation" in message
        assert "customers.aov" in message

    async def test_plain_dimension_entry_errors(self) -> None:
        with pytest.raises(ValueError) as ei:
            await gen(q(dimensions=["customers.aov"],
                        measures=[{"formula": "amount:sum", "name": "a"}]))
        _assert_names_saved_measure(str(ei.value))

    async def test_unselected_filter_errors(self) -> None:
        with pytest.raises(ValueError) as ei:
            await gen(q(dimensions=["customers.tier"],
                        measures=[{"formula": "amount:sum", "name": "a"}],
                        filters=["customers.aov > 100"]))
        _assert_names_saved_measure(str(ei.value))

    async def test_order_by_formula_errors(self) -> None:
        with pytest.raises(ValueError) as ei:
            await gen(q(dimensions=["customers.tier"],
                        measures=[{"formula": "amount:sum", "name": "a"}],
                        order=[{"column": "customers.aov", "direction": "desc"}]))
        _assert_names_saved_measure(str(ei.value))

    async def test_partition_by_member_errors(self) -> None:
        with pytest.raises(ValueError) as ei:
            await gen(q(dimensions=["customers.tier"],
                        measures=[{"formula": "amount:sum(partition_by=customers.aov)",
                                   "name": "a"}]))
        _assert_names_saved_measure(str(ei.value))

    async def test_legal_positions_are_named(self) -> None:
        """At least one ineligible-position error must say WHERE the reference is
        legal (measure formulas / computed dimensions)."""
        with pytest.raises(ValueError) as ei:
            await gen(q(dimensions=["customers.aov"],
                        measures=[{"formula": "amount:sum", "name": "a"}]))
        low = str(ei.value).lower()
        assert "measure formula" in low or "computed dimension" in low, ei.value


class TestRawRowQueries:
    async def test_raw_row_filter_dotted_measure_targeted_error(self) -> None:
        """``distinct_dimension_values=False`` rejects a dotted saved-measure
        reference in a filter with the same targeted error as a bare one — the
        raw-row detector must see through the ``DottedRef``."""
        with pytest.raises(DistinctDimensionValuesError):
            await gen(q(distinct_dimension_values=False,
                        dimensions=["customers.tier"],
                        filters=["customers.aov > 0"]))

    async def test_raw_row_order_dotted_measure_targeted_error(self) -> None:
        with pytest.raises(DistinctDimensionValuesError):
            await gen(q(distinct_dimension_values=False,
                        dimensions=["customers.tier"],
                        order=[{"column": "customers.aov", "direction": "desc"}]))


class TestUnresolvableDottedLeaf:
    async def test_neither_column_nor_measure_names_both_namespaces(self) -> None:
        """``customers.zzz`` matches nothing: the error names ``customers`` and
        mentions BOTH namespaces (a real column AND the measure namespace), and
        never suggests an aggregation suffix for a non-column."""
        with pytest.raises(ValueError) as ei:
            await gen(q(measures=[{"formula": "customers.zzz", "name": "x"}]))
        message = str(ei.value)
        low = message.lower()
        assert "customers" in message
        assert "measure" in low, "must mention the saved-measure namespace"
        # The column namespace is present too — either the word or a real column.
        assert "column" in low or "spend" in message or "tier" in message
        assert ":sum" not in message, "must not suggest an aggregation suffix"

    async def test_close_measure_name_is_suggested(self) -> None:
        with pytest.raises(ValueError) as ei:
            await gen(q(measures=[{"formula": "customers.aovv", "name": "x"}]))
        message = str(ei.value)
        assert "Did you mean" in message and "aov" in message

    async def test_close_column_name_is_suggested(self) -> None:
        """A near-miss to a COLUMN suggests it — both namespaces feed the
        suggester, not just measures."""
        with pytest.raises(ValueError) as ei:
            await gen(q(measures=[{"formula": "customers.tierr", "name": "x"}]))
        message = str(ei.value)
        assert "Did you mean" in message and "tier" in message


# --------------------------------------------------------------------------- #
# Resolution order — a SELECTED dotted measure is addressable by its name.
# --------------------------------------------------------------------------- #
class TestSelectedMeasureAddressable:
    """A selected dotted measure is addressable by its name (declared-alias
    precedence). The tests verify the filter/ORDER actually binds to the
    SELECTED slot by asserting the observed effect, not just that SQL compiled."""

    async def test_dotted_name_filter_applies_to_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.aov"}],
            filters=["customers.aov > 100"]))
        rows = [r for r in resp.data if r.get("orders.customers.aov") is not None]
        assert rows, "filter removed everything — it did not bind to the aov slot"
        for r in rows:
            assert float(r["orders.customers.aov"]) > 100

    async def test_dotted_name_order_uses_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.aov"}],
            order=[{"column": "customers.aov", "direction": "desc"}]))
        vals = [float(r["orders.customers.aov"]) for r in resp.data
                if r.get("orders.customers.aov") is not None]
        assert vals == sorted(vals, reverse=True) and len(vals) > 1

    async def test_explicit_alias_filter_applies_to_selected_slot(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.aov", "name": "myaov"}],
            filters=["myaov > 100"]))
        rows = [r for r in resp.data if r.get("orders.myaov") is not None]
        assert rows
        for r in rows:
            assert float(r["orders.myaov"]) > 100
