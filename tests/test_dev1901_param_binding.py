"""DEV-1901 — binding resolves every aggregation parameter once, onto the key.

Each test binds a formula and asserts the kwargs of the resulting ``AggregateKey``:
a non-overridden definition default and an explicit string fragment land there as
a column key, a scalar, or a ``SqlFragmentKey``; unanalysable text fails at bind.
"""

from __future__ import annotations

from decimal import Decimal
from typing import List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    CircularJoinPathError,
    SlayerError,
    UnanalyzableAggregationParameterError,
    UnknownReferenceError,
)
from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey, SqlFragmentKey
from slayer.core.models import SlayerModel
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import bind_expr
from slayer.engine.compile.stages import _first_unattributable_arg_leaf
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1901_fixtures import (
    BAD_DEFAULTS,
    bundle_of,
    dev1901_models,
    make_exec_engine,
    orders_q,
    revisit_models,
    window_param_models,
)
from tests._dev1954_fixtures import execution_spy

AMOUNT = ColumnKey(path=(), leaf="amount")
SPEND = ColumnKey(path=("customers",), leaf="spend")
POP = ColumnKey(path=("customers", "regions"), leaf="pop")


def _key(formula: str, *, models: Optional[List[SlayerModel]] = None,
         source: str = "orders") -> AggregateKey:
    models = models if models is not None else dev1901_models()
    ordered = sorted(models, key=lambda m: m.name != source)
    key = bind_expr(
        parse_expr(formula), scope=ModelScope(source_model=ordered[0]),
        bundle=bundle_of(ordered), allow_measures=True,
    ).value_key
    assert isinstance(key, AggregateKey), key
    return key


def _kw(formula: str, **kw) -> dict:
    return dict(_key(formula, **kw).kwargs)


def _fragment(value) -> SqlFragmentKey:
    assert isinstance(value, SqlFragmentKey), value
    return value


class TestColumnDefaults:
    def test_bare_default_reads_the_owner_even_when_the_root_shadows_it(self) -> None:
        assert _kw("customers.spend:wshadow") == {
            "weight": ColumnKey(path=("customers",), leaf="factor")}

    def test_dotted_default_from_a_root_owner(self) -> None:
        assert _kw("amount:wcs") == {"weight": SPEND}

    def test_derived_default(self) -> None:
        assert _kw("amount:wcd") == {
            "weight": ColumnSqlKey(path=(), model="orders", column_name="cust_spend")}

    def test_cancelled_default(self) -> None:
        assert _kw("customers.regions.countries.gdp:wsum_region_pop") == {"weight": POP}

    def test_double_cancel(self) -> None:
        assert _kw("customers.regions.countries.gdp:wsum_cust_spend2") == {"weight": SPEND}

    def test_cancel_then_forward(self) -> None:
        assert _kw("customers.regions.countries.gdp:wsum_plan_fee") == {
            "weight": ColumnKey(path=("customers", "plans"), leaf="fee")}

    def test_owner_unreachable_qualifier_anchors_at_the_root(self) -> None:
        assert _kw("customers.spend:wstore") == {
            "weight": ColumnKey(path=("stores",), leaf="rent")}

    def test_owner_at_root_consumes_its_own_name(self) -> None:
        assert _kw("amount:wself") == {
            "weight": ColumnSqlKey(path=(), model="orders", column_name="cost")}

    def test_explicit_argument_replaces_the_default(self) -> None:
        assert _kw("amount:wcs(weight=store_no)") == {
            "weight": ColumnKey(path=(), leaf="store_no")}


class TestScalarAndExpressionDefaults:
    def test_literal_default_binds_the_normalized_scalar(self) -> None:
        assert _kw("amount:wlit") == {"k": Decimal("2")}

    def test_literal_default_equals_its_explicit_twin(self) -> None:
        assert _key("amount:wlit") == _key("amount:wlit(k=2)")

    def test_expression_default_binds_a_fragment(self) -> None:
        frag = _fragment(_kw("amount:wcase")["weight"])
        assert set(frag.refs) == {AMOUNT}
        assert "CASE" in frag.template.upper()

    def test_cancelled_expression_default(self) -> None:
        frag = _fragment(_kw("customers.regions.countries.gdp:wsum_region_pop_expr")["weight"])
        assert frag.refs == (POP,)

    def test_mixed_frame_expression_default_resolves_per_reference(self) -> None:
        frag = _fragment(_kw("customers.spend:wmix_q")["weight"])
        assert set(frag.refs) == {ColumnKey(path=("customers",), leaf="group"), AMOUNT}

    def test_zero_reference_expression(self) -> None:
        frag = _fragment(_kw("amount:wzero")["weight"])
        assert frag.refs == ()

    def test_formatting_variants_intern(self) -> None:
        assert _kw("amount:wfmt_a") == _kw("amount:wfmt_b")


class TestExplicitStringFragments:
    def test_a_string_column_resolves_in_the_query_frame(self) -> None:
        # The default `factor` reads the owner (customers); the string reads the root.
        assert _kw("customers.spend:wshadow(weight='factor')") == {
            "weight": ColumnKey(path=(), leaf="factor")}

    def test_a_string_equals_its_unquoted_twin(self) -> None:
        assert _key("amount:wsum(weight='customers.region_id')") \
            == _key("amount:wsum(weight=customers.region_id)")

    def test_a_string_expression_equals_the_identical_default(self) -> None:
        assert _kw("amount:wsum(weight='customers.spend * 1')") == _kw("amount:wfmt_a")

    def test_a_marker_string_stays_a_string(self) -> None:
        kw = _kw("customers.spend:wshadow(window='90d')")
        assert kw["window"] == "90d"
        assert kw["weight"] == ColumnKey(path=("customers",), leaf="factor")


class TestDefaultEqualsTwin:
    """A default and its identical explicit spelling are one key (one slot)."""

    @pytest.mark.parametrize("default,explicit", [
        ("amount:wsum", "amount:wsum(weight=store_no)"),
        ("amount:wcs", "amount:wcs(weight=customers.spend)"),
        ("customers.spend:wshadow", "customers.spend:wshadow(weight=customers.factor)"),
        ("customers.regions.countries.gdp:wsum_region_pop",
         "customers.regions.countries.gdp:wsum_region_pop(weight=customers.regions.pop)"),
        ("customers.regions.countries.gdp:wsum_fan",
         "customers.regions.countries.gdp:wsum_fan(weight=customers.regions.region_events.value)"),
    ])
    def test_one_key(self, default: str, explicit: str) -> None:
        assert _key(default) == _key(explicit)


class TestOwners:
    def test_saved_measure_binds_its_defaults(self) -> None:
        assert _kw("customers.wsaved") == {"weight": ColumnKey(path=("customers",), leaf="factor")}

    def test_reaggregation_resolves_on_the_host(self) -> None:
        frag = _fragment(_kw("wcase_pop(sum(amount, partition_by=customers.regions.id))")["weight"])
        assert set(frag.refs) == {POP}

    def test_a_stage_source_binds_no_defaults(self) -> None:
        key = _stage_key("rev:max")
        assert key.kwargs == ()

    def test_a_custom_aggregation_without_an_owner_is_unknown(self) -> None:
        with pytest.raises(ValueError, match="wsum"):
            _stage_key("rev:wsum")


def _stage_key(formula: str) -> AggregateKey:
    models = dev1901_models()
    scope = StageSchema(relation_name="stage1", columns=[StageColumn(
        name="rev", sql_alias="rev", public_alias="rev", type=DataType.DOUBLE)])
    key = bind_expr(parse_expr(formula), scope=scope, bundle=ResolvedSourceBundle(
        dialect="postgres", source_model=None, referenced_models=models)).value_key
    assert isinstance(key, AggregateKey), key
    return key


class TestUnanalysableText:
    """Unparseable text, or an aggregate / window / subquery, fails at bind."""

    @staticmethod
    def _assert_names(err: Exception, *, agg: str, text: str) -> None:
        msg = str(err)
        for part in (agg, "weight", text, "orders"):
            assert part in msg, msg

    @pytest.mark.parametrize("agg", sorted(BAD_DEFAULTS))
    def test_default(self, agg: str) -> None:
        with pytest.raises(UnanalyzableAggregationParameterError) as ei:
            _key(f"amount:{agg}")
        self._assert_names(ei.value, agg=agg, text=BAD_DEFAULTS[agg])

    @pytest.mark.parametrize("text", sorted(BAD_DEFAULTS.values()))
    def test_string_argument(self, text: str) -> None:
        quoted = text.replace("'", "\\'")
        with pytest.raises(UnanalyzableAggregationParameterError) as ei:
            _key(f"amount:wsum(weight='{quoted}')")
        self._assert_names(ei.value, agg="wsum", text=text)

    def test_is_a_slayer_error(self) -> None:
        assert issubclass(UnanalyzableAggregationParameterError, SlayerError)

    async def test_no_sql_reaches_the_database(self, engine, monkeypatch) -> None:
        sent = execution_spy(monkeypatch)
        query = orders_q(measures=[{"formula": "amount:wbad_agg", "name": "w"}])
        with pytest.raises(UnanalyzableAggregationParameterError):
            await engine.execute(query)
        assert sent == []


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestOtherBindErrors:
    def test_missing_column_raises_the_explicit_twins_error(self) -> None:
        with pytest.raises(UnknownReferenceError, match="nosuch"):
            _key("amount:wsum(weight=nosuch)")
        with pytest.raises(UnknownReferenceError, match="nosuch"):
            _key("amount:wmissing")

    def test_unbound_placeholder_fails_at_bind(self) -> None:
        with pytest.raises(ValueError, match="(?s)wunbound.*scale|scale.*wunbound"):
            _key("amount:wunbound")

    @pytest.mark.parametrize("formula", ["customers.spend:wrev", "customers.spend:wrev_expr"])
    def test_edge_name_revisit_is_circular(self, formula: str) -> None:
        models = revisit_models()
        with pytest.raises(CircularJoinPathError):
            _key(formula, models=models)

    @pytest.mark.parametrize("placeholder", [True, False])
    def test_window_reserved_for_a_stored_model(self, placeholder: bool) -> None:
        models = window_param_models(placeholder=placeholder)
        with pytest.raises(SlayerError, match="(?s)wwin.*trailing|trailing.*wwin"):
            _key("amount:wwin", models=models)


class TestUnattributableParameterName:
    """The input-safety backstop names the parameter holding an unsafe expression."""

    @pytest.mark.parametrize("template", [
        "{r0} * 1", "CASE WHEN {r0} > 0 THEN 1 ELSE 0 END"])
    def test_names_the_parameter(self, template: str) -> None:
        models = dev1901_models()
        regions = next(m for m in models if m.name == "regions")
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="pop"), agg="wsum_cust_spend",
            kwargs=(("weight", SqlFragmentKey(
                template=template,
                refs=(ColumnKey(path=("region_events",), leaf="value"),))),))
        bundle = bundle_of(sorted(models, key=lambda m: m.name != "regions"))
        out = _first_unattributable_arg_leaf(
            agg=key, target_path=(), root_model=regions,
            models_by_name=bundle.models_by_name, bundle=bundle,
            host_model=regions, host_name="regions")
        assert out, out
        assert out[0][0] == "weight", out
