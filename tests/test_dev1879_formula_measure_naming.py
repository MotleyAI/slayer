"""Derived result keys for unnamed formula measures.

An unnamed measure whose formula is not a plain aggregate reference (an
arithmetic composite, a transform, or a mix with literals) derives its result
key by sanitizing the canonical formula text through
``auto_name_from_expression`` — the product-wide expression-name convention.
Named, saved, and aggregate-rooted measures keep their existing keys.
"""

from __future__ import annotations

import re

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.core.refs import auto_name_from_expression
from tests._dev1879_helpers import dry_response, mart_model


# A result key: dot-separated bare identifiers, nothing else.
_KEY_RE = re.compile(r"[A-Za-z_]\w*(\.[A-Za-z_]\w*)*")


async def _dry(query: SlayerQuery):
    mart = mart_model(
        extra_columns=[
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.DOUBLE),
        ],
        measures=[ModelMeasure(name="aov", formula="sum(revenue) / sum(quantity)")],
    )
    return await dry_response(query, mart=mart)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "mart")
    return SlayerQuery(**kw)


def _month_td() -> list[TimeDimension]:
    return [
        TimeDimension(
            dimension="created_at",
            granularity=TimeGranularity.MONTH,
        )
    ]


class TestDerivedKeys:
    async def test_arithmetic_composite(self) -> None:
        resp = await _dry(_q(measures=["sum(logo_churn) / sum(logo_bop)"]))
        assert "mart.logo_churn_sum_logo_bop_sum" in resp.columns
        for key in resp.columns:
            assert _KEY_RE.fullmatch(key), key

    async def test_transform(self) -> None:
        resp = await _dry(
            _q(
                measures=["time_shift(sum(cmrr_eop), -1, 'year')"],
                time_dimensions=_month_td(),
            )
        )
        assert "mart.time_shift_cmrr_eop_sum_1_year" in resp.columns
        for key in resp.columns:
            assert _KEY_RE.fullmatch(key), key

    async def test_formatting_insensitive(self) -> None:
        r1 = await _dry(_q(measures=["sum(logo_churn) / sum(logo_bop)"]))
        r2 = await _dry(_q(measures=["sum(logo_churn)/sum(logo_bop)"]))
        assert list(r1.columns) == list(r2.columns)

    async def test_long_formula_hash_folds(self) -> None:
        spaced = "sum(logo_churn) / sum(logo_bop) / sum(cmrr_eop) / avg(logo_churn)"
        packed = "sum(logo_churn)/sum(logo_bop)/sum(cmrr_eop)/avg(logo_churn)"
        expected = auto_name_from_expression("logo_churn:sum / logo_bop:sum / cmrr_eop:sum / logo_churn:avg")
        assert re.search(r"_[0-9a-f]{8}_", expected), expected
        r1 = await _dry(_q(measures=[spaced]))
        r2 = await _dry(_q(measures=[packed]))
        assert f"mart.{expected}" in r1.columns
        assert list(r1.columns) == list(r2.columns)

    async def test_leading_digit_guard(self) -> None:
        resp = await _dry(_q(measures=["100 * sum(logo_churn)"]))
        assert "mart.e_100_logo_churn_sum" in resp.columns


def _assert_key_and_alias(resp, key: str) -> None:
    assert key in resp.columns
    assert resp.sql is not None
    assert f'AS "{key}"' in resp.sql


class TestUnchangedPaths:
    async def test_explicit_name_wins(self) -> None:
        resp = await _dry(
            _q(
                measures=[
                    {
                        "formula": "sum(logo_churn) / sum(logo_bop)",
                        "name": "churn_ratio",
                    }
                ]
            )
        )
        _assert_key_and_alias(resp, "mart.churn_ratio")
        assert "mart.logo_churn_sum_logo_bop_sum" not in resp.columns

    async def test_plain_aggregates(self) -> None:
        resp = await _dry(_q(measures=["sum(revenue)", "count(*)"]))
        _assert_key_and_alias(resp, "mart.revenue_sum")
        _assert_key_and_alias(resp, "mart._count")

    async def test_parametric_aggregate(self) -> None:
        resp = await _dry(_q(measures=["percentile(price, p=0.9)"]))
        _assert_key_and_alias(resp, "mart.price_percentile_p_0_9")

    async def test_expression_aggregation(self) -> None:
        resp = await _dry(_q(measures=["sum(logo_churn - logo_bop)"]))
        _assert_key_and_alias(resp, "mart.logo_churn_logo_bop_sum")

    async def test_cross_model_aggregate(self) -> None:
        resp = await _dry(_q(measures=["sum(targets.goal)"]))
        _assert_key_and_alias(resp, "mart.targets.goal_sum")

    async def test_saved_measure_reference(self) -> None:
        resp = await _dry(_q(measures=["aov"]))
        _assert_key_and_alias(resp, "mart.aov")


class TestCollisions:
    async def test_different_formulas_same_key_fail_loudly(self) -> None:
        q = _q(
            measures=[
                "sum(logo_churn) / sum(logo_bop)",
                "sum(logo_churn) * sum(logo_bop)",
            ]
        )
        with pytest.raises(ValueError, match="Rename") as ei:
            await _dry(q)
        msg = str(ei.value)
        assert "sum(logo_churn) / sum(logo_bop)" in msg
        assert "sum(logo_churn) * sum(logo_bop)" in msg
        assert "set 'name'" in msg

    async def test_identical_formulas_merge(self) -> None:
        resp = await _dry(
            _q(
                measures=[
                    "sum(logo_churn) / sum(logo_bop)",
                    "sum(logo_churn) / sum(logo_bop)",
                ]
            )
        )
        assert list(resp.columns).count("mart.logo_churn_sum_logo_bop_sum") == 1

    async def test_identical_formulas_conflicting_metadata_fail_loudly(self) -> None:
        q = _q(
            measures=[
                {"formula": "sum(logo_churn) / sum(logo_bop)", "type": "DOUBLE"},
                {"formula": "sum(logo_churn) / sum(logo_bop)", "type": "INT"},
            ]
        )
        with pytest.raises(ValueError, match="set 'name'"):
            await _dry(q)

    async def test_spacing_variants_merge(self) -> None:
        resp = await _dry(
            _q(
                measures=[
                    "sum(logo_churn) / sum(logo_bop)",
                    "sum(logo_churn)/sum(logo_bop)",
                ]
            )
        )
        assert list(resp.columns).count("mart.logo_churn_sum_logo_bop_sum") == 1


class TestRawFormulaReferences:
    async def test_order_by_raw_formula(self) -> None:
        resp = await _dry(
            _q(
                measures=["sum(logo_churn) / sum(logo_bop)"],
                dimensions=["region"],
                # Raw formula text as the order column is the surface under test.
                order=[
                    OrderItem.model_validate(
                        {
                            "column": "sum(logo_churn) / sum(logo_bop)",
                            "direction": "desc",
                        }
                    )
                ],
            )
        )
        assert "mart.logo_churn_sum_logo_bop_sum" in resp.columns
        assert resp.sql is not None
        assert "ORDER BY" in resp.sql.upper()

    async def test_filter_by_raw_formula(self) -> None:
        resp = await _dry(
            _q(
                measures=["sum(logo_churn) / sum(logo_bop)"],
                dimensions=["region"],
                filters=["sum(logo_churn) / sum(logo_bop) > 0.1"],
            )
        )
        assert "mart.logo_churn_sum_logo_bop_sum" in resp.columns
        assert resp.sql is not None
        assert "0.1" in resp.sql


class TestSanitizerConvention:
    def test_special_characters_collapse(self) -> None:
        assert (
            auto_name_from_expression("logo_churn:sum / logo_bop:sum")
            == "logo_churn_sum_logo_bop_sum"
        )

    def test_quotes_and_minus(self) -> None:
        assert (
            auto_name_from_expression("time_shift(cmrr_eop:sum, -1, 'year')")
            == "time_shift_cmrr_eop_sum_1_year"
        )

    def test_leading_digit_guard(self) -> None:
        assert auto_name_from_expression("100 * x:sum") == "e_100_x_sum"

    def test_no_dunder(self) -> None:
        assert "__" not in auto_name_from_expression("a:sum //  b:sum")
