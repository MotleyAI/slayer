"""Named-measure expansion eligibility (typed pipeline).

DEV-1484 Stage C backfill: the legacy ``the legacy formula parser(..., named_measures=)``
expander had "don't over-substitute" rules pinned by
``test_formula.py::TestNamedMeasureExpansion``. The typed equivalent is
``slayer.engine.measure_expansion.expand_model_measures``, which rewrites a
parsed AST in place. The positive expansion cases (root / transform /
arithmetic / chained / cycle) are covered end-to-end by
``test_named_measures.py``; this file pins the negative eligibility rules
from the module's eligibility matrix that have no other coverage:

* a bare ref in an ``AggCall.source`` slot is NOT expanded (colon syntax);
* a ``DottedRef`` (cross-model path) is NOT expanded;
* a measure can never shadow a transform name in the call position,
  because the ``ModelMeasure`` name validator rejects reserved transform
  names at construction.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.engine.measure_expansion import expand_model_measures
from slayer.engine.syntax import AggCall, DottedRef, Ref, parse_expr


def _model(measures=None) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="ds",
        columns=[
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        ],
        measures=measures or [],
    )


def test_aggcall_source_ref_not_expanded() -> None:
    """``revenue:sum`` keeps its bare ``revenue`` source even when a measure
    named ``revenue`` exists — colon-syntax sources are column-level by
    contract and must not be substituted.
    """
    model = _model()
    shadow = ModelMeasure(name="revenue", formula="*:count")
    result = expand_model_measures(
        expr=parse_expr("revenue:sum"), model=model, extra_measures=[shadow],
    )
    assert result == AggCall(source=Ref(name="revenue"), agg="sum")


def test_dotted_ref_not_expanded() -> None:
    """``customers.aov`` is a cross-model dotted path; it resolves through
    the join graph at bind time, not through measure expansion, so the
    expander leaves the ``DottedRef`` untouched.
    """
    model = _model(measures=[ModelMeasure(name="aov", formula="revenue:sum")])
    result = expand_model_measures(expr=parse_expr("customers.aov"), model=model)
    assert result == DottedRef(parts=("customers", "aov"))


def test_measure_name_cannot_shadow_transform() -> None:
    """A saved measure can never be named after a built-in transform, so the
    legacy "don't substitute ``cumsum`` when followed by ``(``" case cannot
    arise — the ``ModelMeasure`` validator rejects the name at construction.
    """
    with pytest.raises(ValueError, match="reserved transform name"):
        ModelMeasure(name="cumsum", formula="*:count")
