"""Stage 2 (DEV-1450) — ResolvedSourceBundle: eagerly resolved query inputs (P11).

The orchestrator builds this once at the top of execute; the binder reads
from it purely. No ContextVar machinery, no callback re-resolution.

Per I2, ``source_model`` is ``Optional`` from day one so a future
anchor-less mode is a type-additive change. DEV-1450 binder asserts
``source_model is not None`` — the type-level optionality is the
extension point.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle


def _model(name: str, ds: str = "prod") -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=ds,
        sql_table=name,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="value", type=DataType.DOUBLE),
        ],
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_minimal(self):
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.source_model is m
        assert b.referenced_models == [m]
        assert b.inline_extensions == []
        assert b.named_queries == {}
        assert b.query_variables == {}
        assert b.datasource_hint is None

    def test_with_referenced_models(self):
        m = _model("orders")
        c = _model("customers")
        r = _model("regions")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m, c, r])
        assert b.referenced_models == [m, c, r]

    def test_with_extensions(self):
        m = _model("orders")
        ext = ModelExtension(source_name="orders")
        b = ResolvedSourceBundle(
            source_model=m, referenced_models=[m], inline_extensions=[ext]
        )
        assert b.inline_extensions == [ext]

    def test_with_named_queries(self):
        m = _model("orders")
        q = SlayerQuery(source_model="orders")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            named_queries={"stage_a": q},
        )
        assert b.named_queries["stage_a"] is q

    def test_with_query_variables(self):
        m = _model("orders")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            query_variables={"region": "NA", "threshold": 100},
        )
        assert b.query_variables == {"region": "NA", "threshold": 100}

    def test_with_datasource_hint(self):
        m = _model("orders", ds="warehouse")
        b = ResolvedSourceBundle(
            source_model=m,
            referenced_models=[m],
            datasource_hint="warehouse",
        )
        assert b.datasource_hint == "warehouse"


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


class TestGetReferencedModel:
    def test_returns_match(self):
        m = _model("orders")
        c = _model("customers")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m, c])
        assert b.get_referenced_model("customers") is c

    def test_returns_none_for_missing(self):
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.get_referenced_model("absent") is None

    def test_source_model_is_in_referenced(self):
        # Convention: source_model is also in referenced_models so the
        # binder doesn't have to special-case the host.
        m = _model("orders")
        b = ResolvedSourceBundle(source_model=m, referenced_models=[m])
        assert b.get_referenced_model("orders") is m


# ---------------------------------------------------------------------------
# I2 — source_model is Optional from day one
# ---------------------------------------------------------------------------


class TestAnchorlessReadiness:
    def test_source_model_none_is_constructible(self):
        # I2: future anchor-less mode reserves source_model=None.
        b = ResolvedSourceBundle(
            source_model=None,
            referenced_models=[_model("orders"), _model("customers")],
        )
        assert b.source_model is None
        # The bundle still holds the set of referenced models that the
        # future global-join planner will operate over.
        assert len(b.referenced_models) == 2

    def test_default_source_model_is_none(self):
        # Defaulting to None keeps both modes type-compatible without
        # callers having to pass an explicit None.
        b = ResolvedSourceBundle()
        assert b.source_model is None
        assert b.referenced_models == []
