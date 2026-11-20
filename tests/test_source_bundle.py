"""ResolvedSourceBundle: eagerly resolved query inputs (P11).

The orchestrator builds this once at the top of execute; the binder reads
from it purely. No ContextVar machinery, no callback re-resolution.

Per I2, ``source_model`` is ``Optional`` from day one so a future
anchor-less mode is a type-additive change. The binder asserts
``source_model is not None`` — the type-level optionality is the
extension point.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.ir.source_bundle import ResolvedSourceBundle


def _dataset(name: str, ds: str = "prod") -> SlayerDataset:
    return SlayerDataset(
        name=name,
        data_source=ds,
        sql_table=name,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="value", datatype=DataType.FLOAT),
        ],
    )


def _dataset_with(
    name: str,
    *,
    aggs: list[str] | None = None,
    relationships: list[str] | None = None,
    ds: str = "prod",
) -> SlayerDataset:
    return SlayerDataset(
        name=name,
        data_source=ds,
        sql_table=name,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="value", datatype=DataType.FLOAT),
        ],
        aggregations=[
            Aggregation(name=a, expression="AVG({value})") for a in (aggs or [])
        ],
        relationships=[
            DatasetRelationship(to=t, from_columns=["id"], to_columns=["id"])
            for t in (relationships or [])
        ],
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_minimal(self):
        m = _dataset("orders")
        b = ResolvedSourceBundle(dialect="postgres", population=m, referenced_datasets=[m])
        assert b.population is m
        assert b.referenced_datasets == [m]
        assert b.named_queries == {}
        assert b.query_variables == {}
        assert b.datasource_hint is None

    def test_with_referenced_models(self):
        m = _dataset("orders")
        c = _dataset("customers")
        r = _dataset("regions")
        b = ResolvedSourceBundle(dialect="postgres", population=m, referenced_datasets=[m, c, r])
        assert b.referenced_datasets == [m, c, r]

    def test_with_named_queries(self):
        m = _dataset("orders")
        q = SlayerQuery(population="orders")
        b = ResolvedSourceBundle(
            dialect="postgres",
            population=m,
            referenced_datasets=[m],
            named_queries={"stage_a": q},
        )
        assert b.named_queries["stage_a"] is q

    def test_with_query_variables(self):
        m = _dataset("orders")
        b = ResolvedSourceBundle(
            dialect="postgres",
            population=m,
            referenced_datasets=[m],
            query_variables={"region": "NA", "threshold": 100},
        )
        assert b.query_variables == {"region": "NA", "threshold": 100}

    def test_with_datasource_hint(self):
        m = _dataset("orders", ds="warehouse")
        b = ResolvedSourceBundle(
            dialect="postgres",
            population=m,
            referenced_datasets=[m],
            datasource_hint="warehouse",
        )
        assert b.datasource_hint == "warehouse"


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


class TestGetReferencedModel:
    def test_returns_match(self):
        m = _dataset("orders")
        c = _dataset("customers")
        b = ResolvedSourceBundle(dialect="postgres", population=m, referenced_datasets=[m, c])
        assert b.get_referenced_dataset("customers") is c

    def test_returns_none_for_missing(self):
        m = _dataset("orders")
        b = ResolvedSourceBundle(dialect="postgres", population=m, referenced_datasets=[m])
        assert b.get_referenced_dataset("absent") is None

    def test_source_model_is_in_referenced(self):
        # Convention: source_model is also in referenced_models so the
        # binder doesn't have to special-case the host.
        m = _dataset("orders")
        b = ResolvedSourceBundle(dialect="postgres", population=m, referenced_datasets=[m])
        assert b.get_referenced_dataset("orders") is m


# ---------------------------------------------------------------------------
# I2 — source_model is Optional from day one
# ---------------------------------------------------------------------------


class TestAnchorlessReadiness:
    def test_source_model_none_is_constructible(self):
        # I2: future anchor-less mode reserves source_model=None.
        b = ResolvedSourceBundle(
            dialect="postgres",
            population=None,
            referenced_datasets=[_dataset("orders"), _dataset("customers")],
        )
        assert b.population is None
        # The bundle still holds the set of referenced datasets that the
        # future global-join planner will operate over.
        assert len(b.referenced_datasets) == 2

    def test_default_source_model_is_none(self):
        # Defaulting to None keeps both modes type-compatible without
        # callers having to pass an explicit None.
        b = ResolvedSourceBundle(dialect="postgres")
        assert b.population is None
        assert b.referenced_datasets == []
