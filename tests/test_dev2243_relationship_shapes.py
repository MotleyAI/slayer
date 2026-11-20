"""Relationship shapes Ossie allows, for every dataset.

Specs: models/join-keys (computed keys), models/join-traversal (edge names, the ambiguous hop, self-relationships).
"""

from __future__ import annotations

import contextlib
import re
import warnings
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import AmbiguousReferenceError, RelationshipKeyError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.inference import CardinalityVerdict
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse, _detection_skip_reason

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1902_fixtures import cells, warnings_of
from tests._dev2243_fixtures import dataset, document_engine, document_text, error, field, relationship
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
_M2O = RelationshipCardinality.MANY_TO_ONE


async def _run(engine: SlayerQueryEngine, body: dict[str, Any]) -> SlayerResponse:
    return await engine.execute(body)


@contextlib.asynccontextmanager
async def _engine(backend: str, *, tables: list[TableSpec], datasets: list[SlayerDataset],
                  ) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine; every dataset saved through save-time validation."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables), datasets=[], validate=True,
    ) as (engine, _db):
        for ds in datasets:
            await engine.save_dataset(ds)
        yield engine


def _f(name: str, datatype: DataType = DataType.STRING, *, expression: str | None = None,
       pk: bool = False, **extra: Any) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, expression=expression, primary_key=pk, **extra)


def _ds(name: str, *fields: DatasetField, relationships: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(name=name, sql_table=name, data_source="test", fields=list(fields),
                         relationships=relationships or [])


def _rel(to: str, frm: str, to_col: str, *, name: str | None = None) -> DatasetRelationship:
    return DatasetRelationship(to=to, name=name, from_columns=[frm], to_columns=[to_col], cardinality=_M2O)


# --------------------------------------------------------------------------- #
# Computed keys: orders.cust_fk is TEXT, so the key is the computed CAST(cust_fk AS INT)
# --------------------------------------------------------------------------- #
_K_CUSTOMERS = TableSpec(
    name="customers", columns=[("customer_pk", "INT"), ("name", "TEXT"), ("credit", "DOUBLE")],
    rows=[(1, "Ann", 100.0), (2, "Bob", 200.0), (3, "Cid", 300.0)],
)
_K_ORDERS = TableSpec(
    name="orders", columns=[("order_pk", "INT"), ("cust_fk", "TEXT"), ("amount", "DOUBLE"), ("status", "TEXT")],
    rows=[(1, "1", 10.0, "ok"), (2, "1", 20.0, "new"), (3, "2", 30.0, "ok"), (4, "3", 40.0, "ok"),
          (5, "3", 50.0, "new")],
)
_K_TABLES = [_K_CUSTOMERS, _K_ORDERS]
_AMOUNT_BY_CUSTOMER = {"Ann": 30.0, "Bob": 30.0, "Cid": 90.0}
_CREDIT_WITH_NEW_ORDER = 400.0  # Ann + Cid
_CAST_KEY = "CAST(cust_fk AS INT)"


def _k_customers(*, computed_pk: bool = False) -> SlayerDataset:
    pk = (_f("id", DataType.INTEGER, expression="CAST(customer_pk AS INT)", pk=True) if computed_pk
          else _f("id", DataType.INTEGER, expression="customer_pk", pk=True))
    return _ds("customers", pk, _f("name"), _f("credit", DataType.FLOAT))


def _k_orders(key: DatasetField | None = None) -> SlayerDataset:
    key = key or _f("customer_key", DataType.INTEGER, expression=_CAST_KEY)
    return _ds("orders", _f("id", DataType.INTEGER, expression="order_pk", pk=True), _f("cust_fk"), key,
               _f("amount", DataType.FLOAT), _f("status"),
               relationships=[DatasetRelationship(to="customers", from_columns=[key.name], to_columns=["id"])])


def _on_clauses(sql: str) -> list[str]:
    """Every ``JOIN … ON …`` predicate text of ``sql``, flattened to one line."""
    flat = " ".join(sql.split())
    return re.findall(r"\bON\b(.*?)(?=\bLEFT\b|\bINNER\b|\bJOIN\b|\bWHERE\b|\bGROUP\b|\)\s*(?:AS|,|$)|$)", flat,
                      flags=re.IGNORECASE)


def _mentions_cast_key(text: str) -> bool:
    return re.search(r"CAST\(\s*[\w\".]*cust_fk\"?\s+AS\s+INT", text, flags=re.IGNORECASE) is not None


class TestComputedKeys:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_computed_source_key_joins_on_its_expression(self, backend: str) -> None:
        """Spec: models/join-keys › A computed source key joins on its expression."""
        async with _engine(backend, tables=_K_TABLES, datasets=[_k_customers(), _k_orders()]) as engine:
            resp = await _run(engine, {"population": "orders", "dimensions": ["customers.name"],
                                       "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]})
        assert cells(resp, dim_suffix="customers.name", metric="amt") == _AMOUNT_BY_CUSTOMER
        assert resp.sql is not None
        assert any(_mentions_cast_key(on) and "customer_pk" in on for on in _on_clauses(resp.sql)), resp.sql

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_computed_keys_on_both_sides_join(self, backend: str) -> None:
        """Spec: models/join-keys › Join keys name model columns (a computed target key is a declared field)."""
        datasets = [_k_customers(computed_pk=True), _k_orders()]
        async with _engine(backend, tables=_K_TABLES, datasets=datasets) as engine:
            resp = await _run(engine, {"population": "orders", "dimensions": ["customers.name"],
                                       "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]})
        assert cells(resp, dim_suffix="customers.name", metric="amt") == _AMOUNT_BY_CUSTOMER

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_computed_key_correlates_a_pushed_down_restriction(self, backend: str) -> None:
        """Spec: models/join-keys › A computed key correlates a pushed-down restriction."""
        async with _engine(backend, tables=_K_TABLES, datasets=[_k_customers(), _k_orders()]) as engine:
            resp = await _run(engine, {"population": "customers", "filter": ["orders.status = 'new'"],
                                       "metrics": [{"metric": "sum(customers.credit)", "alias": "cr"}]})
        (row,) = resp.data
        assert float(row["cr"]) == pytest.approx(_CREDIT_WITH_NEW_ORDER)
        assert warnings_of(resp, "semi_join_pushed")
        assert resp.sql is not None
        assert _mentions_cast_key(resp.sql), resp.sql

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_cardinality_detection_profiles_a_computed_key(self, backend: str) -> None:
        """Spec: models/join-keys › The physical spelling is applied once, at emission (computed key in profiling)."""
        async with _engine(backend, tables=_K_TABLES, datasets=[_k_customers(), _k_orders()]) as engine:
            report = await engine.detect_relationship_cardinality(data_source="test", dataset="orders")
        (finding,) = [f for f in report.findings if f.to == "customers"]
        assert finding.verdict is not CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert finding.detected is RelationshipCardinality.MANY_TO_ONE

    def test_a_computed_key_is_not_skipped_by_profiling(self) -> None:
        """Spec: models/join-keys › The physical spelling is applied once, at emission."""
        assert _detection_skip_reason(dataset=_k_orders(), target=_k_customers(computed_pk=True),
                                      src_cols=["customer_key"], tgt_cols=["id"]) is None

    def test_a_computed_source_key_constructs(self) -> None:
        """Spec: models/join-keys › A computed source key joins on its expression (the model saves)."""
        orders = _k_orders()
        assert orders.relationships[0].column_pairs == [("customer_key", "id")]

    def test_an_expression_source_key_reading_another_dataset_is_rejected(self) -> None:
        """Spec: models/join-keys › An expression source key is rejected at construction."""
        with pytest.raises(ValueError, match="key 'ck'"):
            _k_orders(_f("ck", DataType.INTEGER, expression="customers.id"))

    def test_a_filtered_source_key_is_rejected(self) -> None:
        """Spec: models/join-keys › A filtered source key is rejected at construction."""
        with pytest.raises(ValueError, match="key 'customer_key'"):
            _k_orders(_f("customer_key", DataType.INTEGER, expression=_CAST_KEY, filter="status = 'ok'"))

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_unresolvable_target_key_fails_closed_at_emission(self, backend: str) -> None:
        """Spec: models/join-keys › An unresolvable target key fails closed at emission."""
        orders = _ds("orders", _f("id", DataType.INTEGER, expression="order_pk", pk=True),
                     _f("cust_fk"), _f("amount", DataType.FLOAT),
                     relationships=[DatasetRelationship(to="customers", from_columns=["cust_fk"],
                                                        to_columns=["cust_key"])])
        async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, _K_TABLES),
                                      datasets=[_k_customers(), orders]) as (engine, _db):
            with pytest.raises(RelationshipKeyError, match="'cust_key'"):
                await engine.execute({"population": "orders", "dimensions": ["customers.name"],
                                      "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]}, dry_run=True)


# --------------------------------------------------------------------------- #
# Edge names
# --------------------------------------------------------------------------- #
_REGIONS = TableSpec(name="regions", columns=[("region_id", "TEXT"), ("name", "TEXT")],
                     rows=[("R1", "north"), ("R2", "south")])
_CUSTOMERS = TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("name", "TEXT")],
                       rows=[("C1", "ann"), ("C2", "bob"), ("C3", "cid")])
_REFUNDS = TableSpec(name="refunds", columns=[("refund_id", "TEXT"), ("label", "TEXT")],
                     rows=[("F1", "small"), ("F2", "large")])
_RETURNS = TableSpec(name="returns", columns=[("return_id", "TEXT"), ("amount", "DOUBLE")],
                     rows=[("T1", 5.0), ("T2", 7.0)])
_ORDERS = TableSpec(
    name="orders",
    columns=[("order_id", "TEXT"), ("customer_id", "TEXT"), ("region_id", "TEXT"), ("refund_id", "TEXT"),
             ("a_id", "TEXT"), ("b_id", "TEXT"), ("amount", "DOUBLE")],
    rows=[("O1", "C1", "R2", "F1", "C1", "C2", 10.0), ("O2", "C1", "R1", "F1", "C1", "C2", 20.0),
          ("O3", "C2", "R2", "F2", "C2", "C3", 30.0), ("O4", "C3", "R2", "F2", "C3", "C1", 40.0)],
)
_E_TABLES = [_REGIONS, _CUSTOMERS, _REFUNDS, _RETURNS, _ORDERS]
_AMOUNT_BY_REGION = {"north": 20.0, "south": 80.0}
_AMOUNT_BY_CUSTOMER_NAME = {"ann": 30.0, "bob": 30.0, "cid": 40.0}
_AMOUNT_BY_REFUND = {"small": 30.0, "large": 70.0}


def _regions() -> SlayerDataset:
    return _ds("regions", _f("region_id", pk=True), _f("name"))


def _customers() -> SlayerDataset:
    return _ds("customers", _f("customer_id", pk=True), _f("name"))


def _refunds() -> SlayerDataset:
    return _ds("refunds", _f("refund_id", pk=True), _f("label"))


def _returns() -> SlayerDataset:
    return _ds("returns", _f("return_id", pk=True), _f("amount", DataType.FLOAT))


def _orders(*relationships: DatasetRelationship) -> SlayerDataset:
    return _ds("orders", _f("order_id", pk=True), _f("customer_id"), _f("region_id"), _f("refund_id"),
               _f("a_id"), _f("b_id"), _f("amount", DataType.FLOAT), relationships=list(relationships))


_AMT = {"metric": "sum(orders.amount)", "alias": "amt"}


class TestEdgeNames:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_edge_named_like_an_existing_dataset_saves_and_resolves_to_the_edge(self, backend: str) -> None:
        """Spec: models/join-traversal › Name colliding with a model is rejected (now: saves, resolves to the edge)."""
        async with _engine(backend, tables=_E_TABLES, datasets=[_regions(), _customers()]) as engine:
            await engine.save_dataset(_orders(_rel("regions", "region_id", "region_id", name="customers")))
            resp = await _run(engine, {"population": "orders", "dimensions": ["orders.customers.name"],
                                       "metrics": [_AMT]})
        assert cells(resp, dim_suffix="customers.name", metric="amt") == _AMOUNT_BY_REGION

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_dataset_named_like_an_existing_edge_saves(self, backend: str) -> None:
        """Spec: models/join-traversal › Model named like an existing edge is rejected (now: saves, both resolve)."""
        datasets = [_refunds(), _orders(_rel("refunds", "refund_id", "refund_id", name="returns"))]
        async with _engine(backend, tables=_E_TABLES, datasets=datasets) as engine:
            await engine.save_dataset(_returns())
            via_edge = await _run(engine, {"population": "orders", "dimensions": ["orders.returns.label"],
                                           "metrics": [_AMT]})
            anchor = await _run(engine, {"population": "returns", "metrics": [{"metric": "sum(returns.amount)", "alias": "ret"}]})
        assert cells(via_edge, dim_suffix="returns.label", metric="amt") == _AMOUNT_BY_REFUND
        (row,) = anchor.data
        assert float(row["ret"]) == 12.0

    async def test_unnamed_parallel_edges_warn(self) -> None:
        """Spec: models/join-traversal › Unnamed parallel edges warn."""
        async with _engine("sqlite", tables=_E_TABLES, datasets=[_customers()]) as engine:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.save_dataset(_orders(_rel("customers", "a_id", "customer_id", name="bill"),
                                                  _rel("customers", "b_id", "customer_id")))
            assert await engine.storage.get_dataset("orders", data_source="test") is not None
        assert any("customers" in str(w.message) and "ambiguous" in str(w.message).lower() for w in caught), \
            [str(w.message) for w in caught]

    async def test_duplicate_incident_edge_names_are_rejected(self) -> None:
        """Spec: models/join-traversal › Duplicate incident edge names are rejected."""
        async with _engine("sqlite", tables=_E_TABLES, datasets=[_customers()]) as engine:
            with pytest.raises(ValueError, match="buyer"):
                await engine.save_dataset(_orders(_rel("customers", "a_id", "customer_id", name="buyer"),
                                                  _rel("customers", "b_id", "customer_id", name="buyer")))
            assert await engine.storage.get_dataset("orders", data_source="test") is None


class TestAmbiguousHop:
    @staticmethod
    def _orders() -> SlayerDataset:
        return _orders(_rel("regions", "region_id", "region_id", name="customers"),
                       _rel("customers", "customer_id", "customer_id", name="orders_to_customers"))

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_both_readings_exist(self, backend: str) -> None:
        """Spec: models/join-traversal › Both readings exist."""
        async with _engine(backend, tables=_E_TABLES, datasets=[_regions(), _customers()]) as engine:
            await engine.save_dataset(self._orders())
            with pytest.raises(error("AmbiguousHopError")) as info:
                await _run(engine, {"population": "orders", "dimensions": ["orders.customers.name"],
                                    "metrics": [_AMT]})
        assert isinstance(info.value, AmbiguousReferenceError)
        assert "customers" in str(info.value)
        assert "orders_to_customers" in str(info.value)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_the_other_edge_name_resolves_alone(self, backend: str) -> None:
        """Spec: models/join-traversal › A hop segment naming both an edge and another edge's target is ambiguous."""
        async with _engine(backend, tables=_E_TABLES, datasets=[_regions(), _customers()]) as engine:
            await engine.save_dataset(self._orders())
            resp = await _run(engine, {"population": "orders",
                                       "dimensions": ["orders.orders_to_customers.name"], "metrics": [_AMT]})
        assert cells(resp, dim_suffix="orders_to_customers.name", metric="amt") == _AMOUNT_BY_CUSTOMER_NAME


# --------------------------------------------------------------------------- #
# Self-relationships
# --------------------------------------------------------------------------- #
_EMPLOYEES = TableSpec(
    name="employees", columns=[("employee_id", "TEXT"), ("name", "TEXT"), ("department", "TEXT"),
                               ("manager_id", "TEXT")],
    rows=[("E1", "ann", "eng", None), ("E2", "bob", "eng", "E1"), ("E3", "cid", "ops", "E1"),
          ("E4", "dan", "ops", "E3"), ("E5", "eve", "ops", "E3")],
)
_DEPARTMENTS = TableSpec(name="departments", columns=[("department", "TEXT"), ("floor", "INT")],
                         rows=[("eng", 1), ("ops", 2)])
_S_TABLES = [_EMPLOYEES, _DEPARTMENTS]
_HEADCOUNT = {"eng": 2.0, "ops": 3.0}
_HEADCOUNT_BY_FLOOR = {1: 2.0, 2: 3.0}
_FLOOR_BY_NAME = {"ann": 1.0, "bob": 1.0, "cid": 2.0, "dan": 2.0, "eve": 2.0}


def _employees() -> SlayerDataset:
    return _ds("employees", _f("employee_id", pk=True), _f("name"), _f("department"), _f("manager_id"),
               relationships=[_rel("employees", "manager_id", "employee_id", name="manager"),
                              _rel("departments", "department", "department")])


def _departments() -> SlayerDataset:
    return _ds("departments", _f("department", pk=True), _f("floor", DataType.INTEGER))


_COUNT = {"metric": "count(employees.*)", "alias": "n"}


class TestSelfRelationship:
    def test_a_self_relationship_constructs(self) -> None:
        """Spec: models/join-traversal › Self-relationships are accepted and take no part in paths."""
        assert [j.to for j in _employees().relationships] == ["employees", "departments"]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_self_relationship_loads_and_stays_inert(self, backend: str) -> None:
        """Spec: models/join-traversal › A self-relationship loads and stays inert."""
        async with _engine(backend, tables=_S_TABLES, datasets=[_departments(), _employees()]) as engine:
            loaded = await engine.storage.get_dataset("employees", data_source="test")
            resp = await _run(engine, {"dimensions": ["employees.department"], "metrics": [_COUNT]})
        assert loaded is not None
        assert [j.name for j in loaded.relationships if j.to == "employees"] == ["manager"]
        assert cells(resp, dim_suffix="department", metric="n") == _HEADCOUNT

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_self_relationship_takes_no_part_in_route_search(self, backend: str) -> None:
        """Spec: models/join-traversal › Self-relationships are accepted and take no part in paths."""
        async with _engine(backend, tables=_S_TABLES, datasets=[_departments(), _employees()]) as engine:
            resp = await _run(engine, {"population": "employees", "dimensions": ["departments.floor"],
                                       "metrics": [_COUNT]})
        assert cells(resp, dim_suffix="floor", metric="n") == _HEADCOUNT_BY_FLOOR

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_self_relationship_takes_no_part_in_association(self, backend: str) -> None:
        """Spec: models/join-traversal › Self-relationships are accepted and take no part in paths."""
        async with _engine(backend, tables=_S_TABLES, datasets=[_departments(), _employees()]) as engine:
            resp = await _run(engine, {"population": "employees", "dimensions": ["employees.name"],
                                       "metrics": [{"metric": "sum(departments.floor)", "alias": "fl"}],
                                       "to_many_handling": "associate"})
        assert cells(resp, dim_suffix="employees.name", metric="fl") == _FLOOR_BY_NAME

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_spelling_the_self_relationship_fails(self, backend: str) -> None:
        """Spec: models/join-traversal › Spelling the self-relationship fails."""
        async with _engine(backend, tables=_S_TABLES, datasets=[_departments(), _employees()]) as engine:
            with pytest.raises(error("SelfRelationshipTraversalError")) as info:
                await _run(engine, {"population": "employees", "dimensions": ["employees.manager.name"],
                                    "metrics": [_COUNT]})
        assert "self-relationships are not traversable" in str(info.value).lower()


# --------------------------------------------------------------------------- #
# The same shapes through a stored Ossie document (datasource ``shop``)
# --------------------------------------------------------------------------- #
_EMPLOYEES_DOC = document_text(
    name="staff",
    datasets=[
        dataset("employees", primary_key=["employee_id"],
                fields=[field("employee_id"), field("name"), field("department"), field("manager_id")]),
    ],
    relationships=[relationship("manager", src="employees", to="employees", from_columns=["manager_id"],
                                to_columns=["employee_id"])],
)

_EDGE_NAMED_DOC = document_text(
    name="geo",
    datasets=[
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("region_id"),
                        field("amount", datatype="Float")]),
        dataset("regions", primary_key=["region_id"], fields=[field("region_id"), field("name")]),
        dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("name")]),
    ],
    relationships=[relationship("customers", src="orders", to="regions", from_columns=["region_id"],
                                to_columns=["region_id"])],
)


class TestThroughAStoredDocument:
    @pytest.mark.parametrize("kind", ["yaml", "sqlite"])
    async def test_a_document_self_relationship_loads_and_stays_inert(self, kind: str) -> None:
        """Spec: models/join-traversal › A self-relationship loads and stays inert (Ossie document)."""
        async with document_engine(kind=kind, documents=(_EMPLOYEES_DOC,), tables=[_EMPLOYEES]) as engine:
            resp = await _run(engine, {"dimensions": ["employees.department"], "metrics": [_COUNT]})
        assert cells(resp, dim_suffix="department", metric="n") == _HEADCOUNT

    async def test_a_document_self_relationship_spelled_fails(self) -> None:
        """Spec: models/join-traversal › Spelling the self-relationship fails (Ossie document)."""
        async with document_engine(documents=(_EMPLOYEES_DOC,), tables=[_EMPLOYEES]) as engine:
            with pytest.raises(error("SelfRelationshipTraversalError")):
                await _run(engine, {"population": "employees", "dimensions": ["employees.manager.name"],
                                    "metrics": [_COUNT]})

    @pytest.mark.parametrize("kind", ["yaml", "sqlite"])
    async def test_a_document_edge_named_like_a_dataset_resolves_to_the_edge(self, kind: str) -> None:
        """Spec: models/join-traversal › Name colliding with a model is rejected (now: resolves, Ossie document)."""
        async with document_engine(kind=kind, documents=(_EDGE_NAMED_DOC,), tables=_E_TABLES) as engine:
            resp = await _run(engine, {"population": "orders", "dimensions": ["orders.customers.name"],
                                       "metrics": [_AMT]})
        assert cells(resp, dim_suffix="customers.name", metric="amt") == _AMOUNT_BY_REGION
