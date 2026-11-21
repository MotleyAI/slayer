"""W8 — derived-field expansion must work when the whole
``DatasetField.sql`` is a single bare reference to another derived field.

``_process_column_node_sync`` finishes by calling
``col.replace(exp.Paren(this=expanded_ast))``. When the fragment is exactly one
column reference, that ``exp.Column`` IS the root of the parsed tree and has no
parent, so sqlglot's ``replace`` is a no-op: the correctly-expanded inner SQL is
computed and then discarded, and ``expand_derived_refs_sync`` returns the
original text.

The emitted SQL then references a SLayer-derived field as if it were a
physical column (``customers__regions.pop_x2``), which no database can bind. Adding
any surrounding expression — even parentheses — makes it work, which is what
makes the defect easy to miss.

Not limited to cross-dataset: a same-dataset derived alias fails identically.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.field_expansion import expand_derived_refs_sync

from tests._engine_helpers import _engine_generate


# ---------------------------------------------------------------------------
# Fixtures — orders -> customers -> regions, with derived fields at each hop
# ---------------------------------------------------------------------------


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="population", datatype=DataType.FLOAT),
            # derived ON regions, referencing a regions field BARE
            DatasetField(name="pop_x2", expression="population * 2", datatype=DataType.FLOAT),
            DatasetField(name="is_live", expression="status = 'live'", datatype=DataType.BOOLEAN),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            # 'status' ALSO exists on regions — same-name collision shape
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="doubled", expression="amount * 2", datatype=DataType.FLOAT),
            # --- the defect shapes: sql is ONE bare derived reference ---
            DatasetField(name="alias_local", expression="orders.doubled", datatype=DataType.FLOAT),
            DatasetField(name="deep_pop", expression="customers.regions.pop_x2",
                   datatype=DataType.FLOAT),
            DatasetField(name="deep_live", expression="customers.regions.is_live",
                   datatype=DataType.BOOLEAN),
            # --- controls: same reference inside a larger expression ---
            DatasetField(name="deep_pop_compound", expression="customers.regions.pop_x2 * 1",
                   datatype=DataType.FLOAT),
            DatasetField(name="deep_pop_paren", expression=ExpressionObject(dialects=[DialectEntry(dialect=d, expression="(customers.regions.pop_x2)") for d in ("DUCKDB", "POSTGRES")]),
                   datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
        )],
    )


_DATASETS = {"orders": _orders(), "customers": _customers(), "regions": _regions()}


def _expand(sql: str) -> str:
    # The SAME ``_DATASETS`` instances — the reference resolver walks
    # ``dataset``'s relationships by identity, so passing a second, equal-but-distinct
    # ``_orders()`` would diverge the moment the root path resolves.
    # ``owner_path=()`` (the default) roots at ``orders``.
    out = expand_derived_refs_sync(
        sql=sql, dataset=_DATASETS["orders"], alias_path="orders",
        datasets_by_name=_DATASETS, dialect="postgres",
    )
    assert out is not None, f"expansion returned None for {sql!r}"
    return out


# ---------------------------------------------------------------------------
# Unit level — the expander itself
# ---------------------------------------------------------------------------


class TestBareDerivedReferenceExpands:
    """The bare-reference-is-the-whole-fragment cases that silently no-op."""

    def test_cross_model_two_hop_bare_reference_expands(self) -> None:
        # regions.pop_x2 is DERIVED ("population * 2") — it is not a real
        # column of the regions table, so leaving the reference intact emits
        # SQL no database can bind.
        out = _expand("customers.regions.pop_x2")
        assert "pop_x2" not in out, (
            f"derived field name leaked into emitted SQL: {out!r}"
        )
        assert "customers__regions.population" in out
        assert "* 2" in out

    def test_same_model_bare_derived_alias_expands(self) -> None:
        # Not a cross-dataset problem: a local derived alias fails identically.
        out = _expand("orders.doubled")
        assert "doubled" not in out, (
            f"derived field name leaked into emitted SQL: {out!r}"
        )
        assert "orders.amount" in out
        assert "* 2" in out

    def test_boolean_derived_of_derived_expands(self) -> None:
        out = _expand("customers.regions.is_live")
        assert "is_live" not in out, (
            f"derived field name leaked into emitted SQL: {out!r}"
        )
        assert "customers__regions.status" in out


class TestCompoundControlsStillWork:
    """These already work today and must not regress — they are why the
    defect is easy to miss."""

    def test_compound_expression_expands(self) -> None:
        out = _expand("customers.regions.pop_x2 * 1")
        assert "pop_x2" not in out
        assert "customers__regions.population" in out

    def test_parenthesised_reference_expands(self) -> None:
        out = _expand("(customers.regions.pop_x2)")
        assert "pop_x2" not in out
        assert "customers__regions.population" in out

    def test_physical_column_reference_is_only_qualified(self) -> None:
        # A physical (non-derived) column is qualified, never inlined.
        out = _expand("amount")
        assert out.strip() in {"orders.amount", '"orders".amount'}


# ---------------------------------------------------------------------------
# End-to-end emission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDerivedOfDerivedEmission:

    async def _sql(self, query: SlayerQuery) -> str:
        return await _engine_generate(
            query=query, dataset=_orders(), dialect="postgres", validate=False,
            extra_datasets=[_customers(), _regions()],
        )

    async def test_dimension_on_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.deep_pop", "alias": "deep_pop"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "m0"}],
        }))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql
        # the join it crosses must still be present
        assert "JOIN regions" in sql

    async def test_measure_over_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.status", "alias": "status"}],
            "metrics": [{"metric": "sum(orders.deep_pop)", "alias": "m0"}],
        }))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql

    async def test_filter_on_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.status", "alias": "status"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "m0"}],
            "filter": ["orders.deep_pop > 100"],
        }))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql

    async def test_local_derived_alias_dimension(self) -> None:
        sql = await self._sql(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.alias_local", "alias": "alias_local"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "m0"}],
        }))
        # 'doubled' is derived on orders; it must inline, not be referenced.
        # Today this emits a BARE, unqualified `doubled` — invalid on any DB.
        assert "doubled" not in sql, f"dangling derived reference:\n{sql}"
        assert "* 2" in sql, f"derived alias did not inline:\n{sql}"

    async def test_same_named_column_resolves_to_owning_model(self) -> None:
        """`status` exists on BOTH orders and regions. The derived
        `deep_live` (regions.is_live -> "status = 'live'") must resolve
        against REGIONS, not the root dataset."""
        sql = await self._sql(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.deep_live", "alias": "deep_live"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "m0"}],
        }))
        assert "customers__regions.status" in sql, (
            f"derived sql resolved against the wrong dataset:\n{sql}"
        )


# ---------------------------------------------------------------------------
# Execution — a dangling derived reference is not merely ugly, it does not run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_derived_of_derived_executes_on_duckdb() -> None:
    duckdb = pytest.importorskip("duckdb")
    sql = await _engine_generate(
        query=SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.deep_pop", "alias": "deep_pop"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "m0"}],
        }),
        dataset=_orders(), dialect="duckdb", validate=False,
        extra_datasets=[_customers(), _regions()],
    )
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE orders(id INT, customer_id INT, amount DOUBLE, status VARCHAR)"
        )
        con.execute("CREATE TABLE customers(id INT, region_id INT)")
        con.execute(
            "CREATE TABLE regions(id INT, status VARCHAR, population DOUBLE)"
        )
        con.execute("INSERT INTO orders VALUES (1, 1, 10.0, 'ok')")
        con.execute("INSERT INTO customers VALUES (1, 1)")
        con.execute("INSERT INTO regions VALUES (1, 'live', 50.0)")

        rows = con.execute(sql).fetchall()
    # regions.population = 50 -> pop_x2 = 100
    assert rows == [(100.0, 10.0)], f"unexpected rows {rows!r} for SQL:\n{sql}"
