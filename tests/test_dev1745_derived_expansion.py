"""DEV-1745 (W8) — derived-column expansion must work when the whole
``Column.sql`` is a single bare reference to another derived column.

``_process_column_node_sync`` finishes by calling
``col.replace(exp.Paren(this=expanded_ast))``. When the fragment is exactly one
column reference, that ``exp.Column`` IS the root of the parsed tree and has no
parent, so sqlglot's ``replace`` is a no-op: the correctly-expanded inner SQL is
computed and then discarded, and ``expand_derived_refs_sync`` returns the
original text.

The emitted SQL then references a SLayer-derived column as if it were a
physical one (``customers__regions.pop_x2``), which no database can bind. Adding
any surrounding expression — even parentheses — makes it work, which is what
makes the defect easy to miss.

Not limited to cross-model: a same-model derived alias fails identically.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.column_expansion import expand_derived_refs_sync

from tests._engine_helpers import _engine_generate


# ---------------------------------------------------------------------------
# Fixtures — orders -> customers -> regions, with derived columns at each hop
# ---------------------------------------------------------------------------


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
            # derived ON regions, referencing a regions column BARE
            Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE),
            Column(name="is_live", sql="status = 'live'", type=DataType.BOOLEAN),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            # 'status' ALSO exists on regions — same-name collision shape
            Column(name="status", type=DataType.TEXT),
            Column(name="doubled", sql="amount * 2", type=DataType.DOUBLE),
            # --- the defect shapes: sql is ONE bare derived reference ---
            Column(name="alias_local", sql="doubled", type=DataType.DOUBLE),
            Column(name="deep_pop", sql="customers.regions.pop_x2",
                   type=DataType.DOUBLE),
            Column(name="deep_live", sql="customers.regions.is_live",
                   type=DataType.BOOLEAN),
            # --- controls: same reference inside a larger expression ---
            Column(name="deep_pop_compound", sql="customers.regions.pop_x2 * 1",
                   type=DataType.DOUBLE),
            Column(name="deep_pop_paren", sql="(customers.regions.pop_x2)",
                   type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )


_MODELS = {"orders": _orders(), "customers": _customers(), "regions": _regions()}


def _resolve(name: str):
    return _MODELS.get(name)


def _expand(sql: str) -> str:
    # The SAME instance ``_resolve`` hands back — the reference resolver walks
    # ``model``'s joins by identity, so passing a second, equal-but-distinct
    # ``_orders()`` would diverge the moment the root path resolves through
    # ``resolve_model``. ``owner_path=()`` (the default) roots at ``orders``.
    out = expand_derived_refs_sync(
        sql=sql, model=_MODELS["orders"], alias_path="orders",
        resolve_model=_resolve, dialect="postgres",
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
            f"derived column name leaked into emitted SQL: {out!r}"
        )
        assert "customers__regions.population" in out
        assert "* 2" in out

    def test_same_model_bare_derived_alias_expands(self) -> None:
        # Not a cross-model problem: a local derived alias fails identically.
        out = _expand("doubled")
        assert "doubled" not in out, (
            f"derived column name leaked into emitted SQL: {out!r}"
        )
        assert "orders.amount" in out
        assert "* 2" in out

    def test_boolean_derived_of_derived_expands(self) -> None:
        out = _expand("customers.regions.is_live")
        assert "is_live" not in out, (
            f"derived column name leaked into emitted SQL: {out!r}"
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
            query=query, model=_orders(), dialect="postgres", validate=False,
            extra_models=[_customers(), _regions()],
        )

    async def test_dimension_on_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "deep_pop", "name": "deep_pop"}],
            measures=[{"formula": "amount:sum", "name": "m0"}],
        ))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql
        # the join it crosses must still be present
        assert "JOIN regions" in sql

    async def test_measure_over_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "deep_pop:sum", "name": "m0"}],
        ))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql

    async def test_filter_on_derived_of_derived(self) -> None:
        sql = await self._sql(SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "amount:sum", "name": "m0"}],
            filters=["deep_pop > 100"],
        ))
        assert "pop_x2" not in sql, f"dangling derived reference:\n{sql}"
        assert "customers__regions.population" in sql

    async def test_local_derived_alias_dimension(self) -> None:
        sql = await self._sql(SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "alias_local", "name": "alias_local"}],
            measures=[{"formula": "amount:sum", "name": "m0"}],
        ))
        # 'doubled' is derived on orders; it must inline, not be referenced.
        # Today this emits a BARE, unqualified `doubled` — invalid on any DB.
        assert "doubled" not in sql, f"dangling derived reference:\n{sql}"
        assert "* 2" in sql, f"derived alias did not inline:\n{sql}"

    async def test_same_named_column_resolves_to_owning_model(self) -> None:
        """`status` exists on BOTH orders and regions. The derived
        `deep_live` (regions.is_live -> "status = 'live'") must resolve
        against REGIONS, not the root model."""
        sql = await self._sql(SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "deep_live", "name": "deep_live"}],
            measures=[{"formula": "amount:sum", "name": "m0"}],
        ))
        assert "customers__regions.status" in sql, (
            f"derived sql resolved against the wrong model:\n{sql}"
        )


# ---------------------------------------------------------------------------
# Execution — a dangling derived reference is not merely ugly, it does not run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_derived_of_derived_executes_on_duckdb() -> None:
    import duckdb

    sql = await _engine_generate(
        query=SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "deep_pop", "name": "deep_pop"}],
            measures=[{"formula": "amount:sum", "name": "m0"}],
        ),
        model=_orders(), dialect="duckdb", validate=False,
        extra_models=[_customers(), _regions()],
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
