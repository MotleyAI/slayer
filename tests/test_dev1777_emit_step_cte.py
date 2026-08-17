"""DEV-1777 sub-item 1: ``_emit_step_cte`` is the one shell shared by the four
transform-chain step-CTE sites (window / unmaterialised-POST, in the host and
cross-model chains). These pin the shell directly and deterministically:

* the multi-slot-in-one-batch body (the caller controls slot order, so unlike
  an end-to-end golden this is not exposed to the base-CTE column-order
  non-determinism tracked in DEV-1795);
* the render-before-mutate ordering invariant a later same-step slot relies on;
* block D (F2 unmaterialised-POST), which no real query reaches — it errors
  earlier in ``_render_outer_composite`` — so it has no golden pin; the shell it
  would use is pinned here, and block C (F1 unmaterialised-POST) covers the same
  shell end-to-end via ``chain/local_multi_step``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.keys import (
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    ScalarCallKey,
    TransformKey,
)
from slayer.sql.generator import SQLGenerator
from slayer.sql.render.cte_assembly import CteEntry


def _gen() -> SQLGenerator:
    return SQLGenerator(dialect="postgres")


def _base_ctes() -> list[CteEntry]:
    return [CteEntry(name="base", query=exp.Select().select(exp.column("x")).from_("t"))]


def _slot(name: str, *, type_=None) -> SimpleNamespace:
    return SimpleNamespace(public_aliases=[name], declared_name=name, type=type_)


def test_single_slot_emits_step_cte_and_advances_chain() -> None:
    gen = _gen()
    ctes = _base_ctes()
    aliases = {"d0": ["orders.d0"]}
    avail = {"d0": "orders.d0"}
    new_tail, new_step_num = gen._emit_step_cte(
        ctes=ctes,
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id=aliases,
        available_alias_by_slot_id=avail,
        source_relation="orders",
        slot_entries=[("sid1", _slot("s1"))],
        render=lambda s: exp.column("v", quoted=True),
    )
    assert (new_tail, new_step_num) == ("step1", 1)
    assert ctes[-1].name == "step1"
    assert ctes[-1].depends_on == ["base"]
    sql = ctes[-1].query.sql(dialect="postgres")
    assert '"orders.d0"' in sql          # carried alias, in plan order
    assert 'AS "orders.s1"' in sql        # rendered column, source-qualified
    assert "FROM base" in sql
    assert aliases["sid1"] == ["orders.s1"]
    assert avail["sid1"] == "orders.s1"


def test_multi_slot_one_batch_preserves_caller_order() -> None:
    gen = _gen()
    ctes = _base_ctes()
    new_tail, _ = gen._emit_step_cte(
        ctes=ctes,
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id={},
        available_alias_by_slot_id={},
        source_relation="orders",
        slot_entries=[("A", _slot("a")), ("B", _slot("b"))],
        render=lambda s: exp.column("v", quoted=True),
    )
    assert new_tail == "step1"
    sql = ctes[-1].query.sql(dialect="postgres")
    assert '"orders.a"' in sql
    assert '"orders.b"' in sql
    # Deterministic: the caller's slot order is the emitted column order.
    assert sql.index('"orders.a"') < sql.index('"orders.b"')


def test_render_runs_before_alias_map_mutation_per_slot() -> None:
    gen = _gen()
    avail: dict = {}
    observed: list[dict] = []

    def render(_slot_obj) -> exp.Expression:
        observed.append(dict(avail))  # snapshot at render time
        return exp.column("v", quoted=True)

    gen._emit_step_cte(
        ctes=_base_ctes(),
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id={},
        available_alias_by_slot_id=avail,
        source_relation="orders",
        slot_entries=[("A", _slot("a")), ("B", _slot("b"))],
        render=render,
    )
    # A's own alias is not yet in the map when A renders.
    assert "A" not in observed[0]
    # B's render sees A (materialised after A's render) but not itself.
    assert "A" in observed[1]
    assert "B" not in observed[1]


def test_typed_slot_is_cast_wrapped() -> None:
    # _wrap_cast_for_type skips a bare exp.Column, so render a non-column
    # expression to exercise the type-enforcing CAST the helper applies.
    gen = _gen()
    ctes = _base_ctes()
    gen._emit_step_cte(
        ctes=ctes,
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id={},
        available_alias_by_slot_id={},
        source_relation="orders",
        slot_entries=[("sid", _slot("s", type_=DataType.DOUBLE))],
        render=lambda s: exp.func("ABS", exp.column("v", quoted=True)),
    )
    assert "CAST" in ctes[-1].query.sql(dialect="postgres").upper()


def test_unmaterialised_post_slots_detects_arith_and_scalar_call() -> None:
    # The detection loop feeding block C (F1) and block D (F2): Arithmetic and
    # ScalarCall POST slots are detected; TransformKey (materialised by a layer),
    # an already-materialised slot, and a plain ColumnKey are skipped.
    arith = SimpleNamespace(
        id="arith", key=ArithmeticKey(op="-", operands=(LiteralKey(value=Decimal(1)),)),
    )
    scalar = SimpleNamespace(
        id="scalar", key=ScalarCallKey(name="abs", args=(ColumnKey(leaf="x"),)),
    )
    transform = SimpleNamespace(
        id="xf", key=TransformKey(op="cumsum", input=ColumnKey(leaf="x")),
    )
    materialised = SimpleNamespace(
        id="done", key=ArithmeticKey(op="+", operands=(LiteralKey(value=Decimal(2)),)),
    )
    column = SimpleNamespace(id="col", key=ColumnKey(leaf="y"))
    pq = SimpleNamespace(
        combined_expression_slots=[arith, scalar, transform, materialised, column],
    )
    out = SQLGenerator._unmaterialised_post_slots(pq, {"done": ["orders.done"]})
    assert [s.id for s in out] == ["arith", "scalar"]


def test_step_num_and_names_increment_across_calls() -> None:
    gen = _gen()
    ctes = _base_ctes()
    alloc = gen._new_allocator()
    tail, n = gen._emit_step_cte(
        ctes=ctes, chain_tail="base", step_num=0, cte_allocator=alloc,
        aliases_by_slot_id={}, available_alias_by_slot_id={},
        source_relation="orders", slot_entries=[("A", _slot("a"))],
        render=lambda s: exp.column("v", quoted=True),
    )
    tail2, n2 = gen._emit_step_cte(
        ctes=ctes, chain_tail=tail, step_num=n, cte_allocator=alloc,
        aliases_by_slot_id={"A": ["orders.a"]}, available_alias_by_slot_id={"A": "orders.a"},
        source_relation="orders", slot_entries=[("B", _slot("b"))],
        render=lambda s: exp.column("v", quoted=True),
    )
    assert (tail, n) == ("step1", 1)
    assert (tail2, n2) == ("step2", 2)
    assert ctes[-1].depends_on == ["step1"]  # chained onto the prior step
