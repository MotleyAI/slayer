"""``_emit_step_cte`` is the one shell shared by the four
transform-chain step-CTE sites (window / unmaterialised-POST, in the host and
cross-dataset chains). These pin the shell directly and deterministically:

* the multi-slot-in-one-batch body, pinned in caller-supplied slot order;
* a step's own aliases publish after its SELECT, so siblings never read each other;
* block D (F2 unmaterialised-POST), which no real query reaches — it errors
  earlier in ``_render_outer_composite`` — so it has no golden pin; the shell it
  would use is pinned here, and block C (F1 unmaterialised-POST) covers the same
  shell end-to-end via ``chain/local_multi_step``.
"""

from __future__ import annotations

from decimal import Decimal

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.keys import (
    ArithmeticKey,
    FieldKey,
    Phase,
    LiteralKey,
    ScalarCallKey,
    TransformKey,
    ValueKey,
)
from slayer.ir.planned import PlannedQuery, Stage, StageKind, ValueSlot
from slayer.sql.generator import SQLGenerator
from slayer.sql.render.cte_assembly import CteEntry


def _gen() -> SQLGenerator:
    return SQLGenerator(dialect="postgres")


def _base_ctes() -> list[CteEntry]:
    return [CteEntry(name="base", query=exp.Select().select(exp.column("x")).from_("t"))]


def _slot(name: str, *, type_: DataType | None = None, public_aliases: list[str] | None = None) -> ValueSlot:
    return ValueSlot(
        id=name, key=FieldKey(path=(), leaf=name), declared_name=name,
        public_aliases=[name] if public_aliases is None else public_aliases, phase=Phase.ROW,
        type=type_, type_is_explicit=type_ is not None,
    )


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
    assert 'AS "s1"' in sql        # rendered column
    assert "FROM base" in sql
    assert aliases["sid1"] == ["s1"]
    assert avail["sid1"] == "s1"


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
    assert '"a"' in sql
    assert '"b"' in sql
    # Deterministic: the caller's slot order is the emitted column order.
    assert sql.index('"a"') < sql.index('"b"')


def test_step_aliases_publish_after_its_select() -> None:
    gen = _gen()
    avail: dict = {}
    observed: list[dict] = []

    def render(_slot_obj) -> Expression:
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
    # B's render sees neither: a step's own aliases publish after its SELECT.
    assert "A" not in observed[1]
    assert "B" not in observed[1]


def test_multi_alias_slot_emits_one_column_per_public_alias() -> None:
    # A slot projected under two names surfaces both, rendered
    # once and emitted per name; the FIRST name stays the canonical handle.
    gen = _gen()
    ctes = _base_ctes()
    aliases: dict = {}
    avail: dict = {}
    calls: list = []

    def render(slot_obj) -> Expression:
        calls.append(slot_obj)
        return exp.column("v", quoted=True)

    gen._emit_step_cte(
        ctes=ctes,
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id=aliases,
        available_alias_by_slot_id=avail,
        source_relation="orders",
        slot_entries=[(
            "sid",
            _slot("a", public_aliases=["a", "b"]),
        )],
        render=render,
    )
    projection = [e.alias_or_name for e in ctes[-1].query.expressions]
    assert projection == ["a", "b"]
    assert aliases["sid"] == ["a", "b"]
    assert avail["sid"] == "a"
    assert len(calls) == 1


def test_no_public_aliases_falls_back_to_declared_name() -> None:
    gen = _gen()
    ctes = _base_ctes()
    aliases: dict = {}
    gen._emit_step_cte(
        ctes=ctes,
        chain_tail="base",
        step_num=0,
        cte_allocator=gen._new_allocator(),
        aliases_by_slot_id=aliases,
        available_alias_by_slot_id={},
        source_relation="orders",
        slot_entries=[(
            "sid",
            _slot("d", public_aliases=[]),
        )],
        render=lambda s: exp.column("v", quoted=True),
    )
    assert 'AS "orders.d"' in ctes[-1].query.sql(dialect="postgres")
    assert aliases["sid"] == ["orders.d"]


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
        slot_entries=[("sid", _slot("s", type_=DataType.FLOAT))],
        render=lambda s: exp.Abs(this=exp.column("v", quoted=True)),
    )
    assert "CAST" in ctes[-1].query.sql(dialect="postgres").upper()


def _post_slot(slot_id: str, key: ValueKey, *, needs_field: bool = False) -> ValueSlot:
    stage = Stage(kind=StageKind.DERIVED, level=1)
    return ValueSlot(id=slot_id, key=key, declared_name=slot_id, phase=Phase.ROW, stage=stage, needs_field=needs_field)


def test_unmaterialised_post_slots_detects_arith_and_scalar_call() -> None:
    # The detection loop feeding block C (F1) and block D (F2): DERIVED
    # Arithmetic and ScalarCall slots needing a column are detected; TransformKey
    # (materialised by a layer), an already-materialised slot, a mask-only slot
    # (needs_field=False), and a plain FieldKey are skipped.
    arith = _post_slot("arith", ArithmeticKey(op="-", operands=(LiteralKey(value=Decimal(1)),)), needs_field=True)
    scalar = _post_slot("scalar", ScalarCallKey(name="abs", args=(FieldKey(leaf="x"),)), needs_field=True)
    transform = _post_slot("xf", TransformKey(op="cumsum", input=FieldKey(leaf="x")))
    materialised = _post_slot("done", ArithmeticKey(op="+", operands=(LiteralKey(value=Decimal(2)),)))
    mask_only = _post_slot("mask", ArithmeticKey(op=">", operands=(LiteralKey(value=Decimal(3)),)))
    column = _post_slot("col", FieldKey(leaf="y"))
    pq = PlannedQuery(
        source_relation="orders",
        combined_expression_slots=[arith, scalar, transform, materialised, mask_only, column],
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
