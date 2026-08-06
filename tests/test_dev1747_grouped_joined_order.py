"""DEV-1747 / DEV-1735 — grouped ORDER BY on a JOINED row column.

Today a grouped query whose sort key is an unprojected JOINED column raises
``UnresolvableOrderColumnError`` (``stage_planner.py``), because an
``AggregateKey`` with a non-empty ``source.path`` always routes to a
TARGET-rooted CTE, which for a host-grain sort key degenerates to a scalar
CROSS JOIN — every group would get the same global value and the sort would
silently do nothing.

DEV-1747 gives the wrap a structural marker (``AggregateKey.grain == "host"``)
that routes it to the DEV-1709 HOST-ROOTED isolated CTE instead: the crossed
join is pulled inside that CTE, the wrap is computed there, the CTE is grouped
on the query grain, and it joins back null-safe. The reference shape already
exists in production for a crossing DERIVED column (see
``test_dev1747_derived_crossing_order.py``); this module holds the bare joined
column to the same contract.

Two things are asserted that SQL-shape alone cannot establish:

* **Per-group, not global.** The corpus gives group ``A`` two different regions
  and group ``B`` one, so a scalar CROSS JOIN would order every group by one
  constant. Executed row order proves it did not.
* **Containment.** ``order_tags`` is 1:N against ``orders`` (order 1 carries
  three tags). If the sort key's join were pulled into the host base, the
  sibling ``amount:sum`` would multiply. Executed values prove it did not, and
  the base's own JOIN list proves why.

D10: the wrap is DIRECTION-AWARE — ``MIN`` on ASC, ``MAX`` on DESC — replacing
today's unconditional ``MAX``. The corpus is built so the two disagree: group
``A`` spans ``Alpha``..``Zulu`` and group ``B`` sits on ``Bravo`` between them,
so ASC-by-MIN yields ``[A, B]`` while ASC-by-MAX yields ``[B, A]``.

Refs: DEV-1747 (D2, D9, D10), DEV-1735, DEV-1709 (the host-rooted vehicle),
DEV-1742 P-C.
"""
from __future__ import annotations

import os
import tempfile

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from tests._dev1747_fixtures import (
    GROUP_A_AMOUNT,
    GROUP_B_AMOUNT,
    GROUP_NULL_AMOUNT,
    aggregate_calls_in,
    all_conjuncts_null_safe,
    base_from_join_aliases,
    cte_map,
    dev1747_models,
    grain_join_back_predicates,
    isolated_cte_bodies,
    make_sqlite_engine,
    order_by_text,
    order_terms,
    relation_names,
    response_column_values,
    seed_dev1747_sqlite,
)
from tests._engine_helpers import _engine_generate

_MEASURE = [{"formula": "amount:sum", "name": "rev"}]


def _wrap_functions(sql: str, column: str, *, dialect: str = "postgres") -> list[str]:
    """Aggregate functions applied to ``column`` — by AST, not by substring.

    ``"MIN(" in sql`` is satisfied by a MIN over ANY column, so it cannot
    distinguish "the sort key is wrapped in MIN" from "some unrelated measure
    is". D10 is precisely a claim about which function wraps which column.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [
        func for func, arg in aggregate_calls_in(tree, dialect=dialect)
        if column in arg
    ]


def _grouped_order_query(*, model: str, name: str, direction: str) -> SlayerQuery:
    """Grouped by ``orders.status``, ordered by an UNPROJECTED joined column."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=_MEASURE,
        order=[OrderItem(
            column=ColumnRef(name=name, model=model), direction=direction,
        )],
    )


async def _execute(query: SlayerQuery):
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "dev1747.db")
        seed_dev1747_sqlite(db)
        engine = await make_sqlite_engine(d, db)
        return await engine.execute(query)


async def _sql(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    models = dev1747_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:], dialect=dialect,
    )


# ---------------------------------------------------------------------------
# Group 1 — it resolves at all (the DEV-1735 headline)
# ---------------------------------------------------------------------------
class TestGroupedJoinedOrderResolves:
    async def test_one_hop_joined_sort_key_resolves(self) -> None:
        sql = await _sql(_grouped_order_query(
            model="customers", name="tier", direction="asc",
        ))
        assert order_by_text(sql), "grouped joined sort key produced no ORDER BY"

    async def test_multi_hop_joined_sort_key_resolves(self) -> None:
        sql = await _sql(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        assert order_by_text(sql), "multi-hop joined sort key produced no ORDER BY"

    async def test_sort_key_is_not_projected(self) -> None:
        """The wrap is HIDDEN — it must not leak into the public projection,
        or adding a sort silently adds a result column."""
        response = await _execute(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        assert response.data
        for row in response.data:
            assert set(row) == {"orders.status", "orders.rev"}, (
                f"hidden order wrap leaked into the result row: {sorted(row)}"
            )


# ---------------------------------------------------------------------------
# Group 2 — per-group, not a global constant (DEV-1735 acceptance)
# ---------------------------------------------------------------------------
class TestPerGroupSortKey:
    async def test_ascending_uses_each_group_own_minimum(self) -> None:
        """Group A spans Alpha..Zulu, group B sits on Bravo, group N is NULL.

        ASC by each group's MIN → Alpha < Bravo < NULL(last) → A, B, N.
        A global scalar would leave the groups in their unsorted order, and
        the old unconditional MAX would give B, A, N — both distinguishable.
        """
        response = await _execute(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        assert response_column_values(response.data, "orders.status") == ["A", "B", "N"]

    async def test_descending_uses_each_group_own_maximum(self) -> None:
        """DESC by each group's MAX → Zulu > Bravo > NULL(last) → A, B, N."""
        response = await _execute(_grouped_order_query(
            model="customers.regions", name="name", direction="desc",
        ))
        assert response_column_values(response.data, "orders.status") == ["A", "B", "N"]

    async def test_sibling_measure_values_are_untouched(self) -> None:
        """Adding a sort key must not change any other field's value — the
        core principle. Distinct per group so a wrong column cannot match."""
        response = await _execute(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        by_status = {r["orders.status"]: r["orders.rev"] for r in response.data}
        assert by_status == {
            "A": GROUP_A_AMOUNT, "B": GROUP_B_AMOUNT, "N": GROUP_NULL_AMOUNT,
        }

    async def test_host_cardinality_is_unchanged(self) -> None:
        response = await _execute(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        assert len(response.data) == 3


# ---------------------------------------------------------------------------
# Group 3 — D10 direction-aware MIN/MAX
# ---------------------------------------------------------------------------
class TestDirectionAwareWrap:
    async def test_ascending_emits_min_not_max(self) -> None:
        sql = await _sql(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        assert _wrap_functions(sql, "customers__regions.name") == ["MIN"], (
            f"ASC must wrap THE SORT KEY in MIN and nothing else (D10); got:\n{sql}"
        )

    async def test_descending_emits_max(self) -> None:
        sql = await _sql(_grouped_order_query(
            model="customers.regions", name="name", direction="desc",
        ))
        assert _wrap_functions(sql, "customers__regions.name") == ["MAX"], (
            f"DESC must wrap THE SORT KEY in MAX (D10); got:\n{sql}"
        )

    async def test_local_row_column_wrap_is_direction_aware_too(self) -> None:
        """D10 also changes the EXISTING local grouped wrap, which has emitted
        an unconditional ``MAX`` since DEV-1703 Phase 1. Both shapes must agree
        or the two order paths mean different things by ``ASC``."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(
                column=ColumnRef(name="created_at"), direction="asc",
            )],
        ))
        assert _wrap_functions(sql, "created_at") == ["MIN"], (
            f"local grouped ASC wrap must be MIN over created_at (D10); got:\n{sql}"
        )

    async def test_two_directions_on_one_column_are_distinct_slots(self) -> None:
        """``ORDER BY a ASC, a DESC`` needs MIN(a) and MAX(a) — two different
        aggregates over one column. The order-key remap must therefore be
        keyed by (key, direction), not by key alone, or the second entry
        silently reuses the first's slot.

        Asserted per COLUMN: a statement-wide "contains MIN and MAX" would also
        pass if both wraps landed on the same column, which is the bug."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[
                OrderItem(column=ColumnRef(name="created_at"), direction="asc"),
                OrderItem(column=ColumnRef(name="amount"), direction="desc"),
            ],
        ))
        assert _wrap_functions(sql, "created_at") == ["MIN"], f"\n{sql}"
        assert "MAX" in _wrap_functions(sql, "amount"), f"\n{sql}"


# ---------------------------------------------------------------------------
# Group 4 — fan-out containment (P-C)
# ---------------------------------------------------------------------------
class TestFanoutContainment:
    """``order_tags`` is 1:N — order 1 carries three tags. Ordering by a tag
    name must not multiply the sibling ``amount:sum``."""

    def _tag_ordered(self, direction: str = "asc") -> SlayerQuery:
        return _grouped_order_query(
            model="order_tags", name="name", direction=direction,
        )

    async def test_sibling_sum_is_not_multiplied_by_the_fanout(self) -> None:
        response = await _execute(self._tag_ordered())
        by_status = {r["orders.status"]: r["orders.rev"] for r in response.data}
        assert by_status == {
            "A": GROUP_A_AMOUNT, "B": GROUP_B_AMOUNT, "N": GROUP_NULL_AMOUNT,
        }, (
            "sibling measure changed when a 1:N sort key was added — the "
            "crossed join leaked into the host base (P-C violation)."
        )

    async def test_row_count_is_unchanged_by_the_fanout(self) -> None:
        response = await _execute(self._tag_ordered())
        assert len(response.data) == 3

    async def test_host_base_does_not_join_the_sort_key_table(self) -> None:
        """The structural reason the values above hold: the crossed join lives
        inside the isolated CTE, never in the base."""
        sql = await _sql(self._tag_ordered())
        assert "order_tags" not in base_from_join_aliases(sql), (
            f"order_tags joined into the host base — fan-out is not contained:\n{sql}"
        )

    async def test_the_isolated_cte_is_where_the_fanout_join_lives(self) -> None:
        """The other half of containment. "Absent from the base" alone is also
        satisfied by the join disappearing altogether — which would leave the
        sort key resolving to nothing rather than being contained."""
        sql = await _sql(self._tag_ordered())
        bodies = isolated_cte_bodies(sql)
        assert bodies, f"no isolated CTE was emitted at all:\n{sql}"
        holders = [
            name for name, body in bodies.items()
            if "order_tags" in relation_names(body)
        ]
        assert holders, (
            f"no isolated CTE joins order_tags — the sort key's rows are "
            f"nowhere:\n{sql}"
        )

    async def test_sort_key_still_orders_correctly_under_fanout(self) -> None:
        """Containment must not cost correctness: MIN tag per group is
        fragile(A) < sale(B) < trial(N)."""
        response = await _execute(self._tag_ordered())
        assert response_column_values(response.data, "orders.status") == ["A", "B", "N"]


# ---------------------------------------------------------------------------
# Group 5 — the isolated-CTE shape
# ---------------------------------------------------------------------------
class TestIsolatedCteShape:
    async def test_crossed_join_lives_in_a_cte_not_the_base(self) -> None:
        sql = await _sql(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        base_joins = base_from_join_aliases(sql)
        assert "customers__regions" not in base_joins, (
            f"the sort key's join was pulled into the host base:\n{sql}"
        )
        assert "customers__regions" in sql, (
            f"the crossed join is missing entirely — the sort key cannot "
            f"resolve:\n{sql}"
        )

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "tsql"])
    async def test_join_back_is_null_safe(self, dialect: str) -> None:
        """A NULL grain member must still join back (P-I). The corpus has no
        NULL status, so this asserts the emitted PREDICATE rather than a row —
        the executed NULL-grain case is owned by the DEV-1746 suite.

        Asserted on the AST of the isolated CTE's own ``ON`` clause: a text
        search for ``" IS "`` also matches an ``IS NULL`` anywhere else in the
        statement, so it would pass on a plain ``=`` join-back. The three
        dialects cover the three spellings ``build_null_safe_eq`` emits.
        """
        sql = await _sql(
            _grouped_order_query(
                model="customers.regions", name="name", direction="asc",
            ),
            dialect=dialect,
        )
        predicates = grain_join_back_predicates(sql, dialect=dialect)
        assert predicates, (
            f"no isolated CTE is joined back at all on {dialect}:\n{sql}"
        )
        for predicate in predicates:
            assert all_conjuncts_null_safe(predicate), (
                f"grain join-back is not null-safe on {dialect}: "
                f"{predicate.sql(dialect=dialect)}\n{sql}"
            )

    @pytest.mark.parametrize("dialect", ["tsql", "bigquery"])
    async def test_grouped_joined_sort_key_survives_a_dialect_round_trip(
        self, dialect: str,
    ) -> None:
        """T-SQL and BigQuery mangle dotted aliases; the wrap's internal alias
        must survive both (§5.13).

        ``assert sql`` would pass on any non-empty string, including one whose
        ORDER BY was dropped — so this parses the emitted SQL back and requires
        a sort term that names a relation the statement actually defines."""
        sql = await _sql(
            _grouped_order_query(
                model="customers.regions", name="name", direction="asc",
            ),
            dialect=dialect,
        )
        terms = order_terms(sql, dialect=dialect)
        assert terms, f"{dialect} lost the ORDER BY:\n{sql}"
        tree = sqlglot.parse_one(sql, dialect=dialect)
        order = tree.find(exp.Order)
        assert order is not None
        qualifiers = {
            column.table for column in order.find_all(exp.Column) if column.table
        }
        known = set(cte_map(sql, dialect=dialect)) | relation_names(tree)
        assert qualifiers <= known, (
            f"the sort term is qualified by {qualifiers - known}, which is not "
            f"a relation in scope — a dotted alias was re-read as a multi-part "
            f"reference:\n{sql}"
        )

    async def test_the_wrap_is_computed_inside_the_cte_over_the_joined_relation(
        self,
    ) -> None:
        """The generator's local-aggregate walkers skip path-bearing sources
        today, which is what would leave a ``grain="host"`` wrap unrendered.

        The observable contract: the aggregate is evaluated INSIDE the isolated
        CTE against the pulled join, and the host base computes no such wrap.
        """
        sql = await _sql(_grouped_order_query(
            model="customers.regions", name="name", direction="asc",
        ))
        bodies = isolated_cte_bodies(sql)
        assert bodies, f"no isolated CTE was emitted:\n{sql}"
        in_cte = [
            (func, arg)
            for body in bodies.values()
            for func, arg in aggregate_calls_in(body)
            if "customers__regions.name" in arg
        ]
        assert [func for func, _ in in_cte] == ["MIN"], (
            f"the host-grain wrap is not computed inside the isolated CTE:\n{sql}"
        )
        base = cte_map(sql).get("_base")
        if base is not None:
            assert not [
                func for func, arg in aggregate_calls_in(base)
                if "customers__regions.name" in arg
            ], f"the wrap was ALSO computed in the host base:\n{sql}"


# ---------------------------------------------------------------------------
# Group 6 — the grouped reject is gone
# ---------------------------------------------------------------------------
class TestRejectRemoved:
    @pytest.mark.parametrize(
        ("model", "name"),
        [("customers", "tier"), ("customers.regions", "name"), ("order_tags", "name")],
    )
    async def test_no_unresolvable_order_column_error(
        self, model: str, name: str,
    ) -> None:
        from slayer.core.errors import UnresolvableOrderColumnError

        try:
            await _sql(_grouped_order_query(
                model=model, name=name, direction="asc",
            ))
        except UnresolvableOrderColumnError as exc:  # pragma: no cover - failure path
            pytest.fail(
                f"grouped joined ORDER BY on {model}.{name} still rejects: {exc}"
            )
