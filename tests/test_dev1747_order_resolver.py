"""DEV-1747 §5.10 — the single order-term resolver.

Four renderers currently build ORDER BY terms independently
(``_apply_order_limit_from_planned``, ``_resolve_combined_order_term``,
``_planned_order_by_sql``, and the ``emit_outer_wrap`` qualifier-strip fix-up).
They disagree on null ordering, on what happens when a term cannot be resolved,
and on how the reference is qualified. §5.10 replaces all four with
``slayer.sql.render.order_terms.resolve_order_term(entry, env)``: one dict
dispatch on ``entry.scope``, zero precedence.

Two behaviour changes fall out of the consolidation and are pinned here:

* **D4** — the combined path currently returns ``None`` when a cross-model
  order slot has no alias, which SILENTLY drops the sort term and returns
  unsorted rows. ``_planned_order_by_sql`` already raises in the same
  situation. The resolver raises everywhere.
* **D5** — the T-SQL ``nulls_first`` pin that suppresses sqlglot's mis-resolving
  ``CASE WHEN … IS NULL`` emulation lives in ``SQLGenerator._ordered`` and is
  therefore MISSING on the combined and transform-chain paths. It moves into
  the dialect strategy (``SqlDialect.build_ordered``), so every path gets it
  (P-H).

Refs: DEV-1747 (D3, D4, D5), DEV-1742 §5.10 / P-G / P-H, DEV-1571 Bug 2.
"""
from __future__ import annotations

import pytest
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from tests._dev1747_fixtures import (
    cte_map,
    dev1747_bundle,
    dev1747_models,
    order_by_text,
    order_terms,
    outermost_select,
)
from tests._engine_helpers import _engine_generate

_MEASURE = [{"formula": "amount:sum", "name": "rev"}]

_MONTH = TimeDimension(
    dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
)

#: One query per render path that builds its own ORDER BY term. D4's claim is
#: that ALL of them raise on an unresolvable slot; today only the transform
#: chain does, and the rest return unsorted rows.
_D4_SHAPES = {
    "host_base": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=_MEASURE,
        order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
    ),
    "cross_model": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            {"formula": "amount:sum", "name": "rev"},
            {"formula": "customers.spend:sum", "name": "cs"},
        ],
        order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
    ),
    "outer_composite": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[{"formula": "customers.spend:sum + amount:sum", "name": "mix"}],
        order=[OrderItem(column=ColumnRef(name="mix"), direction="desc")],
    ),
    "windowed": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[{"formula": "amount:sum(window='90d')", "name": "w"}],
        order=[OrderItem(column=ColumnRef(name="w"), direction="desc")],
    ),
    "transform_chain": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
        order=[OrderItem(column=ColumnRef(name="cs"), direction="asc")],
    ),
}


def _outer_projection(sql: str, *, dialect: str = "postgres"):
    """The outermost SELECT's projection expressions."""
    return outermost_select(sql, dialect=dialect).expressions


def _order_columns(sql: str, *, dialect: str = "postgres"):
    """Every ``exp.Column`` appearing in the outermost ORDER BY."""
    order = outermost_select(sql, dialect=dialect).args.get("order")
    return list(order.find_all(exp.Column)) if order is not None else []


async def _sql(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    models = dev1747_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:], dialect=dialect,
    )


def _assert_orders_by(sql: str, alias: str, *, dialect: str = "postgres") -> None:
    """The outermost ORDER BY names ``alias``.

    ``assert order_by_text(sql)`` only says a sort term exists — it is equally
    satisfied by a term pointing at the wrong column, which is the failure mode
    four independent resolvers actually produce.
    """
    terms = order_terms(sql, dialect=dialect)
    assert terms, f"no ORDER BY emitted:\n{sql}"
    referenced = {c.name for c in _order_columns(sql, dialect=dialect)}
    assert alias in referenced, (
        f"ORDER BY names {referenced or set(terms)}, not {alias!r}:\n{sql}"
    )


def _assert_order_reference_resolves(sql: str, *, dialect: str = "postgres") -> None:
    """Every column in the outermost ORDER BY is something the outer SELECT can
    actually name: one of its own output aliases, or a reference qualified by a
    relation in scope.

    An UNQUALIFIED name that is neither is resolvable only by falling through to
    an input column of the FROM — which Postgres allows and other engines do
    not, and which silently picks a different column the moment two scopes
    project the same name. That is the shape ``_resolve_combined_order_term``
    emits for a cross-model measure today (``ORDER BY
    "orders.customers.spend_sum"`` while the SELECT projects it ``AS
    "orders.cs"``), and the drift §5.10's single resolver removes.
    """
    select = outermost_select(sql, dialect=dialect)
    projected = {s.alias_or_name for s in select.expressions}
    in_scope = set(cte_map(sql, dialect=dialect))
    for table in select.find_all(exp.Table):
        in_scope.add(table.alias_or_name)
    for column in _order_columns(sql, dialect=dialect):
        if column.table:
            assert column.table in in_scope, (
                f"ORDER BY is qualified by {column.table!r}, which is not a "
                f"relation in scope ({sorted(in_scope)}):\n{sql}"
            )
            continue
        assert column.name in projected, (
            f"ORDER BY names {column.name!r} unqualified, but the outer SELECT "
            f"projects {sorted(projected)} — the term resolves only by falling "
            f"through to an input column:\n{sql}"
        )


# ---------------------------------------------------------------------------
# Group 1 — the §5.10 target matrix
# ---------------------------------------------------------------------------
class TestOrderTargetMatrix:
    async def test_duplicate_display_aliases_order_on_one_column(self) -> None:
        """One structural slot carrying TWO public aliases (C13): the same
        formula declared under two names interns once and projects twice. The
        sort must name a column the SELECT actually projects — the resolver
        takes the FIRST alias, since both hold the same value."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "amount:sum", "name": "rev_again"},
            ],
            order=[OrderItem(column=ColumnRef(name="rev_again"), direction="desc")],
        ))
        terms = order_by_text(sql)
        assert terms
        projected = {s.alias_or_name for s in _outer_projection(sql)}
        referenced = {
            c.name for c in _order_columns(sql)
        }
        assert referenced <= projected, (
            f"ORDER BY names {referenced - projected}, which the SELECT does "
            f"not project:\n{sql}"
        )

    async def test_dotted_joined_dimension_alias(self) -> None:
        """A joined dimension projects under its DOTTED result key, so the
        ORDER BY must match that, not the flat ``__`` declared name."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="name", model="customers.regions")],
            measures=_MEASURE,
            order=[OrderItem(
                column=ColumnRef(name="name", model="customers.regions"),
                direction="asc",
            )],
        ))
        terms = order_by_text(sql)
        assert "customers.regions.name" in terms or "customers__regions" in terms, (
            f"dotted joined sort key did not resolve to a projected alias:\n{sql}"
        )

    async def test_hidden_measure_orders_without_being_projected(self) -> None:
        """Both halves: the sort happens, and the hidden slot stays out of the
        public projection. Asserting only the first would pass if the slot were
        quietly projected, which changes the result shape."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=_MEASURE,
            order=[OrderItem(column=ColumnRef(name="id"), direction="desc")],
        ))
        assert order_terms(sql), f"hidden sort key produced no ORDER BY:\n{sql}"
        projected = {s.alias_or_name for s in _outer_projection(sql)}
        assert projected == {"orders.status", "orders.rev"}, (
            f"the hidden order slot leaked into the projection: {projected}"
        )

    async def test_ordinal_looking_alias_is_not_read_as_a_position(self) -> None:
        """A digit-led alias would make ``ORDER BY <alias>`` ambiguous with
        SQL's positional form, which silently sorts by whatever is first.

        ``ColumnRef`` forbids a leading digit outright, so the truly ordinal
        case is unreachable — pinned here so a future relaxation of that
        validator cannot open the hole silently. The nearest REACHABLE shape
        (``_1``) must still emit a quoted identifier, not a bare token."""
        import pydantic

        with pytest.raises(pydantic.ValidationError):
            ColumnRef(name="1")

        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "amount:sum", "name": "_1"}],
            order=[OrderItem(column=ColumnRef(name="_1"), direction="asc")],
        ))
        for term in order_terms(sql):
            stripped = term.strip()
            assert not stripped[0].isdigit(), (
                f"sort term reads as a positional reference:\n{sql}"
            )

    async def test_transformed_measure_orders_at_the_chain_outer_wrap(self) -> None:
        sql = await _sql(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
        ))
        _assert_orders_by(sql, "orders.cs")

    async def test_cross_model_aggregate_orders_on_its_cte_column(self) -> None:
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
        ))
        _assert_order_reference_resolves(sql)

    async def test_rerooted_aggregate_orders_correctly(self) -> None:
        """A cross-model aggregate whose CTE is RE-ROOTED at the target still
        exposes one column to order on — the reroot must not change the
        order-term resolution."""
        sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="name", model="customers.regions")],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
        ))
        _assert_order_reference_resolves(sql)

    async def test_windowed_measure_orders_on_its_cte_column(self) -> None:
        sql = await _sql(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "amount:sum(window='90d')", "name": "w"}],
            order=[OrderItem(column=ColumnRef(name="w"), direction="desc")],
        ))
        _assert_orders_by(sql, "orders.w")


# ---------------------------------------------------------------------------
# Group 2 — D4: unresolvable is an error, never a silent drop
# ---------------------------------------------------------------------------
class TestUnresolvableRaises:
    def test_resolver_raises_when_the_scope_lookup_misses(self) -> None:
        from slayer.core.keys import Phase
        from slayer.engine.planned import OrderEntry, OrderScope
        from slayer.sql.render.order_terms import OrderEnv, resolve_order_term

        entry = OrderEntry(
            slot_id="missing", direction="asc",
            scope=OrderScope.CROSS_MODEL_CTE, phase=Phase.AGGREGATE,
        )
        with pytest.raises(Exception) as exc:
            resolve_order_term(entry=entry, env=OrderEnv())
        assert "missing" in str(exc.value), (
            "the error must name the unresolvable slot so the wiring bug is "
            "findable; a bare exception is barely better than the silent drop"
        )

    def test_every_scope_raises_on_a_missing_slot(self) -> None:
        """Totality of the failure mode, over the dispatch table rather than
        over its source text. A ``return None`` in ONE arm is enough to
        reintroduce the silent drop, and a per-scope loop is what catches an
        arm added later without one."""
        from slayer.core.keys import Phase
        from slayer.engine.planned import OrderEntry, OrderScope
        from slayer.sql.render.order_terms import OrderEnv, resolve_order_term

        for scope in OrderScope:
            entry = OrderEntry(
                slot_id="missing", direction="asc",
                scope=scope, phase=Phase.AGGREGATE,
            )
            with pytest.raises(Exception):
                resolve_order_term(entry=entry, env=OrderEnv())

    @pytest.mark.parametrize("shape", sorted(_D4_SHAPES))
    def test_no_render_path_silently_drops_an_unresolvable_term(
        self, shape: str,
    ) -> None:
        """D4 "everywhere", proven per render path.

        Today only the transform chain raises. The other four rebuild the term
        independently and return unsorted rows with no error — verified by
        rewriting the planned order entry to name a slot that does not exist
        and rendering. Injecting at the PLAN is what makes the injection
        path-independent; every renderer reads the same field.
        """
        from slayer.engine.stage_planner import plan_query
        from slayer.sql.generator import generate_from_planned

        plan = plan_query(query=_D4_SHAPES[shape], bundle=dev1747_bundle())
        assert plan.order, f"{shape} planned no order entry — test is vacuous"
        broken = plan.model_copy(update={
            "order": [
                entry.model_copy(update={"slot_id": "no_such_slot"})
                for entry in plan.order
            ],
        })
        with pytest.raises(Exception) as exc:
            generate_from_planned(broken, bundle=dev1747_bundle())
        assert "no_such_slot" in str(exc.value), (
            f"{shape} raised without naming the slot: {exc.value}"
        )


# ---------------------------------------------------------------------------
# Group 3 — D5: null ordering through the dialect strategy
# ---------------------------------------------------------------------------
class TestNullOrdering:
    @pytest.mark.parametrize("direction", ["asc", "desc"])
    @pytest.mark.parametrize("policy", ["default", "first", "last"])
    def test_dialect_hook_covers_every_direction_and_policy(
        self, direction: str, policy: str,
    ) -> None:
        from slayer.sql.dialects.base import SqlDialect

        ordered = SqlDialect().build_ordered(
            exp.column("a", quoted=True),
            descending=(direction == "desc"),
            nulls=policy,
        )
        assert isinstance(ordered, exp.Ordered)
        assert ordered.args.get("desc") is (direction == "desc")

    @pytest.mark.parametrize("direction", ["asc", "desc"])
    def test_tsql_pins_nulls_first_to_its_native_default(
        self, direction: str,
    ) -> None:
        """T-SQL's ORDER BY resolver mis-resolves the bracketed alias INSIDE
        sqlglot's CASE-WHEN nulls emulation, so the pin suppresses it."""
        from slayer.sql.dialects.tsql import TsqlDialect

        ordered = TsqlDialect().build_ordered(
            exp.column("a", quoted=True),
            descending=(direction == "desc"),
            nulls="default",
        )
        assert ordered.args.get("nulls_first") is (direction == "asc")

    async def test_tsql_combined_path_has_no_case_when_emulation(self) -> None:
        """The gap D5 closes: the combined (cross-model) path builds its own
        ``exp.Ordered`` today and skips the pin entirely."""
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    {"formula": "amount:sum", "name": "rev"},
                    {"formula": "customers.spend:sum", "name": "cs"},
                ],
                order=[OrderItem(column=ColumnRef(name="cs"), direction="desc")],
            ),
            dialect="tsql",
        )
        order_clause = order_by_text(sql, dialect="tsql")
        assert "CASE" not in order_clause.upper(), (
            f"T-SQL combined ORDER BY still emits the CASE-WHEN nulls "
            f"emulation:\n{sql}"
        )

    async def test_tsql_transform_chain_has_no_case_when_emulation(self) -> None:
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                time_dimensions=[TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                )],
                measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
                order=[OrderItem(column=ColumnRef(name="cs"), direction="asc")],
            ),
            dialect="tsql",
        )
        assert "CASE" not in order_by_text(sql, dialect="tsql").upper()


# ---------------------------------------------------------------------------
# Group 4 — one resolver, not four
# ---------------------------------------------------------------------------
class TestSingleResolver:
    """The superseded resolvers stay in the file (P-J state 1) but must lose
    every production caller.

    Proven with raising sentinels over every render shape rather than by
    grepping the module: a source scan cannot tell a live call from one inside
    a docstring or an unreachable branch, and it silently stops meaning
    anything the moment the method is renamed.
    """

    @pytest.mark.parametrize("shape", sorted(_D4_SHAPES))
    @pytest.mark.parametrize(
        "method",
        ["_resolve_combined_order_term", "_apply_order_limit_from_planned"],
    )
    async def test_superseded_resolver_is_never_called(
        self, method: str, shape: str, monkeypatch,
    ) -> None:
        from slayer.sql.generator import SQLGenerator

        assert hasattr(SQLGenerator, method), (
            f"{method} has been deleted; P-J defers deletion to PR 6, so "
            f"update this test deliberately rather than losing the guard"
        )

        def _boom(*_a, **_kw):
            raise AssertionError(
                f"{method} is still on the production render path — §5.10 "
                f"replaces all four resolvers with resolve_order_term"
            )

        monkeypatch.setattr(SQLGenerator, method, _boom)
        await _sql(_D4_SHAPES[shape])

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "tsql"])
    async def test_same_construct_same_sort_term_across_paths(
        self, dialect: str,
    ) -> None:
        """P-G: ordering by ``rev`` means the same thing whether the query is
        single-model (base path) or cross-model (combined path).

        "Both emit an ORDER BY" is not that claim — two paths can both emit a
        term and order by different things. What is pinned is that the two
        paths emit the SAME term, and that each term resolves against the outer
        SELECT. Compared to EACH OTHER rather than to a literal, because the
        public alias is dotted and the mangling dialects legitimately spell it
        differently (``orders___rev`` on T-SQL)."""
        rendered = {}
        for label, query in (
            ("base", SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=_MEASURE,
                order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
            )),
            ("combined", SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    {"formula": "amount:sum", "name": "rev"},
                    {"formula": "customers.spend:sum", "name": "cs"},
                ],
                order=[OrderItem(column=ColumnRef(name="rev"), direction="desc")],
            )),
        ):
            sql = await _sql(query, dialect=dialect)
            terms = order_terms(sql, dialect=dialect)
            assert terms, f"no ORDER BY on {label}/{dialect}:\n{sql}"
            _assert_order_reference_resolves(sql, dialect=dialect)
            rendered[label] = {
                c.name for c in _order_columns(sql, dialect=dialect)
            }
        assert rendered["base"] == rendered["combined"], (
            f"on {dialect} the same construct sorts by {rendered['base']} on "
            f"the base path and {rendered['combined']} on the combined path"
        )
