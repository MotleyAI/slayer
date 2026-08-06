"""DEV-1746 §5.6 — WITH-chain assembly in topological order.

The cross-model paths splice their WITH chain out of f-strings::

    cte_strs = [f"{name} AS (\\n{sql}\\n)" for name, sql in all_ctes[:-1]]
    sql = f"WITH {', '.join(cte_strs)}\\n{combined_select_sql}"

so CTE order is whatever order the python list happened to be built in, and the
transform chain reads its predecessor positionally (``prev_cte = ctes[-1][0]``).
That works today only because the list is assembled in one hard-coded sequence.

§5.6 replaces it with one assembler that takes **explicitly declared**
dependencies and emits a stable topological order (insertion order as the
tiebreak). Dependencies are declared by the caller — ``_wm_`` depends on
``_base``, transform step N on step N-1, ``_cm_`` on nothing — rather than
discovered by scanning the rendered AST, because scanning cannot distinguish a
CTE reference from a same-named real table, is defeated by quoting and case
folding, and would silently mis-order rather than fail (Codex D3).

``assert_unique_cte_names`` stays as the belt: the assembler is responsible for
ORDER, the belt for name collisions, including the case-folding collisions that
only appear on dialects that fold (DEV-1726).

Scope note: per the PR's ruling only the CROSS-MODEL sites adopt the assembler
in this PR (the combined tail and the cross-model transform chain). The local
single-model transform chain keeps its splice until PR 4, so the engine-level
tests here use cross-model shapes.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.sql.naming import assert_unique_cte_names

from tests._cross_model_chain import _gen
from tests._dev1746_fixtures import cte_names_in_order

#: Imported lazily so a missing implementation fails these tests rather than
#: erroring collection for the whole module.
_ASSEMBLER_MODULE = "slayer.sql.render.cte_assembly"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]


def _cross_model_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.status")],
        measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
    )


def _two_cross_model_measures_query() -> SlayerQuery:
    """Two independent ``_cm_`` CTEs — neither depends on the other, so their
    relative order is decided purely by the declaration-order tiebreak."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="customers_v2.status")],
        measures=[
            ModelMeasure(formula="customers_v2.lifetime_value:sum", name="ltv"),
            ModelMeasure(formula="customers_v2.lifetime_value:avg", name="ltv_avg"),
        ],
    )


def _windowed_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="amount:sum(window='90d')", name="rev_w")],
    )


def _mixed_query() -> SlayerQuery:
    """Cross-model AND windowed in one query — the mix §5.6 names."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="amount:sum(window='90d')", name="rev_w"),
            ModelMeasure(formula="customers_v2.lifetime_value:sum", name="ltv"),
        ],
    )


# =========================================================================== #
# The assembler.
# =========================================================================== #
class TestWithChainAssembler:

    @staticmethod
    def _mod():
        import importlib

        return importlib.import_module(_ASSEMBLER_MODULE)

    @staticmethod
    def _sel(from_: str = "t") -> exp.Select:
        return exp.Select().select(exp.column("a")).from_(from_)

    def _entry(self, name: str, deps: list[str]):
        mod = self._mod()
        return mod.CteEntry(name=name, query=self._sel(), depends_on=deps)

    def test_dependencies_precede_their_dependents(self) -> None:
        """The one hard ordering rule: a CTE is emitted after everything it
        declares a dependency on. SQL requires it — a CTE cannot reference a
        later sibling."""
        mod = self._mod()
        entries = [
            self._entry("c", ["b"]),
            self._entry("b", ["a"]),
            self._entry("a", []),
        ]
        out = mod.assemble_with_chain(entries=entries, final=self._sel("c"))
        names = [cte.alias_or_name for cte in out.args["with_"].expressions]
        assert names.index("a") < names.index("b") < names.index("c"), names

    def test_independent_entries_keep_insertion_order(self) -> None:
        """The tiebreak. Two CTEs with no dependency between them must come out
        in the order the caller declared them — otherwise emitted SQL would
        vary run to run for the same plan."""
        mod = self._mod()
        entries = [self._entry(n, []) for n in ("first", "second", "third")]
        out = mod.assemble_with_chain(entries=entries, final=self._sel("first"))
        names = [cte.alias_or_name for cte in out.args["with_"].expressions]
        assert names == ["first", "second", "third"], names

    def test_ordering_is_deterministic_across_repeated_assembly(self) -> None:
        """The same entries must assemble the same way every time.

        Asserted against the expected order rather than only against a second
        run: comparing two runs to each other proves they agree but not that
        they agree on the RIGHT thing, and a set-based implementation could
        still be stable within one process while varying across them.
        """
        mod = self._mod()

        def build() -> list[str]:
            entries = [
                self._entry("wm", ["base"]),
                self._entry("cm", []),
                self._entry("base", []),
            ]
            out = mod.assemble_with_chain(entries=entries, final=self._sel("base"))
            return [cte.alias_or_name for cte in out.args["with_"].expressions]

        # ``wm`` depends on ``base``, so ``base`` is pulled ahead of it; ``cm``
        # depends on nothing and keeps its declared position between them.
        expected = ["base", "wm", "cm"]
        first_run = build()
        second_run = build()
        assert first_run == expected, first_run
        assert second_run == expected, second_run

    def test_a_dependency_cycle_raises(self) -> None:
        """A cycle cannot be emitted as a WITH chain at all. Failing loudly
        beats emitting a plausible-looking order that references forward."""
        mod = self._mod()
        entries = [self._entry("a", ["b"]), self._entry("b", ["a"])]
        with pytest.raises(ValueError, match="(?i)cycle"):
            mod.assemble_with_chain(entries=entries, final=self._sel("a"))

    def test_an_unknown_dependency_raises(self) -> None:
        """Declaring a dependency on a CTE that was never supplied is a wiring
        bug; silently ignoring it would emit SQL referencing a missing table."""
        mod = self._mod()
        entries = [self._entry("a", ["nope"])]
        with pytest.raises(ValueError, match="(?i)unknown|missing|nope"):
            mod.assemble_with_chain(entries=entries, final=self._sel("a"))

    def test_duplicate_names_raise(self) -> None:
        mod = self._mod()
        entries = [self._entry("dup", []), self._entry("dup", [])]
        with pytest.raises(ValueError, match="(?i)duplicate|dup"):
            mod.assemble_with_chain(entries=entries, final=self._sel("dup"))

    def test_no_entries_yields_the_final_select_unwrapped(self) -> None:
        """No CTEs means no WITH clause — not an empty one, which is invalid."""
        mod = self._mod()
        out = mod.assemble_with_chain(entries=[], final=self._sel())
        assert out.args.get("with_") is None, out.sql()

    def test_assembled_statement_is_a_select_not_a_string(self) -> None:
        """The point of §5.6: the chain is AST all the way, so a caller can keep
        transforming it (pagination, outer wraps) without re-parsing."""
        mod = self._mod()
        out = mod.assemble_with_chain(
            entries=[self._entry("a", [])], final=self._sel("a"),
        )
        assert isinstance(out, exp.Select), type(out)

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_assembled_statement_round_trips_through_every_dialect(
        self, dialect: str,
    ) -> None:
        mod = self._mod()
        entries = [self._entry("base", []), self._entry("wm", ["base"])]
        out = mod.assemble_with_chain(entries=entries, final=self._sel("wm"))
        rendered = out.sql(dialect=dialect)
        parsed = sqlglot.parse(rendered, dialect=dialect)
        assert len(parsed) == 1, f"[{dialect}] did not round-trip:\n{rendered}"
        assert_unique_cte_names(rendered, dialect=dialect)

    def test_quoted_and_mixed_case_names_survive_assembly(self) -> None:
        """A quoted alias must stay one identifier through assembly."""
        mod = self._mod()
        entries = [self._entry("MixedCase", []), self._entry("other", ["MixedCase"])]
        out = mod.assemble_with_chain(entries=entries, final=self._sel("other"))
        names = [cte.alias_or_name for cte in out.args["with_"].expressions]
        assert names == ["MixedCase", "other"], names

    def test_case_folding_duplicates_are_rejected(self) -> None:
        """DEV-1726: two names differing only in case collide on a folding
        dialect. The belt catches it in emitted SQL; the assembler must not be
        the thing that introduces it."""
        mod = self._mod()
        entries = [self._entry("dup", []), self._entry("DUP", [])]
        out = mod.assemble_with_chain(entries=entries, final=self._sel("dup"))
        with pytest.raises(ValueError):
            assert_unique_cte_names(out.sql(dialect="snowflake"), dialect="snowflake")


# =========================================================================== #
# Engine-level: the assembled chain for real cross-model shapes.
# =========================================================================== #
class TestAssembledChainForRealQueries:

    async def test_base_precedes_the_cross_model_cte(self) -> None:
        sql = await _gen(_cross_model_query(), dialect="postgres")
        names = cte_names_in_order(sql)
        assert "_base" in names, names
        cm = [n for n in names if n.startswith("_cm_")]
        assert cm, f"no _cm_ CTE in {names}"
        assert names.index("_base") < names.index(cm[0]), names

    async def test_windowed_cte_follows_the_base_it_reads(self) -> None:
        """``_wm_`` selects FROM ``_base``, so the dependency is real: emitting
        it first would be invalid SQL, not merely untidy."""
        sql = await _gen(_windowed_query(), dialect="postgres")
        names = cte_names_in_order(sql)
        wm = [n for n in names if n.startswith("_wm_")]
        assert wm, f"no _wm_ CTE in {names}"
        assert names.index("_base") < names.index(wm[0]), names

    async def test_mixed_cross_model_and_windowed_chain_is_ordered(self) -> None:
        sql = await _gen(_mixed_query(), dialect="postgres")
        names = cte_names_in_order(sql)
        assert names[0] == "_base", names
        assert any(n.startswith("_cm_") for n in names), names
        assert any(n.startswith("_wm_") for n in names), names

    async def test_two_independent_cross_model_ctes_follow_declaration_order(
        self,
    ) -> None:
        """The tiebreak, end to end: two ``_cm_`` CTEs that do not depend on
        each other appear in measure-declaration order."""
        sql = await _gen(_two_cross_model_measures_query(), dialect="postgres")
        names = [n for n in cte_names_in_order(sql) if n.startswith("_cm_")]
        assert len(names) == 2, f"expected two _cm_ CTEs, got {names}"
        assert "sum" in names[0] and "avg" in names[1], (
            f"_cm_ CTEs are not in measure-declaration order: {names}"
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_assembled_chain_parses_and_has_unique_names(
        self, dialect: str,
    ) -> None:
        sql = await _gen(_mixed_query(), dialect=dialect)
        parsed = sqlglot.parse(sql, dialect=dialect)
        assert len(parsed) == 1, f"[{dialect}] did not parse:\n{sql}"
        assert_unique_cte_names(sql, dialect=dialect)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_no_nested_with_clause_is_emitted(self, dialect: str) -> None:
        """One statement, one WITH. A nested WITH inside a CTE body is invalid
        on T-SQL and a sign the assembler spliced a complete statement in as a
        CTE body.

        Counted over parsed ``With`` nodes rather than lines starting with
        ``WITH``: an indented or inline nested WITH would slip past the textual
        check entirely.
        """
        sql = await _gen(_cross_model_query(), dialect=dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        with_nodes = list(tree.find_all(exp.With))
        assert len(with_nodes) <= 1, (
            f"[{dialect}] {len(with_nodes)} WITH clauses — a CTE body contains "
            f"a complete statement:\n{sql}"
        )

    async def test_the_assembler_is_what_builds_the_cross_model_chain(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Production-path proof for §5.6.

        The ordering tests above would still pass if the legacy f-string splice
        happened to produce the same order, which for today's shapes it does.
        So this asserts the assembler is actually the thing that runs, and that
        it receives EXPLICIT dependency metadata (Codex D3) rather than being
        handed a bare list to sort by itself.
        """
        from slayer.sql import generator as generator_mod

        calls: list = []
        original = generator_mod.assemble_with_chain

        def _wrapped(*, entries, final, **kwargs):
            calls.append(list(entries))
            return original(entries=entries, final=final, **kwargs)

        # Patch the GENERATOR's binding, not the defining module's: the
        # generator imports the symbol directly (imports live at the top of the
        # file), so rebinding the source module would leave production calling
        # the original and the spy would record nothing.
        monkeypatch.setattr(
            generator_mod, "assemble_with_chain", _wrapped, raising=True,
        )
        sql = await _gen(_mixed_query(), dialect="postgres")
        assert calls, (
            "the cross-model WITH chain was assembled without the shared "
            f"assembler — the f-string splice is still in use:\n{sql}"
        )
        entries = calls[-1]
        names = {e.name for e in entries}
        assert any(n.startswith("_wm_") for n in names), names
        # The windowed CTE reads _base, so its dependency must be DECLARED.
        wm_entries = [e for e in entries if e.name.startswith("_wm_")]
        assert all("_base" in e.depends_on for e in wm_entries), (
            "a _wm_ CTE selects FROM _base but did not declare that "
            f"dependency: {[(e.name, e.depends_on) for e in wm_entries]}"
        )
