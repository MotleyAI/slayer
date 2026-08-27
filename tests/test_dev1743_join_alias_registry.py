"""DEV-1743 — the join-alias registry (WP3, D4).

Emitted join aliases become internal-only: minted by the per-generation
allocator, length-fitted, and silently uniquified in case-fold space. These
tests assert real JOIN wiring (which physical table each alias resolves to),
never just alias spellings — a spelling collision that silently merged two
different relations would pass a spelling-only check.

Fail-first behaviors:
* path ``(a, b)`` and a model literally named ``a__b`` in one query mint
  DISTINCT aliases wired to their DISTINCT physical tables;
* a 4-hop chain of long names keeps every emitted join alias within the
  dialect's byte budget (the DEV-1756-deferred Postgres-63 defect).

Invariant lock:
* a plain chain still mints ``customers`` / ``customers__regions`` byte-for-byte
  (the registry's preferred spelling equals today's, so golden SQL is stable).
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.query import SlayerQuery

from tests._dev1743_fixtures import (
    ambiguity_impossible_models,
    chain_models,
    long_chain_models,
    LONG_DOTTED_PATH,
)
from tests._engine_helpers import _engine_generate


def _join_table_to_alias(sql: str, *, dialect: str = "postgres") -> dict[str, str]:
    """Map each joined physical table name to the alias it is given.

    ``LEFT JOIN a_b_direct AS a__b`` → ``{"a_b_direct": "a__b"}``. Subquery
    joins (``LEFT JOIN (SELECT ...) AS x``) are keyed by their alias only.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    out: dict[str, str] = {}
    for join in tree.find_all(exp.Join):
        target = join.this
        if isinstance(target, exp.Table):
            out[target.name] = target.alias_or_name
    return out


def _all_join_aliases(sql: str, *, dialect: str = "postgres") -> list[str]:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    aliases: list[str] = []
    for join in tree.find_all(exp.Join):
        aliases.append(join.this.alias_or_name)
    return aliases


# --------------------------------------------------------------------------- #
# Spelling collision: path (a, b) vs a model literally named a__b.
# --------------------------------------------------------------------------- #
class TestSpellingCollisionMintsDistinctAliases:
    @pytest.mark.asyncio
    async def test_chain_and_direct_dunder_model_are_distinct(self) -> None:
        models = ambiguity_impossible_models()
        host, extra = models[0], models[1:]
        # Project two distinctly-named host columns so both joins land in one
        # SELECT without tripping the orthogonal flat public-name namespace:
        #   chain_val  (sql=a.b.val)  -> chain host->a->b (alias naturally a__b)
        #   direct_val (sql=a__b.val) -> the direct model literally named a__b
        sql = await _engine_generate(
            query=SlayerQuery(source_model="host",
                              dimensions=["chain_val", "direct_val"]),
            model=host, extra_models=extra,
        )
        tbl = _join_table_to_alias(sql)
        # Both distinct physical tables are joined.
        assert "b" in tbl, f"chain leaf table 'b' not joined:\n{sql}"
        assert "a_b_direct" in tbl, f"direct model table not joined:\n{sql}"
        # And their aliases are distinct (no silent merge).
        assert tbl["b"] != tbl["a_b_direct"], (
            f"chain and direct model collapsed onto one alias:\n{sql}"
        )


# --------------------------------------------------------------------------- #
# Long-name chain: every emitted join alias fits the dialect byte budget.
# --------------------------------------------------------------------------- #
class TestLongChainAliasesAreLengthFitted:
    @pytest.mark.asyncio
    async def test_postgres_63_byte_limit_respected(self) -> None:
        models = long_chain_models()
        host, extra = models[0], models[1:]
        sql = await _engine_generate(
            query=SlayerQuery(source_model=host.name,
                              dimensions=[LONG_DOTTED_PATH]),
            model=host, extra_models=extra, dialect="postgres",
        )
        aliases = _all_join_aliases(sql, dialect="postgres")
        assert aliases, f"expected joins in:\n{sql}"
        for a in aliases:
            assert len(a.encode("utf-8")) <= 63, (
                f"join alias {a!r} is {len(a.encode('utf-8'))} bytes, over "
                f"Postgres's 63-byte limit (silent truncation → wrong join):\n{sql}"
            )
        # Fitting must not collapse two paths onto one alias.
        assert len(aliases) == len(set(aliases)), (
            f"length-fitting produced duplicate aliases:\n{sql}"
        )


# --------------------------------------------------------------------------- #
# Invariant: a plain chain keeps today's alias spellings (golden byte-stability).
# --------------------------------------------------------------------------- #
class TestHostGrainOrderWrapUsesEmittedAlias:
    @pytest.mark.asyncio
    async def test_host_grain_agg_qualifies_to_chain_leaf_not_direct_model(self) -> None:
        """FAIL-FIRST (CR): a host-grain aggregate over the chain path (a, b) must
        qualify to the EMITTED chain-leaf alias, not a raw ``"__".join(path)`` —
        which, when a direct model literally named ``a__b`` is also joined,
        silently reads the WRONG physical table (a_b_direct instead of b)."""
        models = ambiguity_impossible_models()
        host, extra = models[0], models[1:]
        # Group by the direct model's column, order by the CHAIN path → the
        # planner wraps the un-grouped chain column in a host-grain MIN.
        sql = await _engine_generate(
            query=SlayerQuery(
                source_model="host",
                dimensions=["direct_val"],
                measures=["*:count"],
                order=[{"column": "a.b.val", "direction": "asc"}],
            ),
            model=host, extra_models=extra,
        )
        tbl = _join_table_to_alias(sql)
        b_alias = tbl["b"]                 # chain leaf physical table
        direct_alias = tbl["a_b_direct"]  # the direct __-named model
        assert b_alias != direct_alias, f"aliases collapsed:\n{sql}"
        tree = sqlglot.parse_one(sql, dialect="postgres")
        aggs = list(tree.find_all(exp.Min)) + list(tree.find_all(exp.Max))
        assert aggs, f"no host-grain MIN/MAX order-wrap emitted:\n{sql}"
        quals: set[str] = set()
        for a in aggs:
            quals |= {c.table for c in a.find_all(exp.Column)}
        assert b_alias in quals, (
            f"host-grain agg does not read the chain leaf alias {b_alias!r}:\n{sql}"
        )
        assert direct_alias not in quals, (
            f"host-grain agg reads the WRONG table (direct model alias "
            f"{direct_alias!r}):\n{sql}"
        )


class TestPlainChainSpellingIsStable:
    @pytest.mark.asyncio
    async def test_single_and_two_hop_alias_spellings(self) -> None:
        models = chain_models()
        host, extra = models[0], models[1:]
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders",
                              dimensions=["customers.name",
                                          "customers.regions.name"]),
            model=host, extra_models=extra,
        )
        aliases = set(_all_join_aliases(sql))
        # Preferred spelling unchanged: bare name for the single hop, __-joined
        # for the two-hop path.
        assert "customers" in aliases, f"single-hop alias changed:\n{sql}"
        assert "customers__regions" in aliases, f"two-hop alias changed:\n{sql}"


# --------------------------------------------------------------------------- #
# [C8] The registry is keyed by (root, path), not path alone: a query with two
# roots (a cross-model measure reroots at a__b while the outer query joins the
# a -> b chain) whose target spelling coincides must still wire correctly.
# --------------------------------------------------------------------------- #
class TestTwoRootsSameSpelling:
    @pytest.mark.asyncio
    async def test_cross_model_measure_and_chain_dim_coexist(self) -> None:
        models = ambiguity_impossible_models()  # host, a, b, a__b
        host, extra = models[0], models[1:]
        sql = await _engine_generate(
            query=SlayerQuery(
                source_model="host",
                dimensions=["a.b.val"],        # chain -> alias a__b under host
                measures=["a__b.*:count"],     # cross-model -> reroot at a__b
            ),
            model=host, extra_models=extra,
        )
        # Both relations are present and the query is structurally valid
        # (_engine_generate scope-checks it): a path-only registry key would
        # risk collapsing the chain alias and the reroot root.
        assert "a_b_direct" in sql, f"direct model a__b missing:\n{sql}"
        assert "b" in _join_table_to_alias(sql) or " b " in sql
