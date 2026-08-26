"""DEV-1743 — the one Mode-A resolution door (WP2), through public seams.

Exercised end-to-end (``engine.save_model`` for the save-time pass [C5],
``_engine_generate`` for the generation-time door) rather than against a private
helper, so the tests are robust to the resolver's internal shape.

Fail-first behaviors (feature missing today):
* a ``__``-named model resolves as an exact-match join target in Mode-A SQL;
* the legacy ``customers__regions`` split-alias form is a hard D2 error naming
  the dotted replacement, at BOTH save time and generation time;
* a dotted chain whose later hop is not a join raises naming the failing hop.

Invariant locks (must hold before and after):
* a physical ``schema.table.column`` reference stays opaque (never split).
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1743_fixtures import (
    ai_a,
    ai_a__b,
    ai_b,
    chain_customers,
    chain_regions,
    dunder_target,
)
from tests._engine_helpers import _engine_generate, _join_aliases


async def _engine_with(models, *, dialect="postgres"):
    """Build an engine over ``models`` saved through the ENGINE (save-time pass)."""
    d = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=d)
    await storage.save_datasource(DatasourceConfig(name="test", type=dialect))
    engine = SlayerQueryEngine(storage=storage)
    for m in models:
        await engine.save_model(m)
    return engine


# --------------------------------------------------------------------------- #
# A __-named model resolves as an exact-match Mode-A join target.
# --------------------------------------------------------------------------- #
class TestDunderTargetResolves:
    def _host_with_label(self) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="cr_id", type=DataType.INT),
                # Mode-A reference to a DIRECT join target literally named
                # ``customer__region`` — exact match, single hop.
                Column(name="region_label", type=DataType.TEXT,
                       sql="customer__region.label"),
            ],
            joins=[ModelJoin(target_model="customer__region",
                             join_pairs=[["cr_id", "id"]])],
        )

    @pytest.mark.asyncio
    async def test_dunder_target_joins_and_projects(self) -> None:
        host = self._host_with_label()
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders", dimensions=["region_label"]),
            model=host, extra_models=[dunder_target()],
        )
        # The dim's physical table is joined and its label column read.
        assert "customer_region_dim" in sql
        assert "label" in sql


# --------------------------------------------------------------------------- #
# The legacy __ split-alias form is a hard D2 error — save time.
# --------------------------------------------------------------------------- #
class TestLegacyDunderIsHardErrorAtSave:
    def _orders_with_legacy_alias(self) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                # No model literally named ``customers__regions`` exists; the
                # naive split walks customers -> regions, so this must be a D2
                # error naming the dotted replacement.
                Column(name="rn", type=DataType.TEXT,
                       sql="customers__regions.name"),
            ],
            joins=[ModelJoin(target_model="customers",
                             join_pairs=[["customer_id", "id"]])],
        )

    @pytest.mark.asyncio
    async def test_save_raises_d2_with_dotted_suggestion(self) -> None:
        engine = await _engine_with([chain_regions(), chain_customers()])
        with pytest.raises(ValueError) as ei:
            await engine.save_model(self._orders_with_legacy_alias())
        msg = str(ei.value)
        # Names BOTH the rejected legacy qualifier and the dotted replacement,
        # so an unrelated downstream error that merely quotes the path can't
        # satisfy it.
        assert "customers__regions" in msg
        assert "customers.regions.name" in msg


# --------------------------------------------------------------------------- #
# The legacy form ALSO errors at generation time (door covers bypass paths) [C5].
# _engine_generate saves via storage.save_model, bypassing the save-time pass.
# --------------------------------------------------------------------------- #
class TestLegacyDunderIsHardErrorAtGeneration:
    def _orders_with_legacy_alias(self) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="rn", type=DataType.TEXT,
                       sql="customers__regions.name"),
            ],
            joins=[ModelJoin(target_model="customers",
                             join_pairs=[["customer_id", "id"]])],
        )

    @pytest.mark.asyncio
    async def test_generation_raises_d2(self) -> None:
        with pytest.raises(ValueError) as ei:
            await _engine_generate(
                query=SlayerQuery(source_model="orders", dimensions=["rn"]),
                model=self._orders_with_legacy_alias(),
                extra_models=[chain_customers(), chain_regions()],
                validate=False,  # bypass save-time pass; door must still fire
            )
        msg = str(ei.value)
        assert "customers__regions" in msg
        assert "customers.regions.name" in msg


# --------------------------------------------------------------------------- #
# A dotted chain whose later hop is not a join raises naming the failing hop.
# --------------------------------------------------------------------------- #
class TestBrokenChainNamesFailingHop:
    def _orders_broken_chain(self) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                # customers is a join target; nonexistent is not a join on it.
                Column(name="bad", type=DataType.TEXT,
                       sql="customers.nonexistent.name"),
            ],
            joins=[ModelJoin(target_model="customers",
                             join_pairs=[["customer_id", "id"]])],
        )

    @pytest.mark.asyncio
    async def test_save_raises_naming_the_hop(self) -> None:
        engine = await _engine_with([chain_regions(), chain_customers()])
        with pytest.raises(Exception) as ei:
            await engine.save_model(self._orders_broken_chain())
        assert "nonexistent" in str(ei.value)


# Physical-ref opacity ("a non-join-target qualifier is never segmented") is a
# resolver-level invariant tested against the normalizer in the flipped
# tests/test_dot_path_in_sql.py — end-to-end it fights the scope checker, since
# a 3-part ref to a table that is not actually joined leaks scope regardless of
# the flip.


# --------------------------------------------------------------------------- #
# Invariant: an explicit host-name qualifier resolves to the host (2-part ref
# where the qualifier equals the source relation).
# --------------------------------------------------------------------------- #
class TestExactHostQualifier:
    @pytest.mark.asyncio
    async def test_host_qualified_column_resolves(self) -> None:
        host = SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                # Explicit host qualifier — resolves to the host relation.
                Column(name="doubled", type=DataType.DOUBLE, sql="orders.amount * 2"),
                Column(name="amount", type=DataType.DOUBLE),
            ],
        )
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders",
                              measures=[{"formula": "doubled:sum"}]),
            model=host,
        )
        assert "amount" in sql


# --------------------------------------------------------------------------- #
# The same resolution applies on the OTHER two Mode-A free-SQL surfaces:
# Column.filter and SlayerModel.filters (the plan changes all three).
# --------------------------------------------------------------------------- #
class TestFilterSurfacesResolve:
    def _host(self, *, col_filter=None, model_filters=None) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE, filter=col_filter),
            ],
            joins=[ModelJoin(target_model="customers",
                             join_pairs=[["customer_id", "id"]])],
            filters=list(model_filters) if model_filters else [],
        )

    @pytest.mark.asyncio
    async def test_column_filter_legacy_dunder_is_d2(self) -> None:
        engine = await _engine_with([chain_regions(), chain_customers()])
        with pytest.raises(ValueError) as ei:
            await engine.save_model(
                self._host(col_filter="customers__regions.name = 'US'"))
        assert "customers.regions.name" in str(ei.value)

    @pytest.mark.asyncio
    async def test_model_filter_legacy_dunder_is_d2(self) -> None:
        engine = await _engine_with([chain_regions(), chain_customers()])
        with pytest.raises(ValueError) as ei:
            await engine.save_model(
                self._host(model_filters=["customers__regions.name = 'US'"]))
        assert "customers.regions.name" in str(ei.value)


# --------------------------------------------------------------------------- #
# D1 headline: ambiguity is structurally impossible. With a direct model named
# ``a__b`` AND a chain ``a → b``, ``a__b.x`` is the direct model and ``a.b.x`` is
# the chain — both resolve, to DIFFERENT relations, no error.
# --------------------------------------------------------------------------- #
class TestAmbiguityImpossible:
    def _host(self) -> SlayerModel:
        return SlayerModel(
            name="host", data_source="test", sql_table="host",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="a_id", type=DataType.INT),
                Column(name="ab_id", type=DataType.INT),
                Column(name="chain_val", type=DataType.TEXT, sql="a.b.val"),
                Column(name="direct_val", type=DataType.TEXT, sql="a__b.val"),
            ],
            joins=[
                ModelJoin(target_model="a", join_pairs=[["a_id", "id"]]),
                ModelJoin(target_model="a__b", join_pairs=[["ab_id", "id"]]),
            ],
        )

    @pytest.mark.asyncio
    async def test_direct_model_and_chain_resolve_to_distinct_relations(self) -> None:
        sql = await _engine_generate(
            query=SlayerQuery(source_model="host",
                              dimensions=["chain_val", "direct_val"]),
            model=self._host(), extra_models=[ai_a(), ai_b(), ai_a__b()],
        )
        # The chain's leaf table and the direct model's table are BOTH joined.
        assert "a_b_direct" in sql, f"direct model a__b not joined:\n{sql}"
        assert "b" in _join_aliases(sql) or " b " in sql


# --------------------------------------------------------------------------- #
# Deep dotted chain (5 name parts -> nested Dot AST, not flat Column.parts) [C4].
# --------------------------------------------------------------------------- #
class TestDeepDottedChainResolves:
    def _four_hop_models(self):
        d = lambda name, cols, joins=(): SlayerModel(  # noqa: E731
            name=name, data_source="test", sql_table=name,
            columns=cols, joins=list(joins),
        )
        leaf = d("dd", [Column(name="id", type=DataType.INT, primary_key=True),
                        Column(name="v", type=DataType.TEXT)])
        cc = d("cc", [Column(name="id", type=DataType.INT, primary_key=True),
                      Column(name="dd_id", type=DataType.INT)],
               [ModelJoin(target_model="dd", join_pairs=[["dd_id", "id"]])])
        bb = d("bb", [Column(name="id", type=DataType.INT, primary_key=True),
                      Column(name="cc_id", type=DataType.INT)],
               [ModelJoin(target_model="cc", join_pairs=[["cc_id", "id"]])])
        aa = d("aa", [Column(name="id", type=DataType.INT, primary_key=True),
                      Column(name="bb_id", type=DataType.INT)],
               [ModelJoin(target_model="bb", join_pairs=[["bb_id", "id"]])])
        host = SlayerModel(
            name="hh", data_source="test", sql_table="hh",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="aa_id", type=DataType.INT),
                # 5 name parts: aa.bb.cc.dd.v — a nested-Dot AST, not 4-part
                # Column.parts. The extraction layer must still walk it.
                Column(name="deep", type=DataType.TEXT, sql="aa.bb.cc.dd.v"),
            ],
            joins=[ModelJoin(target_model="aa", join_pairs=[["aa_id", "id"]])],
        )
        return host, [aa, bb, cc, leaf]

    @pytest.mark.asyncio
    async def test_five_part_chain_resolves(self) -> None:
        host, extra = self._four_hop_models()
        sql = await _engine_generate(
            query=SlayerQuery(source_model="hh", dimensions=["deep"]),
            model=host, extra_models=extra,
        )
        # Every hop's physical table is joined into the query.
        for tbl in ("aa", "bb", "cc", "dd"):
            assert tbl in _join_aliases(sql) or f" {tbl} " in sql
