"""DEV-1743 category 6 / WP8 / D5 — the flatten map stops being injective.

Once source columns may contain ``__``, the projection flattening ``.`` → ``__``
can map two DISTINCT public columns onto one downstream name: a joined dim
``customers.region`` (flattens to ``customers__region``) and a literal model
column ``customers__region`` (the C11 carve-out). That collision must raise a
LOUD, CLEAR error — never silently uniquify or bind the first match.

Two surfaces:

* ``_emit_stage_schema`` (``stage_planner.py``) — the per-stage guard. Its
  mechanism is live TODAY (the same-name invariant lock below proves it), but
  the specific ``__``-flatten collision is only REACHABLE after the flip: today
  the Mode-B parser rejects the ``customers__region`` reference before planning,
  so the collision message is not yet produced. Asserting on the collision
  message therefore fails today for a feature reason.
* Query-backed model expansion (``query_engine._expand_query_backed_model``) —
  WP8 [C9] adds a local, flatten-specific error BEFORE ``build_flat_rename_
  wrapper``, replacing a confusing Pydantic duplicate-column error. Asserting on
  the improved (``flatten`` / ``collision``) wording fails today.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1743_fixtures import DS, datasource
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Model shapes: a host that both JOINS ``customers`` (with a ``region`` column)
# and carries its OWN literal column ``customers__region``. Post-flip the query
# ``["customers.region", "customers__region"]`` projects both — and both flatten
# to ``customers__region``.
# --------------------------------------------------------------------------- #
def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source=DS, sql_table="customers_t",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )


def _host_with_flat_collider() -> SlayerModel:
    return SlayerModel(
        name="hostm", data_source=DS, sql_table="hostm_t",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="cust_id", type=DataType.INT),
            # Literal flat column that collides with the joined dim's flattened
            # alias (C11 carve-out). Legal to declare today (D5).
            Column(name="customers__region", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["cust_id", "id"]])],
    )


async def _execute_named(models, name: str, *, dialect: str = "postgres"):
    """Save ``models`` (validation off) and run ``engine.execute(name)``."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name=DS, type=dialect))
        for m in models:
            await storage.save_model(m, _validate=False)
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(name, dry_run=True)


# --------------------------------------------------------------------------- #
# 1. Stage-level: the __-flatten collision must raise a clear collision error.
# --------------------------------------------------------------------------- #
class TestStageFlattenCollision:
    def _qb(self) -> SlayerModel:
        return SlayerModel(
            name="qb", data_source=DS,
            source_queries=[
                # Inner stage projects BOTH the joined dim and the literal
                # flat column; both flatten downstream to ``customers__region``.
                SlayerQuery(
                    name="inner", source_model="hostm",
                    dimensions=["customers.region", "customers__region"],
                ),
                SlayerQuery(source_model="inner"),
            ],
        )

    @pytest.mark.asyncio
    async def test_flatten_collision_raises_clear_error(self) -> None:
        """FAIL-FIRST: the joined ``customers.region`` and literal
        ``customers__region`` collide on the flattened downstream name and must
        raise a clear collision error naming it.

        Fails today: the Mode-B parser rejects the ``customers__region``
        reference (a different, pre-flip error) before ``_emit_stage_schema``
        can produce the collision message. Post-flip the reference binds (D5)
        and the stage guard fires.
        """
        with pytest.raises(ValueError, match=r"[Cc]ollision"):
            await _execute_named([_customers(), _host_with_flat_collider(), self._qb()], "qb")


# --------------------------------------------------------------------------- #
# 2. Invariant lock: the _emit_stage_schema guard itself is live today.
#    Two same-named transform measures flatten to one name → collision. No `__`
#    involved, so this holds identically before and after the flip.
# --------------------------------------------------------------------------- #
class TestSameNameCollisionGuardLive:
    def _orders(self) -> SlayerModel:
        return SlayerModel(
            name="orders", data_source=DS, sql_table="orders_t",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
        )

    @pytest.mark.asyncio
    async def test_two_same_named_measures_collide(self) -> None:
        """INVARIANT LOCK: two projected columns that flatten to the same
        downstream name raise, before and after the flip."""
        query = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "cumsum(amount:sum)", "name": "dup"},
                {"formula": "cumsum(amount:sum)", "name": "dup"},
            ],
            time_dimensions=[
                {"dimension": {"name": "created_at"}, "granularity": "month"},
            ],
        )
        with pytest.raises(ValueError, match=r"Stage column name collision on 'dup'"):
            await _engine_generate(query=query, model=self._orders(), validate=False)


# --------------------------------------------------------------------------- #
# 3. Query-backed expansion: WP8 [C9] clear flatten error before the wrapper.
# --------------------------------------------------------------------------- #
class TestQueryBackedFlattenMessage:
    @pytest.mark.asyncio
    async def test_create_model_from_query_flatten_collision_message(self) -> None:
        """FAIL-FIRST: expanding a query-backed model whose projection flattens
        two columns to one ``__`` name must raise a flatten-specific /
        collision message — NOT a bare Pydantic "duplicate column" error.

        Fails today: the Mode-B parser rejects ``customers__region`` (a
        pre-flip error mentioning neither ``flatten`` nor ``collision``) during
        expansion. The match deliberately excludes the colliding NAME because
        that leaks into today's parser-ban message; it keys on the improved
        wording so the test only passes once the flip + WP8 land.
        """
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(datasource())
            await storage.save_model(_customers(), _validate=False)
            await storage.save_model(_host_with_flat_collider(), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            with pytest.raises(Exception, match=r"(?i)flatten|collision"):
                await engine.create_model_from_query(
                    query=SlayerQuery(
                        source_model="hostm",
                        dimensions=["customers.region", "customers__region"],
                    ),
                    name="cmfq",
                    save=False,
                )

    @pytest.mark.asyncio
    async def test_collision_fires_before_build_flat_rename_wrapper(
        self, monkeypatch
    ) -> None:
        """FAIL-FIRST [C9]: the flatten-collision check must fire BEFORE
        ``build_flat_rename_wrapper``. We stub the wrapper to raise a sentinel;
        if the check fires first the collision error propagates, if the wrapper
        is reached first we'd see the sentinel instead — proving the ordering.
        """
        import slayer.engine.query_engine as qe

        def _sentinel(*a, **k):
            raise RuntimeError("WRAPPER_REACHED_BEFORE_COLLISION_CHECK")

        monkeypatch.setattr(qe, "build_flat_rename_wrapper", _sentinel)
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(datasource())
            await storage.save_model(_customers(), _validate=False)
            await storage.save_model(_host_with_flat_collider(), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            with pytest.raises(Exception, match=r"(?i)flatten|collision") as ei:
                await engine.create_model_from_query(
                    query=SlayerQuery(
                        source_model="hostm",
                        dimensions=["customers.region", "customers__region"],
                    ),
                    name="cmfq2", save=False,
                )
            # The sentinel must NOT be what propagated.
            assert "WRAPPER_REACHED" not in str(ei.value)
