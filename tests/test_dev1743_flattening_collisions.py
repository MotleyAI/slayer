"""Category 6 / WP8 / D5 — the flatten map stops being injective.

Once source fields may contain ``__``, the projection flattening ``.`` → ``__``
can map two DISTINCT public fields onto one downstream name: a joined dim
``customers.region`` (flattens to ``customers__region``) and a literal dataset
field ``customers__region`` (the C11 carve-out). That collision must raise a
LOUD, CLEAR error — never silently uniquify or bind the first match.

Two surfaces:

* ``_emit_stage_schema`` (``stage_planner.py``) — the per-stage guard. Its
  mechanism is live TODAY (the same-name invariant lock below proves it), but
  the specific ``__``-flatten collision is only REACHABLE after the flip: today
  the Mode-B parser rejects the ``customers__region`` reference before planning,
  so the collision message is not yet produced. Asserting on the collision
  message therefore fails today for a feature reason.
* Query-backed dataset expansion (``query_engine._expand_query_backed_model``) —
  WP8 [C9] adds a local, flatten-specific error BEFORE ``build_flat_rename_
  wrapper``, replacing a confusing Pydantic duplicate-column error. Asserting on
  the improved (``flatten`` / ``collision``) wording fails today.
"""

from __future__ import annotations

import tempfile

import pytest

import slayer.engine.query_engine as qe
from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1743_fixtures import DS, datasource
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Dataset shapes: a host that both RELATES to ``customers`` (with a ``region`` field)
# and carries its OWN literal field ``customers__region``. Post-flip the query
# ``["customers.region", "customers__region"]`` projects both — and both flatten
# to ``customers__region``.
# --------------------------------------------------------------------------- #
def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers_t",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region", datatype=DataType.STRING),
        ],
    )


def _host_with_flat_collider() -> SlayerDataset:
    return SlayerDataset(
        name="hostm", data_source=DS, sql_table="hostm_t",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="cust_id", datatype=DataType.INTEGER),
            # Literal flat field that collides with the joined dim's flattened
            # alias (C11 carve-out). Legal to declare today (D5).
            DatasetField(name="customers__region", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["cust_id"], to_columns=["id"])],
    )


async def _execute_named(datasets, name: str, *, dialect: str = "postgres"):
    """Save ``datasets`` (validation off) and run ``engine.execute(name)``."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name=DS, type=dialect))
        for m in datasets:
            await storage.save_dataset(m, _validate=False)
        engine = SlayerQueryEngine(storage=storage)
        return await engine.execute(name, dry_run=True)


# --------------------------------------------------------------------------- #
# 1. Stage-level: the __-flatten collision must raise a clear collision error.
# --------------------------------------------------------------------------- #
class TestStageFlattenCollision:
    def _qb(self) -> SlayerDataset:
        return SlayerDataset(
            name="qb", data_source=DS,
            source_queries=[
                # Inner stage projects BOTH the joined dim and the literal
                # flat field; both flatten downstream to ``customers__region``.
                SlayerQuery(
                    name="inner", population="hostm",
                    dimensions=["customers.region", "hostm.customers__region"],
                ),
                SlayerQuery(population="inner"),
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
        models = [_customers(), _host_with_flat_collider(), self._qb()]
        with pytest.raises(ValueError, match=r"[Cc]ollision"):
            await _execute_named(models, "qb")


# --------------------------------------------------------------------------- #
# 2. Invariant lock: the _emit_stage_schema guard itself is live today.
#    Two same-named transform metrics flatten to one name → collision. No `__`
#    involved, so this holds identically before and after the flip.
# --------------------------------------------------------------------------- #
class TestSameNameCollisionGuardLive:
    def _orders(self) -> SlayerDataset:
        return SlayerDataset(
            name="orders", data_source=DS, sql_table="orders_t",
            default_time_dimension="created_at",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT),
                DatasetField(name="created_at", datatype=DataType.DATETIME),
            ],
        )

    @pytest.mark.asyncio
    async def test_two_same_named_measures_collide(self) -> None:
        """INVARIANT LOCK: two projected fields that flatten to the same
        downstream name raise, before and after the flip."""
        query = SlayerQuery(
            population="orders",
            metrics=[
                {"metric": "cumsum(sum(orders.amount))", "alias": "dup"},
                {"metric": "cumsum(sum(orders.amount))", "alias": "dup"},
            ],
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
            ],
        )
        model = self._orders()
        with pytest.raises(ValueError, match=r"Stage field name collision on 'dup'"):
            await _engine_generate(query=query, dataset=model, validate=False)


# --------------------------------------------------------------------------- #
# 3. Query-backed expansion: WP8 [C9] clear flatten error before the wrapper.
# --------------------------------------------------------------------------- #
class TestQueryBackedFlattenMessage:
    @pytest.mark.asyncio
    async def test_create_model_from_query_flatten_collision_message(self) -> None:
        """FAIL-FIRST: expanding a query-backed dataset whose projection flattens
        two fields to one ``__`` name must raise a flatten-specific /
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
            await storage.save_dataset(_customers(), _validate=False)
            await storage.save_dataset(_host_with_flat_collider(), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                population="hostm",
                dimensions=["customers.region", "hostm.customers__region"],
            )
            with pytest.raises(Exception, match=r"(?i)flatten|collision"):
                await engine.create_dataset_from_query(
                    query=query, name="cmfq", save=False,
                )

    @pytest.mark.asyncio
    async def test_collision_fires_before_build_flat_rename_wrapper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FAIL-FIRST [C9]: the flatten-collision check must fire BEFORE
        ``build_flat_rename_wrapper``. We stub the wrapper to raise a sentinel;
        if the check fires first the collision error propagates, if the wrapper
        is reached first we'd see the sentinel instead — proving the ordering.
        """
        def _sentinel(*a, **k):
            raise RuntimeError("WRAPPER_REACHED_BEFORE_COLLISION_CHECK")

        monkeypatch.setattr(qe, "build_flat_rename_wrapper", _sentinel)
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(datasource())
            await storage.save_dataset(_customers(), _validate=False)
            await storage.save_dataset(_host_with_flat_collider(), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                population="hostm",
                dimensions=["customers.region", "hostm.customers__region"],
            )
            with pytest.raises(Exception, match=r"(?i)flatten|collision") as ei:
                await engine.create_dataset_from_query(
                    query=query, name="cmfq2", save=False,
                )
            # The sentinel must NOT be what propagated.
            assert "WRAPPER_REACHED" not in str(ei.value)


# --------------------------------------------------------------------------- #
# 4. Computed dimension whose name flattens onto a dotted dim's
#    downstream name collides too — the computed branch must not `continue` past
#    the [D5] flatten guard (Codex, post-merge review).
# --------------------------------------------------------------------------- #
class TestComputedDimFlattenCollision:
    def _host(self) -> SlayerDataset:
        # No literal ``customers__region`` field here (so the computed-dim name
        # doesn't trip the field/metric shadow check first) — only the joined
        # ``customers.region``, which flattens to ``customers__region``.
        return SlayerDataset(
            name="hostm", data_source=DS, sql_table="hostm_t",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="cust_id", datatype=DataType.INTEGER),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers",
                             from_columns=["cust_id"], to_columns=["id"])],
        )

    @pytest.mark.asyncio
    async def test_computed_dim_named_like_flattened_join_collides(self) -> None:
        """FAIL-FIRST: a computed dim explicitly named ``customers__region`` and a
        joined dim ``customers.region`` both flatten to ``customers__region`` and
        must raise the clear D5 message, not a generic duplicate-name error."""
        query = SlayerQuery.model_validate({"population": "hostm", "dimensions": [
                "customers.region",
                {"field": "hostm.amount", "alias": "customers__region"},
            ], "metrics": [{"metric": "sum(hostm.amount)"}]})
        host = self._host()
        extras = [_customers()]
        with pytest.raises(ValueError, match=r"flatten to the same downstream name"):
            await _engine_generate(
                query=query, dataset=host, extra_datasets=extras, validate=False,
            )
