"""DEV-1410: save-time derived-column cycle detection.

Cycles in derived ``Column.sql`` chains must be detected at
``storage.save_model`` time so the broken model never reaches a query.
Compile-time detection remains as defence in depth.

The validator lives in ``StorageBackend.save_model`` (converted to a
template method) so it fires for every save path uniformly — direct
``storage.save_model`` calls, ``engine.save_model``, MCP edit_model, CLI
create/edit, and the migration write-back (the migration path passes
``_validate=False`` so legacy cyclic data remains loadable).
"""
import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ColumnCycleError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _yaml_storage(tmp_path) -> YAMLStorage:
    return YAMLStorage(base_dir=str(tmp_path))


def _sqlite_storage(tmp_path) -> SQLiteStorage:
    return SQLiteStorage(db_path=str(tmp_path / "storage.db"))


# ---------------------------------------------------------------------------
# 1. Same-model cycles.
# ---------------------------------------------------------------------------


async def test_save_model_rejects_same_model_cycle(tmp_path) -> None:
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="c1 - 1", type=DataType.DOUBLE),
        ],
    )
    with pytest.raises(ColumnCycleError) as exc_info:
        await storage.save_model(model)
    # Backwards compat: still catchable as ValueError.
    assert isinstance(exc_info.value, ValueError)
    msg = str(exc_info.value)
    assert "A.c1" in msg and "A.c2" in msg, f"cycle chain missing names: {msg}"


async def test_save_model_rejects_three_deep_cycle(tmp_path) -> None:
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="c3 + 1", type=DataType.DOUBLE),
            Column(name="c3", sql="c1 + 1", type=DataType.DOUBLE),
        ],
    )
    with pytest.raises(ColumnCycleError) as exc_info:
        await storage.save_model(model)
    msg = str(exc_info.value)
    for c in ("A.c1", "A.c2", "A.c3"):
        assert c in msg, f"cycle chain missing {c}: {msg}"


async def test_save_model_rejects_self_referential_derived(tmp_path) -> None:
    """A column referencing ITSELF in a NON-trivial expression (sql != name)
    is a single-step cycle. Distinct from the trivial base case where sql
    equals the column name verbatim (that's how base columns are written)."""
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="c1 + 1", type=DataType.DOUBLE),
        ],
    )
    with pytest.raises(ColumnCycleError) as exc_info:
        await storage.save_model(model)
    assert "A.c1" in str(exc_info.value)


async def test_save_model_accepts_acyclic_derived_dag(tmp_path) -> None:
    """Diamond DAG (d = b + c, b = a + 1, c = a + 2) is acyclic and must
    save cleanly."""
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a", sql="a", type=DataType.DOUBLE),
            Column(name="b", sql="a + 1", type=DataType.DOUBLE),
            Column(name="c", sql="a + 2", type=DataType.DOUBLE),
            Column(name="d", sql="b + c", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(model)
    reloaded = await storage.get_model("A", data_source="ds")
    assert reloaded is not None
    assert {c.name for c in reloaded.columns} == {"id", "a", "b", "c", "d"}


async def test_save_model_accepts_base_columns_only(tmp_path) -> None:
    """Sanity check: a model with no derived columns saves cleanly."""
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a", sql="a", type=DataType.DOUBLE),
            Column(name="b", sql="b", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(model)


# ---------------------------------------------------------------------------
# 2. Cross-model cycles (within a single data_source).
# ---------------------------------------------------------------------------


async def test_save_model_rejects_cross_model_cycle_within_datasource(
    tmp_path,
) -> None:
    """A and B both exist in storage with a derived ref into each other.
    Save of A (when B already exists with a back-ref to A) raises."""
    storage = _yaml_storage(tmp_path)
    # Seed B first with a back-reference into A — saving B alone succeeds
    # because A doesn't exist yet (best-effort save-time validation; the
    # unresolved A.x ref is silently skipped).
    model_b = SlayerModel(
        name="B",
        data_source="ds",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a_id", sql="a_id", type=DataType.DOUBLE),
            Column(name="y", sql="A.x + 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="A", join_pairs=[["a_id", "id"]])],
    )
    await storage.save_model(model_b)
    # Now save A with a forward ref to B.y, completing the cycle:
    # A.x → B.y → A.x.
    model_a = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="x", sql="B.y + 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    with pytest.raises(ColumnCycleError) as exc_info:
        await storage.save_model(model_a)
    msg = str(exc_info.value)
    assert "A.x" in msg and "B.y" in msg, f"cross-model cycle chain missing names: {msg}"


async def test_save_model_rejects_cross_model_cycle_when_second_model_completes_it(
    tmp_path,
) -> None:
    """Order-sensitive: A saves first (B doesn't exist; A.foo's ``B.bar`` ref
    is unresolved and silently skipped — best-effort). When B saves with
    a back-ref to A.foo, the save-time validator on B's save MUST detect
    the cycle (B's reachable graph includes A and A.foo's ref into B.bar)."""
    storage = _yaml_storage(tmp_path)
    # A → B (the ModelJoin is required so the cycle is reachable via joins).
    model_a = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="foo", sql="B.bar + 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    # B does not exist yet — unresolved B.bar ref is silently skipped.
    await storage.save_model(model_a)
    # Now save B with a back-ref to A.foo, completing the cycle.
    model_b = SlayerModel(
        name="B",
        data_source="ds",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a_id", sql="a_id", type=DataType.DOUBLE),
            Column(name="bar", sql="A.foo + 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="A", join_pairs=[["a_id", "id"]])],
    )
    with pytest.raises(ColumnCycleError):
        await storage.save_model(model_b)


async def test_save_model_tolerates_unresolved_joined_ref(tmp_path) -> None:
    """A's column references B.bar but B is not saved yet. A saves cleanly —
    save-time validation is best-effort and silently skips unresolvable
    refs. (The compile-time guard catches the broken ref at query time.)"""
    storage = _yaml_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="foo", sql="B.bar + 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    # No exception expected.
    await storage.save_model(model_a)


async def test_save_model_skips_subquery_scope_refs_in_cycle_detection(
    tmp_path,
) -> None:
    """A bare ref inside a subquery is NOT a derived-column dependency —
    the subquery has its own scope. So a model where the only ``cycle``
    is hidden inside a subquery does NOT raise at save time."""
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
            # c1 references c2 inside a subquery — out of scope for cycle
            # detection (the subquery's bare ``c2`` is not c2-on-this-model).
            # The root-scope expression uses only base raw_a, so no cycle.
            Column(
                name="c1",
                sql="(SELECT MAX(c2) FROM other_table) + raw_a",
                type=DataType.DOUBLE,
            ),
            # c2 references c1 inside a subquery — same treatment.
            Column(
                name="c2",
                sql="(SELECT MAX(c1) FROM other_table) + raw_a",
                type=DataType.DOUBLE,
            ),
        ],
    )
    # No exception — subquery-scope refs are not dependencies.
    await storage.save_model(model)


# ---------------------------------------------------------------------------
# 3. Template-method dispatch: validation must fire through every concrete
# backend. Parameterised so a future backend that overrides _save_model_impl
# without remembering to call super().save_model still gets validated.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend_factory", [_yaml_storage, _sqlite_storage])
async def test_save_model_template_method_runs_for_yaml_and_sqlite_backends(
    tmp_path, backend_factory,
) -> None:
    storage = backend_factory(tmp_path)
    cyclic = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="c1 - 1", type=DataType.DOUBLE),
        ],
    )
    with pytest.raises(ColumnCycleError):
        await storage.save_model(cyclic)


# ---------------------------------------------------------------------------
# 4. Migration write-back: legacy cyclic data must remain LOADABLE.
# storage.get_model() calls save_model() internally after running migrations,
# so the implicit write-back at base.py:_migrate_and_refine_on_load must
# bypass cycle validation. Otherwise the user could never repair a broken
# legacy YAML file through the API.
# ---------------------------------------------------------------------------


async def test_save_model_migration_writeback_does_not_validate(tmp_path) -> None:
    """Write a legacy v4 cyclic model to disk by hand (bypassing save_model),
    then load it through storage.get_model. The migration write-back must
    not raise — the cycle should be tolerated on load."""
    import yaml

    from slayer.core.models import DatasourceConfig

    storage = YAMLStorage(base_dir=str(tmp_path))
    # Persist a datasource so the migration's type-refinement step does not
    # hard-fail on "datasource unavailable" — we want the cycle path to be
    # the only thing being tested here.
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:")
    )

    # Write a hand-rolled v4 YAML with a cycle, no Pydantic validation, no
    # save_model. v4 is one below the current v5, so loading triggers a
    # migration → write-back path.
    ds_dir = tmp_path / "models" / "ds"
    ds_dir.mkdir(parents=True)
    cyclic_dict = {
        "version": 4,
        "name": "A",
        "data_source": "ds",
        "sql_table": "A",
        "columns": [
            # TEXT-typed so has_refineable_columns is False and the
            # migration does not need to introspect the live datasource.
            {"name": "id", "sql": "id", "type": "TEXT", "primary_key": True},
            {"name": "c1", "sql": "c2 + 1", "type": "TEXT"},
            {"name": "c2", "sql": "c1 - 1", "type": "TEXT"},
        ],
        "joins": [],
        "measures": [],
        "aggregations": [],
        "filters": [],
        "source_queries": None,
    }
    (ds_dir / "A.yaml").write_text(yaml.safe_dump(cyclic_dict))

    # Must NOT raise — the migration write-back bypasses validation.
    loaded = await storage.get_model("A", data_source="ds")
    assert loaded is not None
    assert loaded.name == "A"
    assert {c.name for c in loaded.columns} == {"id", "c1", "c2"}


async def test_save_model_explicit_skip_validate_kwarg(tmp_path) -> None:
    """The migration path needs an explicit escape hatch. The template
    method must accept ``_validate=False`` so callers in the migration
    path can persist legacy data unchanged."""
    storage = _yaml_storage(tmp_path)
    cyclic = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="c1 - 1", type=DataType.DOUBLE),
        ],
    )
    # No exception — explicit _validate=False bypasses validation.
    await storage.save_model(cyclic, _validate=False)
    reloaded = await storage.get_model("A", data_source="ds")
    assert reloaded is not None
