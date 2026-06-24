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
from typing import Tuple

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


# Fixture factories — the cross-model cycle tests below all need variants
# of two model graphs (A↔B and A→B→C), so the boilerplate is hoisted out.


def _model_a_to_b(*, foo_sql: str) -> SlayerModel:
    """A model with a single derived column ``foo`` joined to a target B."""
    return SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="foo", sql=foo_sql, type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )


def _abc_chain(
    *,
    a_foo_sql: str,
    c_x_sql: str,
    c_joins_back_to_a: bool = False,
) -> Tuple[SlayerModel, SlayerModel, SlayerModel]:
    """The canonical A → B → C three-model scaffold used by the strict
    alias-walk tests. ``foo`` on A and ``x`` on C are derived; their
    expressions are the per-test variable.

    ``c_joins_back_to_a`` adds an ``a_id`` column + a C→A join, which
    closes a back-edge needed to make a cycle through C.x reachable.
    """
    c_columns = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="x", sql=c_x_sql, type=DataType.DOUBLE),
    ]
    c_joins = []
    if c_joins_back_to_a:
        c_columns.insert(
            1, Column(name="a_id", sql="a_id", type=DataType.DOUBLE),
        )
        c_joins.append(
            ModelJoin(target_model="A", join_pairs=[["a_id", "id"]]),
        )
    model_c = SlayerModel(
        name="C",
        data_source="ds",
        sql_table="C",
        columns=c_columns,
        joins=c_joins,
    )
    model_b = SlayerModel(
        name="B",
        data_source="ds",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c_id", sql="c_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="C", join_pairs=[["c_id", "id"]])],
    )
    model_a = SlayerModel(
        name="A",
        data_source="ds",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="foo", sql=a_foo_sql, type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    return model_a, model_b, model_c


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


async def test_save_model_accepts_quoted_self_identity_column(tmp_path) -> None:
    """A column whose sql is the double-quoted form of its own name is a base
    reference, not a derived expression — required to reach a mixed-case
    physical column on case-folding dialects (Prisma-style camelCase). It must
    NOT be mistaken for a single-step self-cycle."""
    storage = _yaml_storage(tmp_path)
    model = SlayerModel(
        name="merchant",
        data_source="ds",
        sql_table="merchant",
        columns=[
            Column(name="legalEntityType", sql='"legalEntityType"', type=DataType.TEXT),
        ],
    )
    await storage.save_model(model)
    reloaded = await storage.get_model("merchant", data_source="ds")
    assert reloaded is not None
    assert {c.name for c in reloaded.columns} == {"legalEntityType"}


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
    # B does not exist yet — unresolved B.bar ref is silently skipped.
    await storage.save_model(_model_a_to_b(foo_sql="B.bar + 1"))
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
    # No exception expected.
    await storage.save_model(_model_a_to_b(foo_sql="B.bar + 1"))


async def test_save_model_ignores_indirect_join_target_in_cycle_detection(
    tmp_path,
) -> None:
    """Save-time alias resolution must match compile-time strictness:
    a bare ``C.x`` ref on A is only a dependency if A has a *direct* join
    to C. With A → B → C (no direct A→C join), the compile-time expander
    leaves ``C.x`` alone, so the save-time validator must too — even when
    C is reachable from A's join graph BFS."""
    storage = _yaml_storage(tmp_path)
    # If the strict walk treated ``C.x`` on A as a dep, this would close a
    # cycle (A.foo → C.x → A.foo). It must not — A has no direct A→C join.
    model_a, model_b, model_c = _abc_chain(
        a_foo_sql="C.x + 1", c_x_sql="A.foo + 1",
    )
    await storage.save_model(model_c, _validate=False)  # C alone has unresolvable A.foo
    await storage.save_model(model_b)
    # No cycle raised — the indirect-reach ref is not a strict dependency.
    await storage.save_model(model_a)


async def test_save_model_detects_cycle_via_canonical_multihop_path_alias(
    tmp_path,
) -> None:
    """Canonical ``__``-delimited multi-hop path aliases (``B__C.x``) must
    resolve at save time the same way they do at compile time — by
    walking each hop through the join chain. A.foo references B__C.x;
    C.x references A.foo via a back-walk; the cycle must be detected."""
    storage = _yaml_storage(tmp_path)
    # A → B → C; A.foo references B__C.x via canonical multi-hop path.
    # C.x has the back-reference into A.foo (C joins A), so saving A
    # completes a cycle.
    model_a, model_b, model_c = _abc_chain(
        a_foo_sql="B__C.x + 1", c_x_sql="A.foo + 1", c_joins_back_to_a=True,
    )
    await storage.save_model(model_c, _validate=False)  # A doesn't exist yet
    await storage.save_model(model_b)
    with pytest.raises(ColumnCycleError) as exc_info:
        await storage.save_model(model_a)
    msg = str(exc_info.value)
    assert "A.foo" in msg and "C.x" in msg, (
        f"multi-hop cycle chain missing names: {msg}"
    )


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
