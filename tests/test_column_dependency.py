"""Save-time derived-field cycle detection.

Cycles in derived ``DatasetField.sql`` chains must be detected at
``storage.save_dataset`` time so the broken dataset never reaches a query.
Compile-time detection remains as defence in depth.

The validator lives in ``StorageBackend.save_dataset`` (converted to a
template method) so it fires for every save path uniformly — direct
``storage.save_dataset`` calls, ``engine.save_dataset``, MCP edit_dataset, CLI
create/edit, and the migration write-back (the migration path passes
``_validate=False`` so legacy cyclic data remains loadable).
"""

from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.errors import DatasetFieldCycleError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _yaml_storage(tmp_path) -> YAMLStorage:
    return YAMLStorage(base_dir=str(tmp_path))


def _sqlite_storage(tmp_path) -> SQLiteStorage:
    return SQLiteStorage(db_path=str(tmp_path / "storage.db"))


# Fixture factories — the cross-dataset cycle tests below all need variants
# of two dataset graphs (A↔B and A→B→C), so the boilerplate is hoisted out.


def _dataset_a_to_b(*, foo_sql: str) -> SlayerDataset:
    """A dataset with a single derived field ``foo`` and a relationship to B."""
    return SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", sql="b_id", datatype=DataType.FLOAT),
            DatasetField(name="foo", sql=foo_sql, datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )


def _abc_chain(
    *,
    a_foo_sql: str,
    c_x_sql: str,
    c_joins_back_to_a: bool = False,
) -> tuple[SlayerDataset, SlayerDataset, SlayerDataset]:
    """The canonical A → B → C three-dataset scaffold used by the strict
    alias-walk tests. ``foo`` on A and ``x`` on C are derived; their
    expressions are the per-test variable.

    ``c_joins_back_to_a`` adds an ``a_id`` field + a C→A relationship, which
    closes a back-edge needed to make a cycle through C.x reachable.
    """
    c_columns = [
        DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="x", sql=c_x_sql, datatype=DataType.FLOAT),
    ]
    c_joins = []
    if c_joins_back_to_a:
        c_columns.insert(
            1, DatasetField(name="a_id", sql="a_id", datatype=DataType.FLOAT),
        )
        c_joins.append(
            DatasetRelationship(to="A", from_columns=["a_id"], to_columns=["id"]),
        )
    model_c = SlayerDataset(
        name="C",
        data_source="ds",
        sql_table="C",
        fields=c_columns,
        relationships=c_joins,
    )
    model_b = SlayerDataset(
        name="B",
        data_source="ds",
        sql_table="B",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c_id", sql="c_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="C", from_columns=["c_id"], to_columns=["id"])],
    )
    model_a = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", sql="b_id", datatype=DataType.FLOAT),
            DatasetField(name="foo", sql=a_foo_sql, datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    return model_a, model_b, model_c


# ---------------------------------------------------------------------------
# 1. Same-dataset cycles.
# ---------------------------------------------------------------------------


async def test_save_model_rejects_same_model_cycle(tmp_path) -> None:
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c1", sql="c2 + 1", datatype=DataType.FLOAT),
            DatasetField(name="c2", sql="c1 - 1", datatype=DataType.FLOAT),
        ],
    )
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await storage.save_dataset(model)
    # Backwards compat: still catchable as ValueError.
    assert isinstance(exc_info.value, ValueError)
    msg = str(exc_info.value)
    assert "A.c1" in msg, f"cycle chain missing names: {msg}"
    assert "A.c2" in msg, f"cycle chain missing names: {msg}"


async def test_save_model_rejects_three_deep_cycle(tmp_path) -> None:
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c1", sql="c2 + 1", datatype=DataType.FLOAT),
            DatasetField(name="c2", sql="c3 + 1", datatype=DataType.FLOAT),
            DatasetField(name="c3", sql="c1 + 1", datatype=DataType.FLOAT),
        ],
    )
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await storage.save_dataset(model)
    msg = str(exc_info.value)
    for c in ("A.c1", "A.c2", "A.c3"):
        assert c in msg, f"cycle chain missing {c}: {msg}"


async def test_save_model_rejects_self_referential_derived(tmp_path) -> None:
    """A field referencing ITSELF in a NON-trivial expression (sql != name)
    is a single-step cycle. Distinct from the trivial base case where sql
    equals the field name verbatim (that's how base fields are written)."""
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c1", sql="c1 + 1", datatype=DataType.FLOAT),
        ],
    )
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await storage.save_dataset(model)
    assert "A.c1" in str(exc_info.value)


async def test_save_model_accepts_acyclic_derived_dag(tmp_path) -> None:
    """Diamond DAG (d = b + c, b = a + 1, c = a + 2) is acyclic and must
    save cleanly."""
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="a", sql="a", datatype=DataType.FLOAT),
            DatasetField(name="b", sql="a + 1", datatype=DataType.FLOAT),
            DatasetField(name="c", sql="a + 2", datatype=DataType.FLOAT),
            DatasetField(name="d", sql="b + c", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(model)
    reloaded = await storage.get_dataset("A", data_source="ds")
    assert reloaded is not None
    assert {c.name for c in reloaded.fields} == {"id", "a", "b", "c", "d"}


async def test_save_model_accepts_base_columns_only(tmp_path) -> None:
    """Sanity check: a dataset with no derived fields saves cleanly."""
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="a", sql="a", datatype=DataType.FLOAT),
            DatasetField(name="b", sql="b", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(model)


async def test_save_model_accepts_quoted_self_identity_column(tmp_path) -> None:
    """A field whose sql is the double-quoted form of its own name is a base
    reference, not a derived expression — required to reach a mixed-case
    physical column on case-folding dialects (Prisma-style camelCase). It must
    NOT be mistaken for a single-step self-cycle."""
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="merchant",
        data_source="ds",
        sql_table="merchant",
        fields=[
            DatasetField(name="legalEntityType", sql='"legalEntityType"', datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(model)
    reloaded = await storage.get_dataset("merchant", data_source="ds")
    assert reloaded is not None
    assert {c.name for c in reloaded.fields} == {"legalEntityType"}


# ---------------------------------------------------------------------------
# 2. Cross-dataset cycles (within a single data_source).
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
    model_b = SlayerDataset(
        name="B",
        data_source="ds",
        sql_table="B",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="a_id", sql="a_id", datatype=DataType.FLOAT),
            DatasetField(name="y", sql="A.x + 1", datatype=DataType.FLOAT),
        ],
        # No declared B→A edge: the ref resolves over the reverse hop;
        # a second declared edge would make the pair ambiguous.
    )
    await storage.save_dataset(model_b)
    # Now save A with a forward ref to B.y, completing the cycle:
    # A.x → B.y → A.x.
    model_a = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", sql="b_id", datatype=DataType.FLOAT),
            DatasetField(name="x", sql="B.y + 1", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await storage.save_dataset(model_a)
    msg = str(exc_info.value)
    assert "A.x" in msg, f"cross-dataset cycle chain missing names: {msg}"
    assert "B.y" in msg, f"cross-dataset cycle chain missing names: {msg}"


async def test_save_model_rejects_cross_model_cycle_when_second_model_completes_it(
    tmp_path,
) -> None:
    """Order-sensitive: B saves first (A doesn't exist; B.bar's ``A.foo`` ref
    is unresolved and silently skipped — best-effort). When A saves with a
    relationship to B and a back-ref to B.bar, the save-time validator on A's save
    MUST detect the cycle — B.bar's ref resolves over the reverse hop,
    no declared B→A edge needed."""
    storage = _yaml_storage(tmp_path)
    model_b = SlayerDataset(
        name="B",
        data_source="ds",
        sql_table="B",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="a_id", sql="a_id", datatype=DataType.FLOAT),
            DatasetField(name="bar", sql="A.foo + 1", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(model_b)
    cyclic = _dataset_a_to_b(foo_sql="B.bar + 1")
    with pytest.raises(DatasetFieldCycleError):
        await storage.save_dataset(cyclic)


async def test_save_model_rejects_cycle_when_joinless_model_saves_last(
    tmp_path,
) -> None:
    """The saved dataset declares NO relationships: its peers load via the
    datasource-wide prefetch and the cycle crosses A's edge in reverse."""
    storage = _yaml_storage(tmp_path)
    await storage.save_dataset(_dataset_a_to_b(foo_sql="B.bar + 1"))
    model_b = SlayerDataset(
        name="B",
        data_source="ds",
        sql_table="B",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="bar", sql="A.foo + 1", datatype=DataType.FLOAT),
        ],
    )
    with pytest.raises(DatasetFieldCycleError):
        await storage.save_dataset(model_b)


async def test_save_model_tolerates_unresolved_joined_ref(tmp_path) -> None:
    """A's field references B.bar but B is not saved yet. A saves cleanly —
    save-time validation is best-effort and silently skips unresolvable
    refs. (The compile-time guard catches the broken ref at query time.)"""
    storage = _yaml_storage(tmp_path)
    # No exception expected.
    await storage.save_dataset(_dataset_a_to_b(foo_sql="B.bar + 1"))


async def test_save_model_ignores_indirect_join_target_in_cycle_detection(
    tmp_path,
) -> None:
    """Save-time alias resolution must match compile-time strictness:
    a bare ``C.x`` ref on A is only a dependency if A has a *direct* relationship
    to C. With A → B → C (no direct A→C relationship), the compile-time expander
    leaves ``C.x`` alone, so the save-time validator must too — even when
    C is reachable from A's relationship graph BFS."""
    storage = _yaml_storage(tmp_path)
    # If the strict walk treated ``C.x`` on A as a dep, this would close a
    # cycle (A.foo → C.x → A.foo). It must not — A has no direct A→C relationship.
    model_a, model_b, model_c = _abc_chain(
        a_foo_sql="C.x + 1", c_x_sql="A.foo + 1",
    )
    await storage.save_dataset(model_c, _validate=False)  # C alone has unresolvable A.foo
    await storage.save_dataset(model_b)
    # No cycle raised — the indirect-reach ref is not a strict dependency.
    await storage.save_dataset(model_a)


async def test_save_model_detects_cycle_via_canonical_multihop_path_alias(
    tmp_path,
) -> None:
    """Canonical DOTTED multi-hop path chains (``B.C.x``) must resolve at save
    time the same way they do at compile time — by walking each hop through the
    relationship chain (dots are the canonical delimiter; the legacy ``__``
    split-alias no longer resolves). A.foo references B.C.x; C.x references
    A.foo via a back-walk; the cycle must be detected."""
    storage = _yaml_storage(tmp_path)
    # A → B → C; A.foo references B.C.x via the canonical dotted multi-hop path.
    # C.x has the back-reference into A.foo (C relates to A), so saving A
    # completes a cycle.
    model_a, model_b, model_c = _abc_chain(
        a_foo_sql="B.C.x + 1", c_x_sql="A.foo + 1", c_joins_back_to_a=True,
    )
    await storage.save_dataset(model_c, _validate=False)  # A doesn't exist yet
    await storage.save_dataset(model_b)
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await storage.save_dataset(model_a)
    msg = str(exc_info.value)
    assert "A.foo" in msg, f"multi-hop cycle chain missing names: {msg}"
    assert "C.x" in msg, f"multi-hop cycle chain missing names: {msg}"


async def test_save_model_skips_subquery_scope_refs_in_cycle_detection(
    tmp_path,
) -> None:
    """A bare ref inside a subquery is NOT a derived-field dependency —
    the subquery has its own scope. So a dataset where the only ``cycle``
    is hidden inside a subquery does NOT raise at save time."""
    storage = _yaml_storage(tmp_path)
    model = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_a", sql="raw_a", datatype=DataType.FLOAT),
            # c1 references c2 inside a subquery — out of scope for cycle
            # detection (the subquery's bare ``c2`` is not c2-on-this-dataset).
            # The root-scope expression uses only base raw_a, so no cycle.
            DatasetField(
                name="c1",
                sql="(SELECT MAX(c2) FROM other_table) + raw_a",
                datatype=DataType.FLOAT,
            ),
            # c2 references c1 inside a subquery — same treatment.
            DatasetField(
                name="c2",
                sql="(SELECT MAX(c1) FROM other_table) + raw_a",
                datatype=DataType.FLOAT,
            ),
        ],
    )
    # No exception — subquery-scope refs are not dependencies.
    await storage.save_dataset(model)


# ---------------------------------------------------------------------------
# 3. Template-method dispatch: validation must fire through every concrete
# backend. Parameterised so a future backend that overrides _save_dataset_impl
# without remembering to call super().save_dataset still gets validated.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("backend_factory", [_yaml_storage, _sqlite_storage])
async def test_save_model_template_method_runs_for_yaml_and_sqlite_backends(
    tmp_path: Path, backend_factory: Callable[[Path], StorageBackend],
) -> None:
    storage = backend_factory(tmp_path)
    cyclic = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c1", sql="c2 + 1", datatype=DataType.FLOAT),
            DatasetField(name="c2", sql="c1 - 1", datatype=DataType.FLOAT),
        ],
    )
    with pytest.raises(DatasetFieldCycleError):
        await storage.save_dataset(cyclic)


# ---------------------------------------------------------------------------
# 4. Migration write-back: legacy cyclic data must remain LOADABLE.
# storage.get_dataset() calls save_dataset() internally after running migrations,
# so the implicit write-back at base.py:_migrate_and_refine_on_load must
# bypass cycle validation. Otherwise the user could never repair a broken
# legacy YAML file through the API.
# ---------------------------------------------------------------------------


async def test_save_model_migration_writeback_does_not_validate(tmp_path) -> None:
    """Write a legacy v4 cyclic dataset to disk by hand (bypassing save_dataset),
    then load it through storage.get_dataset. The migration write-back must
    not raise — the cycle should be tolerated on load."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    # Persist a datasource so the migration's type-refinement step does not
    # hard-fail on "datasource unavailable" — we want the cycle path to be
    # the only thing being tested here.
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:")
    )

    # Write a hand-rolled v4 YAML with a cycle, no Pydantic validation, no
    # save_dataset. v4 is one below the current v5, so loading triggers a
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
    loaded = await storage.get_dataset("A", data_source="ds")
    assert loaded is not None
    assert loaded.name == "A"
    assert {c.name for c in loaded.fields} == {"id", "c1", "c2"}


async def test_save_model_explicit_skip_validate_kwarg(tmp_path) -> None:
    """The migration path needs an explicit escape hatch. The template
    method must accept ``_validate=False`` so callers in the migration
    path can persist legacy data unchanged."""
    storage = _yaml_storage(tmp_path)
    cyclic = SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c1", sql="c2 + 1", datatype=DataType.FLOAT),
            DatasetField(name="c2", sql="c1 - 1", datatype=DataType.FLOAT),
        ],
    )
    # No exception — explicit _validate=False bypasses validation.
    await storage.save_dataset(cyclic, _validate=False)
    reloaded = await storage.get_dataset("A", data_source="ds")
    assert reloaded is not None
