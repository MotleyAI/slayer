"""DEV-1367: refuse queries that reference models or columns the engine
cannot bind.

Three classes of bug all surface as unbound table / column references in
generated SQL today, producing cryptic database-runtime errors instead of
clean translation-time ``ValueError``s:

1. **Filter dotted-ref to unjoined model** — e.g.
   ``filters=["transportation_assets.total_vehicles >= 3"]`` when
   ``transportation_assets`` is not in the source model's ``joins``. The
   issue's exact reproducer.
2. **Filter ref to nonexistent column** on a model that IS joined.
3. **Bare-name typo** — a filter that names a column that simply does not
   exist on the source model.

The same silent-drop affects dimensions / time-dimensions / cross-model
measures whenever the referenced model isn't reachable via the join
graph from the source model.

These tests pin the contract: every such case raises ``ValueError`` at
``enrich_query`` time with a message that names (a) the offending
construct (filter string / dimension ref / measure formula), (b) the
unresolvable model or column, and (c) the source model.

Negative regressions assert that the legitimate auto-join cases — joined
dotted refs (``customers.region``) and bare-name local derived columns
whose ``sql`` crosses joins (DEV-1334) — keep working.
"""
from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _engine_with_storage(tmp_path) -> tuple[SlayerQueryEngine, YAMLStorage]:
    storage = YAMLStorage(base_dir=str(tmp_path))
    return SlayerQueryEngine(storage=storage), storage


async def _gen_sql(
    engine: SlayerQueryEngine, query: SlayerQuery, model: SlayerModel,
    *, dialect: str = "sqlite",
) -> str:
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect=dialect).generate(enriched=enriched)


async def _save_a_b(
    storage: YAMLStorage,
    *,
    a_extra_columns: list[Column] | None = None,
    a_extra_measures: list[ModelMeasure] | None = None,
    a_filters: list[str] | None = None,
) -> SlayerModel:
    """Build A → B fixture. A joins B; B has columns ``id``, ``foo``,
    ``region``. A has columns ``id``, ``b_id``, ``amount``, ``ts`` plus
    whatever extras the caller injects.
    """
    model_b = SlayerModel(
        name="B",
        data_source="test",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="foo", sql="foo", type=DataType.DOUBLE),
            Column(name="region", sql="region", type=DataType.TEXT),
        ],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="ts", sql="ts", type=DataType.TIMESTAMP),
            *(a_extra_columns or []),
        ],
        measures=list(a_extra_measures or []),
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
        filters=list(a_filters or []),
    )
    await storage.save_model(model_a)
    return model_a


async def _save_a_only(
    storage: YAMLStorage,
    *,
    a_extra_columns: list[Column] | None = None,
    a_extra_measures: list[ModelMeasure] | None = None,
    a_filters: list[str] | None = None,
) -> SlayerModel:
    """Build a stand-alone A with no joins. Used for bare-name typo cases
    where the bug surfaces without any cross-model context."""
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            *(a_extra_columns or []),
        ],
        measures=list(a_extra_measures or []),
        filters=list(a_filters or []),
    )
    await storage.save_model(model_a)
    return model_a


# ---------------------------------------------------------------------------
# 1. Query filter dotted ref to unjoined model — the DEV-1367 reproducer.
# ---------------------------------------------------------------------------


async def test_query_filter_dotted_ref_to_unjoined_model(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["transportation_assets.total_vehicles >= 3"],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "transportation_assets" in msg, msg
    assert "A" in msg, msg
    assert "transportation_assets.total_vehicles >= 3" in msg, msg


async def test_query_filter_multihop_dotted_ref_unjoined_at_second_hop(
    tmp_path,
) -> None:
    """``B.warehouses.foo`` — ``B`` IS joined to A, but ``warehouses`` is
    not joined to ``B``. The error must mention ``warehouses`` and ``B``.
    """
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["B.warehouses.foo > 0"],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "warehouses" in msg, msg
    # Failure happens on the B → warehouses hop, so B is the parent.
    assert "B" in msg, msg


# ---------------------------------------------------------------------------
# 2. Model-level filter dotted ref to unjoined model.
# ---------------------------------------------------------------------------


async def test_model_filter_dotted_ref_to_unjoined_model(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(
        storage,
        a_filters=["transportation_assets.total_vehicles >= 3"],
    )
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "transportation_assets" in msg, msg
    assert "A" in msg, msg


# ---------------------------------------------------------------------------
# 3. Column.filter (CASE WHEN at aggregation time) dotted ref to unjoined.
# ---------------------------------------------------------------------------


async def test_column_filter_dotted_ref_to_unjoined_model(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(
        storage,
        a_extra_columns=[
            Column(
                name="filtered_amount",
                sql="amount",
                type=DataType.DOUBLE,
                filter="transportation_assets.flag = 1",
            ),
        ],
    )
    query = SlayerQuery(
        source_model="A",
        measures=[ModelMeasure(formula="filtered_amount:sum")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "transportation_assets" in msg, msg


# ---------------------------------------------------------------------------
# 4. Filter dotted ref names a JOINED model but a column that does not exist.
# ---------------------------------------------------------------------------


async def test_query_filter_dotted_ref_joined_model_missing_column(
    tmp_path,
) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["B.nonexistent_col > 0"],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "nonexistent_col" in msg, msg
    assert "B" in msg, msg


# ---------------------------------------------------------------------------
# 5. Dimension ref to unjoined model.
# ---------------------------------------------------------------------------


async def test_dimension_ref_to_unjoined_model(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="total_vehicles", model="transportation_assets")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "transportation_assets" in msg, msg


# ---------------------------------------------------------------------------
# 6. Time-dimension ref to unjoined model.
# ---------------------------------------------------------------------------


async def test_time_dimension_ref_to_unjoined_model(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="event_time", model="transportation_assets"),
                granularity=TimeGranularity.DAY,
            ),
        ],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "transportation_assets" in msg, msg


# ---------------------------------------------------------------------------
# 7. Cross-model measure ref to unjoined model.
# ---------------------------------------------------------------------------


async def test_cross_model_measure_unjoined(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        measures=[ModelMeasure(formula="customers.revenue:sum")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "customers" in msg, msg


# ---------------------------------------------------------------------------
# 8. Bare-name typo in a query-level filter (not a column, not a known
# allowlisted token).
# ---------------------------------------------------------------------------


async def test_query_filter_bare_name_typo(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["nonexistent_col > 100"],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "nonexistent_col" in msg, msg
    assert "A" in msg, msg


async def test_model_filter_bare_name_typo(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(storage, a_filters=["nonexistent_col > 100"])
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "nonexistent_col" in msg, msg


async def test_column_filter_bare_name_typo(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(
        storage,
        a_extra_columns=[
            Column(
                name="filtered_amt",
                sql="amount",
                type=DataType.DOUBLE,
                filter="nonexistent_col > 0",
            ),
        ],
    )
    query = SlayerQuery(
        source_model="A",
        measures=[ModelMeasure(formula="filtered_amt:sum")],
    )
    with pytest.raises(ValueError) as excinfo:
        await _gen_sql(engine, query, model_a)
    msg = str(excinfo.value)
    assert "nonexistent_col" in msg, msg


# ---------------------------------------------------------------------------
# 9. Bare-name allowlist negatives — these MUST NOT raise. Each filter
# references something that's legitimately in the allowlist:
#   * SQL keywords (null, true, false)
#   * the canonical alias for ``*:count`` (``_count``)
#   * a named ModelMeasure formula on the same model
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filter_str",
    [
        "amount is null",
        "amount = true",
        "amount = false",
    ],
)
async def test_filter_allowlist_sql_keywords_do_not_raise(
    tmp_path, filter_str: str,
) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=[filter_str],
    )
    # Must not raise — these are valid SQL even if the comparison shape
    # is unusual.
    await _gen_sql(engine, query, model_a)


async def test_filter_allowlist_named_measure_does_not_raise(tmp_path) -> None:
    """A bare-name filter referencing a saved ``ModelMeasure`` (post-agg
    HAVING-style filter) must not be rejected by the bare-name guard.
    """
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(
        storage,
        a_extra_measures=[ModelMeasure(formula="amount:sum", name="total_amt")],
    )
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        measures=[ModelMeasure(formula="total_amt")],
        filters=["total_amt > 0"],
    )
    await _gen_sql(engine, query, model_a)


async def test_filter_allowlist_star_count_alias_does_not_raise(tmp_path) -> None:
    """``*:count`` materializes as the alias ``_count``. A filter that
    references it by that alias must not be rejected.
    """
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_only(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        measures=[ModelMeasure(formula="*:count")],
        filters=["_count > 0"],
    )
    await _gen_sql(engine, query, model_a)


# ---------------------------------------------------------------------------
# 10. Negative regression — joined-model dotted ref still auto-joins.
# ---------------------------------------------------------------------------


async def test_filter_dotted_ref_joined_model_still_works(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["B.region = 'US'"],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = " ".join(sql.split())
    assert "JOIN B" in norm, sql
    assert "B.region" in norm, sql


# ---------------------------------------------------------------------------
# 11. Negative regression — DEV-1334: bare-name local derived column
# whose ``sql`` crosses a join must still trigger the implied LEFT JOIN.
# ---------------------------------------------------------------------------


async def test_filter_bare_local_derived_crossing_join_still_auto_joins(
    tmp_path,
) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    # ``is_eu`` lives on A but its sql references B — the planner walks
    # the chain and adds the join automatically per DEV-1334.
    model_a = await _save_a_b(
        storage,
        a_extra_columns=[
            Column(
                name="is_eu",
                sql="CASE WHEN B.region = 'EU' THEN 1 ELSE 0 END",
                type=DataType.INT,
            ),
        ],
    )
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="id")],
        filters=["is_eu = 1"],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = " ".join(sql.split())
    assert "JOIN B" in norm, sql
    assert "B.region" in norm, sql
