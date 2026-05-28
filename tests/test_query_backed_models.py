"""Tests for query-backed models — engine.execute(str), variable precedence,
and error paths.

These tests run against an in-process SQLite datasource (dry_run=False would
need real data; we use ``dry_run=True`` plus assertions on the generated SQL
to keep tests hermetic and fast).
"""
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders_t",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


def _ds() -> DatasourceConfig:
    return DatasourceConfig(name="ds", type="sqlite", database=":memory:")


async def _engine_with_orders(*extra_models: SlayerModel) -> tuple:
    """Build a YAMLStorage with `orders` saved + any extras, and an engine.

    Returns (engine, tmpdir_handle). Caller must keep the tmpdir alive.
    """
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(_ds())
    await storage.save_model(_orders_model())
    for m in extra_models:
        await storage.save_model(m)
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


class TestExecuteByName:
    async def test_missing_model_raises(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            with pytest.raises(ValueError, match="Model 'nope' not found"):
                await engine.execute("nope")
        finally:
            tmp.cleanup()

    async def test_table_backed_model_raises_clear_error(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            with pytest.raises(ValueError, match="not query-backed"):
                await engine.execute("orders")
        finally:
            tmp.cleanup()

    async def test_query_backed_runs_dry(self) -> None:
        saved = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_by_region", dry_run=True)
            assert resp.sql is not None
            assert "amount" in resp.sql.lower()
            assert "region" in resp.sql.lower()
        finally:
            tmp.cleanup()


class TestVariablePrecedence:
    async def test_runtime_kwarg_overrides_stage_variables(self) -> None:
        """``execute(query, variables={...})`` overrides query.variables."""
        engine, tmp = await _engine_with_orders()
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                filters=["region = '{r}'"],
                variables={"r": "US"},
            )
            resp = await engine.execute(q, variables={"r": "EU"}, dry_run=True)
            assert resp.sql is not None
            assert "'EU'" in resp.sql
            assert "'US'" not in resp.sql
        finally:
            tmp.cleanup()

    async def test_runtime_kwarg_overrides_query_variables_when_not_set_on_query(
        self,
    ) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                filters=["region = '{r}'"],
            )
            resp = await engine.execute(q, variables={"r": "EU"}, dry_run=True)
            assert resp.sql is not None
            assert "'EU'" in resp.sql
        finally:
            tmp.cleanup()

    async def test_unknown_kwarg_silently_ignored(self) -> None:
        """Variables not referenced in any filter are silently dropped."""
        engine, tmp = await _engine_with_orders()
        try:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                filters=["region = '{r}'"],
                variables={"r": "US"},
            )
            resp = await engine.execute(q, variables={"unrelated": 99}, dry_run=True)
            # 'r' kept its original value because the kwarg didn't override it
            # ('unrelated' is unknown and silently ignored).
            assert "'US'" in resp.sql
        finally:
            tmp.cleanup()

    async def test_run_by_name_with_runtime_kwarg(self) -> None:
        """``execute("M", variables=K)`` threads K into stage filter substitution."""
        saved = SlayerModel(
            name="rev_filtered",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region = '{r}'"],
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered", variables={"r": "US"}, dry_run=True)
            assert resp.sql is not None
            assert "'US'" in resp.sql
        finally:
            tmp.cleanup()

    async def test_run_by_name_uses_query_variables_default(self) -> None:
        """When no kwarg, ``model.query_variables`` provides defaults to the stage."""
        saved = SlayerModel(
            name="rev_filtered",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region = '{r}'"],
            )],
            query_variables={"r": "DEFAULT_R"},
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered", dry_run=True)
            assert "'DEFAULT_R'" in resp.sql
        finally:
            tmp.cleanup()

    async def test_run_by_name_kwarg_overrides_query_variables_default(self) -> None:
        saved = SlayerModel(
            name="rev_filtered",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                filters=["region = '{r}'"],
            )],
            query_variables={"r": "DEFAULT_R"},
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered", variables={"r": "US"}, dry_run=True)
            assert "'US'" in resp.sql
            assert "DEFAULT_R" not in resp.sql
        finally:
            tmp.cleanup()


class TestCreateModelFromQuery:
    async def test_caches_columns_and_sql(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            saved = await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                name="rev_by_region",
            )
            # Cache is populated
            assert len(saved.columns) >= 2  # region + amount_sum
            col_names = {c.name for c in saved.columns}
            assert "region" in col_names
            assert "amount_sum" in col_names
            assert saved.backing_query_sql is not None
            assert "amount" in saved.backing_query_sql.lower()
            # Reload from storage and confirm cache persisted
            from_storage = await engine.storage.get_model("rev_by_region")
            assert from_storage is not None
            assert from_storage.backing_query_sql == saved.backing_query_sql
            assert [c.name for c in from_storage.columns] == [c.name for c in saved.columns]
        finally:
            tmp.cleanup()

    async def test_variables_kwarg_populates_query_variables(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            saved = await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    filters=["region = '{r}'"],
                ),
                name="rev_filtered",
                variables={"r": "US"},
            )
            assert saved.query_variables == {"r": "US"}
        finally:
            tmp.cleanup()

    async def test_save_false_returns_populated_model_without_persisting(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            built = await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                name="not_persisted",
                save=False,
            )
            assert built.backing_query_sql is not None
            assert built.columns
            # Storage doesn't have it
            from_storage = await engine.storage.get_model("not_persisted")
            assert from_storage is None
        finally:
            tmp.cleanup()

    async def test_save_time_placeholder_fill_for_unresolved_var(self) -> None:
        """Save succeeds when filters reference an unresolved {var} — '0' is
        substituted at save-time so SQL generation doesn't fail."""
        engine, tmp = await _engine_with_orders()
        try:
            saved = await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    filters=["amount > {threshold}"],
                ),
                name="filtered_no_default",
                # No variables= → {threshold} will be filled with '0' at save
            )
            # Cache populated despite unresolved variable
            assert saved.backing_query_sql is not None
            assert saved.query_variables == {}
        finally:
            tmp.cleanup()

    async def test_accepts_dict_query(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            saved = await engine.create_model_from_query(
                query={
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                    "dimensions": ["region"],
                },
                name="from_dict",
            )
            assert saved.source_queries is not None
            assert len(saved.source_queries) == 1
        finally:
            tmp.cleanup()


class TestSaveModelGuards:
    async def test_rejects_user_columns_on_query_backed(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            from slayer.core.enums import DataType
            from slayer.core.models import Column

            m = SlayerModel(
                name="bad",
                data_source="ds",
                source_queries=[SlayerQuery(source_model="orders")],
                columns=[Column(name="x", sql="x", type=DataType.TEXT)],
            )
            with pytest.raises(ValueError, match="auto-generated and must not be supplied"):
                await engine.save_model(m)
        finally:
            tmp.cleanup()

    async def test_rejects_user_backing_query_sql(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            m = SlayerModel(
                name="bad2",
                data_source="ds",
                source_queries=[SlayerQuery(source_model="orders")],
                backing_query_sql="SELECT 1",
            )
            with pytest.raises(ValueError, match="backing_query_sql.*auto-managed"):
                await engine.save_model(m)
        finally:
            tmp.cleanup()


def _wrap_save_counter(storage):
    """Spy on ``storage.save_model``. Returns ``(calls, restore)`` where
    ``calls`` is a live list of saved-model names and ``restore()`` undoes
    the wrapping. Used to assert that read paths never write to storage.
    """
    calls: list[str] = []
    real = storage.save_model

    async def counting(m):
        calls.append(m.name)
        return await real(m)

    storage.save_model = counting  # type: ignore[method-assign]

    def restore() -> None:
        storage.save_model = real  # type: ignore[method-assign]

    return calls, restore


class TestCacheRefreshOnExecute:
    """Read paths must never write to storage. Cache (`columns`,
    `backing_query_sql`, `data_source`) is populated only by
    ``engine.save_model`` / ``create_model_from_query(save=True)``.
    Lost-update race fix: see issue #74.
    """

    async def test_execute_does_not_populate_cache_for_raw_saved_model(self) -> None:
        """A query-backed model written directly to storage (bypassing the
        engine) keeps its empty cache after ``engine.execute`` — read paths
        no longer initialize cache as a side effect.
        """
        empty = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
            )],
        )
        engine, tmp = await _engine_with_orders()
        try:
            await engine.storage.save_model(empty)
            stored = await engine.storage.get_model("rev_by_region")
            assert stored is not None
            assert stored.columns == []
            assert stored.backing_query_sql is None

            await engine.execute("rev_by_region", dry_run=True)
            refreshed = await engine.storage.get_model("rev_by_region")
            assert refreshed is not None
            assert refreshed.columns == []
            assert refreshed.backing_query_sql is None
        finally:
            tmp.cleanup()

    async def test_model_extension_over_query_backed_model_adds_columns(self) -> None:
        """ModelExtension wrapping a saved query-backed model adds extra
        columns to the resolved virtual model — but must NOT write those
        extension columns back into the base model's persisted cache.
        """
        engine, tmp = await _engine_with_orders()
        try:
            await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                name="rev_by_region",
            )
            # Outer query uses the saved query-backed model under a
            # ModelExtension that adds an extra computed column.
            outer = SlayerQuery.model_validate({
                "source_model": {
                    "source_name": "rev_by_region",
                    "columns": [{
                        "name": "is_high_rev",
                        "sql": "CASE WHEN amount_sum > 1000 THEN 1 ELSE 0 END",
                        "type": "number",
                    }],
                },
                "dimensions": ["region", "is_high_rev"],
                "measures": [{"formula": "amount_sum:max"}],
            })
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            assert "is_high_rev" in resp.sql
            assert "amount_sum" in resp.sql

            # Extension columns must not bleed into the base model's cache.
            base = await engine.storage.get_model("rev_by_region")
            assert base is not None
            assert not any(c.name == "is_high_rev" for c in base.columns)
            assert "is_high_rev" not in (base.backing_query_sql or "")
        finally:
            tmp.cleanup()

    async def test_model_extension_over_named_query_stage_adds_columns(self) -> None:
        """ModelExtension wrapping a named-query stage in a runtime list adds
        extra columns to the resolved virtual model. The named-query stage
        is ephemeral and must never get persisted as a side effect.
        """
        engine, tmp = await _engine_with_orders()
        try:
            queries = [
                SlayerQuery(
                    name="staged",
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                SlayerQuery.model_validate({
                    "source_model": {
                        "source_name": "staged",
                        "columns": [{
                            "name": "doubled",
                            "sql": "amount_sum * 2",
                            "type": "number",
                        }],
                    },
                    "dimensions": ["region", "doubled"],
                    "measures": [{"formula": "amount_sum:max"}],
                }),
            ]
            resp = await engine.execute(queries, dry_run=True)
            assert resp.sql is not None
            assert "doubled" in resp.sql

            # Named-query stage must not have been persisted.
            assert await engine.storage.get_model("staged") is None
        finally:
            tmp.cleanup()

    async def test_execute_never_writes_to_storage(self) -> None:
        """Stronger invariant: ``engine.execute`` never calls
        ``storage.save_model`` regardless of cache state — populated, empty,
        or stale, with or without runtime/outer variables. Eliminates the
        lost-update race (issue #74) by removing the writer entirely.
        """
        engine, tmp = await _engine_with_orders()
        try:
            await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                name="rev_by_region",
            )

            # Case 1: cache freshly populated by save above — execute must not write.
            calls, restore = _wrap_save_counter(engine.storage)
            try:
                await engine.execute("rev_by_region", dry_run=True)
                assert calls == [], f"populated-cache execute wrote: {calls}"
            finally:
                restore()

            # Case 2: outer query carries its own variables (formerly took the
            # canonical-second-render branch). Execute must still not write.
            calls, restore = _wrap_save_counter(engine.storage)
            try:
                await engine.execute(SlayerQuery(
                    source_model="rev_by_region",
                    dimensions=["region"],
                    measures=[{"formula": "amount_sum:max"}],
                    variables={"unused": "X"},
                ), dry_run=True)
                assert calls == [], f"outer-variables execute wrote: {calls}"
            finally:
                restore()

            # Case 3: empty cache (raw storage save bypassing the engine).
            await engine.storage.save_model(SlayerModel(
                name="raw_qb",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                )],
            ))
            calls, restore = _wrap_save_counter(engine.storage)
            try:
                await engine.execute("raw_qb", dry_run=True)
                assert calls == [], f"empty-cache execute wrote: {calls}"
            finally:
                restore()

            # Case 4: stale cache (persisted SQL doesn't match what would now
            # resolve). We forge a stale entry by hand-saving with a bogus
            # backing_query_sql; execute still must not rewrite it.
            stale = (await engine.storage.get_model("rev_by_region"))
            assert stale is not None
            stale_sql = "SELECT 'stale' AS region"
            await engine.storage.save_model(
                stale.model_copy(update={"backing_query_sql": stale_sql})
            )
            calls, restore = _wrap_save_counter(engine.storage)
            try:
                await engine.execute("rev_by_region", dry_run=True)
                assert calls == [], f"stale-cache execute wrote: {calls}"
            finally:
                restore()
            after = await engine.storage.get_model("rev_by_region")
            assert after is not None
            assert after.backing_query_sql == stale_sql, (
                "execute must not overwrite even an obviously stale cache"
            )
        finally:
            tmp.cleanup()


class TestQueryBackedColumnTypes:
    """``engine.get_column_types`` must derive its datasource from the
    expanded query-backed model, not from the (possibly stale or blank)
    stored ``data_source`` on the unexpanded record.
    """

    async def test_get_column_types_uses_resolved_datasource(self) -> None:
        from slayer.core.enums import DataType
        from slayer.core.models import Column, DatasourceConfig

        tmp = tempfile.TemporaryDirectory()
        try:
            storage = YAMLStorage(base_dir=tmp.name)
            ds_path = f"{tmp.name}/probe.db"  # file-backed so the table persists across connections
            await storage.save_datasource(DatasourceConfig(name="ds_b", type="sqlite", database=ds_path))
            await storage.save_model(SlayerModel(
                name="t_b", sql_table="orders_t", data_source="ds_b",
                columns=[
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            ))
            engine = SlayerQueryEngine(storage=storage)

            # Pre-create the table so the type probe can run.
            # SlayerSQLClient.execute() expects rowsets, so use sqlite3 directly for DDL.
            import sqlite3
            conn = sqlite3.connect(ds_path)
            conn.execute("CREATE TABLE orders_t (amount NUMERIC)")
            conn.execute("INSERT INTO orders_t (amount) VALUES (1)")
            conn.commit()
            conn.close()

            # Register a second, deliberately-stale datasource and pin the
            # query-backed model to it. Saving via raw storage skips the
            # cache populator that would otherwise refresh data_source from
            # the resolved virtual model, so the on-disk record disagrees
            # with the source query's actual datasource (ds_b).
            await storage.save_datasource(DatasourceConfig(name="ds_stale", type="sqlite", database=":memory:"))
            await storage.save_model(SlayerModel(
                name="qb",
                data_source="ds_stale",  # stale: real source is ds_b
                source_queries=[SlayerQuery(
                    source_model="t_b",
                    measures=[{"formula": "amount:sum"}],
                )],
            ))
            stored = await storage.get_model("qb")
            assert stored is not None and stored.data_source == "ds_stale", (
                "test setup expects stale data_source on the raw-saved model"
            )

            # Should resolve datasource from the expanded model (ds_b), open
            # the right SQLite file, and successfully probe — ignoring the
            # stale ds_stale on the stored record.
            types = await engine.get_column_types("qb")
            assert types, "expected non-empty column type map"
        finally:
            tmp.cleanup()

    async def test_get_column_types_with_required_unbound_variable(self) -> None:
        """A query-backed model with a required-but-undefaulted ``{var}``
        placeholder should still produce a column-type map (the type probe
        runs with placeholder fill, not with caller variables). Codex review
        of PR #67 commit 73f69b0.
        """
        from slayer.core.enums import DataType
        from slayer.core.models import Column, DatasourceConfig

        tmp = tempfile.TemporaryDirectory()
        try:
            storage = YAMLStorage(base_dir=tmp.name)
            ds_path = f"{tmp.name}/probe.db"
            await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=ds_path))
            await storage.save_model(SlayerModel(
                name="t", sql_table="orders_t", data_source="ds",
                columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            ))
            import sqlite3
            conn = sqlite3.connect(ds_path)
            conn.execute("CREATE TABLE orders_t (amount NUMERIC)")
            conn.execute("INSERT INTO orders_t (amount) VALUES (1)")
            conn.commit()
            conn.close()

            # `{threshold}` is referenced in the filter but no default is set
            # at either model.query_variables or stage.variables.
            engine = SlayerQueryEngine(storage=storage)
            await engine.save_model(SlayerModel(
                name="qb_unbound",
                source_queries=[SlayerQuery(
                    source_model="t",
                    measures=[{"formula": "amount:sum"}],
                    filters=["amount > {threshold}"],
                )],
                # NO query_variables — threshold is required at run time.
            ))
            types = await engine.get_column_types("qb_unbound")
            assert types, (
                "type probing must succeed for query-backed models with "
                "unbound required variables (placeholder fill applies)"
            )
        finally:
            tmp.cleanup()


class TestBackingQuerySQLCacheHygiene:
    """``backing_query_sql`` is the canonical placeholder-fill render produced
    by ``engine.save_model`` and must not capture per-request runtime
    variables. After issue #74, read paths can no longer write to storage at
    all — so the per-request leak is structurally impossible. These tests
    pin both invariants: the save-time render is canonical, AND no execute
    call (with runtime kwargs or outer-query variables) can modify it.
    """

    async def test_runtime_variables_do_not_leak_into_persisted_sql(
        self,
    ) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            await engine.save_model(SlayerModel(
                name="rev_filtered",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    filters=["region = '{r}'"],
                )],
                query_variables={"r": "DEFAULT_R"},
            ))
            initial = await engine.storage.get_model("rev_filtered")
            initial_sql = initial.backing_query_sql
            # Save-time canonical render should already have the default value.
            assert initial_sql is not None
            assert "'DEFAULT_R'" in initial_sql

            # Execute with a runtime variable that overrides the default —
            # must not modify the persisted cache.
            await engine.execute("rev_filtered", variables={"r": "REQUEST_VAL"}, dry_run=True)

            after = await engine.storage.get_model("rev_filtered")
            assert after is not None
            assert "'REQUEST_VAL'" not in (after.backing_query_sql or "")
            assert after.backing_query_sql == initial_sql
        finally:
            tmp.cleanup()

    async def test_outer_query_variables_do_not_leak_into_persisted_sql(
        self,
    ) -> None:
        """Variables from an enclosing ``SlayerQuery.variables`` must also not
        modify ``backing_query_sql``.
        """
        engine, tmp = await _engine_with_orders()
        try:
            await engine.save_model(SlayerModel(
                name="rev_filtered",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                    filters=["region = '{r}'"],
                )],
                query_variables={"r": "DEFAULT_R"},
            ))
            initial_sql = (await engine.storage.get_model("rev_filtered")).backing_query_sql
            assert initial_sql and "'DEFAULT_R'" in initial_sql

            # Outer query supplies r via SlayerQuery.variables (NOT runtime kwarg).
            outer = SlayerQuery(
                source_model="rev_filtered",
                dimensions=["region"],
                measures=[{"formula": "amount_sum:max"}],
                variables={"r": "OUTER_VAL"},
            )
            await engine.execute(outer, dry_run=True)
            after_sql = (await engine.storage.get_model("rev_filtered")).backing_query_sql
            assert "'OUTER_VAL'" not in (after_sql or ""), (
                f"outer query variables must not be persisted, got:\n{after_sql}"
            )
            assert after_sql == initial_sql
        finally:
            tmp.cleanup()

    async def test_data_source_refreshed_when_backing_query_changes(self) -> None:
        """Editing a query-backed model so its final stage now resolves through
        a different datasource must update the persisted ``data_source`` even
        when the caller still passes the old one — otherwise
        ``get_column_types()`` opens the wrong client.
        """
        from slayer.core.enums import DataType
        from slayer.core.models import Column, DatasourceConfig

        tmp = tempfile.TemporaryDirectory()
        try:
            storage = YAMLStorage(base_dir=tmp.name)
            await storage.save_datasource(DatasourceConfig(name="ds_a", type="sqlite", database=":memory:"))
            await storage.save_datasource(DatasourceConfig(name="ds_b", type="sqlite", database=":memory:"))
            await storage.save_model(SlayerModel(
                name="t_a", sql_table="t_a", data_source="ds_a",
                columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            ))
            await storage.save_model(SlayerModel(
                name="t_b", sql_table="t_b", data_source="ds_b",
                columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            ))
            engine = SlayerQueryEngine(storage=storage)

            await engine.save_model(SlayerModel(
                name="qb",
                data_source="ds_a",
                source_queries=[SlayerQuery(
                    source_model="t_a",
                    measures=[{"formula": "amount:sum"}],
                )],
            ))
            assert (await storage.get_model("qb")).data_source == "ds_a"

            # Edit: backing query now resolves through ds_b, but the caller
            # still passes the stale data_source="ds_a". The engine must
            # overwrite from the virtual model.
            await engine.save_model(SlayerModel(
                name="qb",
                data_source="ds_a",  # stale!
                source_queries=[SlayerQuery(
                    source_model="t_b",
                    measures=[{"formula": "amount:sum"}],
                )],
            ))
            assert (await storage.get_model("qb")).data_source == "ds_b", (
                "data_source must be refreshed from the virtual model, not "
                "preserved from the stale caller-supplied value"
            )
        finally:
            tmp.cleanup()


class TestInlineQueryBackedSourceModel:
    """``source_model`` may be an inline ``SlayerModel(source_queries=[...])`` —
    that model must be expanded into a virtual model with executable SQL,
    same as a stored query-backed model.
    """

    async def test_inline_slayermodel_with_source_queries_executes(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            inline = SlayerModel(
                name="inline_qb",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                )],
            )
            outer = SlayerQuery(
                source_model=inline,
                dimensions=["region"],
                measures=[{"formula": "amount_sum:max"}],
            )
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            # The outer query references amount_sum (the inner result column).
            assert "amount_sum" in resp.sql.lower()
            assert "region" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_inline_dict_slayermodel_with_source_queries_executes(self) -> None:
        engine, tmp = await _engine_with_orders()
        try:
            outer = SlayerQuery.model_validate({
                "source_model": {
                    "name": "inline_qb_dict",
                    "data_source": "ds",
                    "source_queries": [{
                        "source_model": "orders",
                        "measures": [{"formula": "amount:sum"}],
                        "dimensions": ["region"],
                    }],
                },
                "dimensions": ["region"],
                "measures": [{"formula": "amount_sum:max"}],
            })
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            assert "amount_sum" in resp.sql.lower()
        finally:
            tmp.cleanup()


class TestJoinTargetIsQueryBacked:
    """Joining onto a saved query-backed model: the join target must be
    expanded through the same resolution path so its rendered SQL is
    available as the join source.
    """

    async def test_join_target_with_variables_uses_runtime_value(self) -> None:
        """Saved query-backed join target with `filters=["amount > {threshold}"]`
        must see the enclosing query's runtime ``variables`` — not the cached
        placeholder-fill or model defaults.
        """
        from slayer.core.models import ModelJoin

        engine, tmp = await _engine_with_orders()
        try:
            # Save a query-backed rollup whose stage filter references {threshold}.
            await engine.save_model(SlayerModel(
                name="rev_filtered",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                    filters=["amount > {threshold}"],
                )],
                query_variables={"threshold": 0},  # save-time default
            ))
            # Add a join from orders → rev_filtered on region.
            orders = await engine.storage.get_model("orders")
            orders = orders.model_copy(update={
                "joins": [ModelJoin(
                    target_model="rev_filtered",
                    join_pairs=[["region", "region"]],
                )],
            })
            await engine.storage.save_model(orders)

            # Outer query passes a runtime value for {threshold}; the join
            # target's rendered SQL must use 999, not the saved 0.
            outer = SlayerQuery(
                source_model="orders",
                dimensions=["region", "rev_filtered.amount_sum"],
                measures=[{"formula": "*:count"}],
                variables={"threshold": 999},
            )
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            assert "999" in resp.sql, (
                f"join target should use runtime threshold=999, got SQL:\n{resp.sql}"
            )
        finally:
            tmp.cleanup()

    async def test_join_target_resolving_set_is_per_context(self) -> None:
        """The recursion guard set must be isolated per asyncio task / request,
        not shared on the engine instance. Two tasks each push a unique name
        into the set and assert they only see their own.
        """
        import asyncio

        engine, tmp = await _engine_with_orders()
        try:
            both_started = asyncio.Event()
            checked = asyncio.Event()

            async def task(my_name: str, started: asyncio.Event) -> set:
                s = engine._get_join_target_resolving()
                s.add(my_name)
                started.set()
                # Wait until both tasks have populated their own sets, then
                # observe — if the set were instance-shared, each task would
                # see both names.
                await both_started.wait()
                snapshot = set(engine._get_join_target_resolving())
                checked.set()
                return snapshot

            e1 = asyncio.Event()
            e2 = asyncio.Event()

            async def gate():
                await asyncio.gather(e1.wait(), e2.wait())
                both_started.set()

            t1 = asyncio.create_task(task("alpha", e1))
            t2 = asyncio.create_task(task("beta", e2))
            await gate()
            s1, s2 = await asyncio.gather(t1, t2)
            assert s1 == {"alpha"}, f"task 1 leaked sibling state: {s1}"
            assert s2 == {"beta"}, f"task 2 leaked sibling state: {s2}"
        finally:
            tmp.cleanup()

    async def test_join_target_is_query_backed_model(self) -> None:
        from slayer.core.models import ModelJoin

        engine, tmp = await _engine_with_orders()
        try:
            # Create a saved query-backed "rollup" model joinable from orders.
            await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                ),
                name="rev_by_region",
            )
            # Add a join from orders → rev_by_region on region.
            orders = await engine.storage.get_model("orders")
            orders = orders.model_copy(update={
                "joins": [ModelJoin(
                    target_model="rev_by_region",
                    join_pairs=[["region", "region"]],
                )],
            })
            await engine.storage.save_model(orders)

            outer = SlayerQuery(
                source_model="orders",
                dimensions=["region", "rev_by_region.amount_sum"],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(outer, dry_run=True)
            assert resp.sql is not None
            # The join target should resolve to a sub-query containing the
            # rollup SQL (not raise about missing sql_table).
            assert "amount_sum" in resp.sql.lower()
        finally:
            tmp.cleanup()


class TestRunByNamePlanFlags:
    """``engine.execute(str, ...)`` must honor caller-supplied dry_run / explain
    so REST/MCP/CLI run-by-name doesn't silently execute when plan-only was asked.
    """

    async def test_dry_run_kwarg_returns_sql_without_executing(self) -> None:
        """Caller passes dry_run=True on a stored stage that has dry_run=False."""
        saved = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
                # NOTE: dry_run NOT set on the stage; only the caller asks.
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            # Track whether a SQL execute was attempted.
            execute_calls = 0
            from slayer.sql.client import SlayerSQLClient
            real_execute = SlayerSQLClient.execute

            async def counting_execute(self, *a, **kw):
                nonlocal execute_calls
                execute_calls += 1
                return await real_execute(self, *a, **kw)

            SlayerSQLClient.execute = counting_execute  # type: ignore[method-assign]
            try:
                resp = await engine.execute("rev_by_region", dry_run=True)
            finally:
                SlayerSQLClient.execute = real_execute  # type: ignore[method-assign]
            assert resp.sql is not None
            assert "amount" in resp.sql.lower()
            assert execute_calls == 0, "dry_run=True must not execute SQL"
        finally:
            tmp.cleanup()

    async def test_explain_kwarg_routes_through_explain_builder(self) -> None:
        """Caller passes explain=True; engine should invoke the EXPLAIN-SQL
        builder rather than executing the raw query.
        """
        saved = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            import slayer.engine.query_engine as qe
            real_explain = qe._build_explain_sql
            calls: list = []

            def tracking_explain(*, dialect, sql):
                calls.append(sql)
                return real_explain(dialect=dialect, sql=sql)

            qe._build_explain_sql = tracking_explain  # type: ignore[assignment]
            try:
                # Don't care about the actual EXPLAIN output (no table created);
                # we just want to confirm the explain path was reached.
                with pytest.raises(Exception):  # noqa: BLE001 — DB error is fine
                    await engine.execute("rev_by_region", explain=True)
            finally:
                qe._build_explain_sql = real_explain  # type: ignore[assignment]
            assert calls, "explain=True must route through _build_explain_sql"
        finally:
            tmp.cleanup()


class TestSiblingStageJoins:
    """Non-final stages in source_queries can join prior named sibling stages.

    Regression coverage for DEV-1340. The final stage already supported
    ``joins.target_model`` referencing a named sibling (see
    ``test_model_extension_over_named_query_stage_adds_columns`` and the
    "Query lists" docs); this class extends that contract to non-final
    stages, plus pins clear errors for forward and self references.
    """

    @staticmethod
    async def _engine() -> tuple:
        """Storage with orders (id, customer_id, region, amount) +
        customers (id, name)."""
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders_t",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="customers",
            sql_table="customers_t",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        ))
        return SlayerQueryEngine(storage=storage), tmp

    @staticmethod
    def _three_stage_dag() -> list:
        """3-stage DAG used by both the lookup-fix smoke test and the
        idempotency test. Stage 1 ``kpis`` aggregates orders per customer;
        stage 2 ``tagged`` (non-final) joins back to customers; stage 3
        re-aggregates ``tagged``.
        """
        return [
            SlayerQuery(
                name="kpis",
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["customer_id"],
            ),
            SlayerQuery.model_validate({
                "name": "tagged",
                "source_model": {
                    "source_name": "customers",
                    "joins": [{"target_model": "kpis", "join_pairs": [["id", "customer_id"]]}],
                },
                "dimensions": ["name"],
                "measures": [{"formula": "kpis.amount_sum:sum"}],
            }),
            SlayerQuery.model_validate({
                "source_model": "tagged",
                "dimensions": ["name"],
            }),
        ]

    async def test_non_final_named_stage_joins_prior_named_sibling(self) -> None:
        """Stage 2 (named ``tagged``) joins prior named ``kpis``; stage 3 uses ``tagged``."""
        engine, tmp = await self._engine()
        try:
            resp = await engine.execute(self._three_stage_dag(), dry_run=True)
            assert resp.sql is not None
            sql_lower = resp.sql.lower()
            # Both inner stages must show up in the rendered SQL.
            assert "orders_t" in sql_lower, resp.sql
            assert "customers_t" in sql_lower, resp.sql
            assert "amount_sum" in sql_lower, resp.sql
        finally:
            tmp.cleanup()

    async def test_final_stage_joins_two_prior_named_siblings(self) -> None:
        """Issue's "3-stage definition where stage 3 joins stage 1 and stage 2"."""
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery(
                    name="kpis",
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["customer_id"],
                ),
                SlayerQuery(
                    name="counts",
                    source_model="orders",
                    measures=[{"formula": "*:count"}],
                    dimensions=["customer_id"],
                ),
                SlayerQuery.model_validate({
                    "source_model": {
                        "source_name": "customers",
                        "joins": [
                            {"target_model": "kpis",   "join_pairs": [["id", "customer_id"]]},
                            {"target_model": "counts", "join_pairs": [["id", "customer_id"]]},
                        ],
                    },
                    "dimensions": ["name"],
                    "measures": [
                        {"formula": "kpis.amount_sum:sum"},
                        {"formula": "counts._count:sum"},
                    ],
                }),
            ]
            resp = await engine.execute(queries, dry_run=True)
            assert resp.sql is not None
            sql_lower = resp.sql.lower()
            assert "amount_sum" in sql_lower, resp.sql
            assert "_count" in sql_lower, resp.sql
            assert "customers_t" in sql_lower, resp.sql
        finally:
            tmp.cleanup()

    async def test_runtime_list_dag_auto_sorted(self) -> None:
        """Caller submits stages in non-topological order — engine auto-sorts.

        Stage ``a`` references stage ``b`` via ``joins.target_model``,
        but ``b`` is defined later in the input list. Pre-auto-sort this
        was rejected as a forward reference; now the engine reorders to
        ``[b, a, final]`` and emits valid SQL. The last entry of the
        input stays last (DAG root).
        """
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery.model_validate({
                    "name": "a",
                    "source_model": {
                        "source_name": "customers",
                        "joins": [{"target_model": "b", "join_pairs": [["id", "customer_id"]]}],
                    },
                    "dimensions": ["name"],
                    "measures": [{"formula": "b._count:sum"}],
                }),
                SlayerQuery(
                    name="b",
                    source_model="orders",
                    measures=[{"formula": "*:count"}],
                    dimensions=["customer_id"],
                ),
                SlayerQuery.model_validate({
                    "source_model": "a",
                    "dimensions": ["name"],
                }),
            ]
            # Auto-sort succeeds where strict-order would have rejected
            # this as a forward reference. Both inner stages surface in
            # the emitted SQL.
            resp = await engine.execute(queries, dry_run=True)
            assert resp.sql is not None
            sql_lower = resp.sql.lower()
            assert "orders_t" in sql_lower, resp.sql
            assert "customers_t" in sql_lower, resp.sql
            assert "amount_sum" in sql_lower or "_count" in sql_lower, resp.sql
        finally:
            tmp.cleanup()

    async def test_cycle_in_runtime_list_raises(self) -> None:
        """Mutual reference between two stages must raise with a clear
        cycle error naming the cycle members.
        """
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery.model_validate({
                    "name": "a",
                    "source_model": {
                        "source_name": "customers",
                        "joins": [{"target_model": "b", "join_pairs": [["id", "id"]]}],
                    },
                    "dimensions": ["name"],
                }),
                SlayerQuery.model_validate({
                    "name": "b",
                    "source_model": {
                        "source_name": "customers",
                        "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
                    },
                    "dimensions": ["name"],
                }),
                SlayerQuery.model_validate({
                    "source_model": "a",
                    "dimensions": ["name"],
                }),
            ]
            with pytest.raises(ValueError, match=r"[Cc]ycle"):
                await engine.execute(queries, dry_run=True)
        finally:
            tmp.cleanup()

    async def test_unused_utility_stage_is_kept(self) -> None:
        """A named utility stage that nothing references is allowed.

        The engine accepts the extra stage (no error) — it's effectively
        dead code in the emitted SQL since nothing resolves it, but
        validation passes so the caller can keep a library of utility
        sub-queries alongside the primary one.
        """
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery(
                    name="utility",  # never referenced
                    source_model="orders",
                    measures=[{"formula": "amount:max"}],
                ),
                SlayerQuery(
                    name="kpis",
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["customer_id"],
                ),
                SlayerQuery.model_validate({
                    "source_model": "kpis",
                    "dimensions": ["customer_id"],
                }),
            ]
            resp = await engine.execute(queries, dry_run=True)
            assert resp.sql is not None
            # The reachable stage (kpis) must surface.
            assert "amount" in resp.sql.lower()
        finally:
            tmp.cleanup()

    async def test_root_referenced_by_sibling_raises(self) -> None:
        """A non-final stage referencing the root must raise — root must
        be the dependency sink, not a CTE that other stages depend on.
        """
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery.model_validate({
                    "name": "util",
                    "source_model": "primary",  # references the root by name
                    "dimensions": ["customer_id"],
                }),
                SlayerQuery(
                    name="primary",  # the root
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["customer_id"],
                ),
            ]
            with pytest.raises(ValueError, match=r"DAG root|must not be referenced"):
                await engine.execute(queries, dry_run=True)
        finally:
            tmp.cleanup()

    async def test_empty_query_list_raises_validation_error(self) -> None:
        """Empty list must surface a clean ValueError, not IndexError."""
        engine, tmp = await self._engine()
        try:
            with pytest.raises(ValueError, match=r"non-empty list"):
                await engine.execute([], dry_run=True)
        finally:
            tmp.cleanup()

    async def test_unnamed_non_final_stage_raises(self) -> None:
        """A non-final entry without a ``name`` is unreachable by design
        (siblings reference each other by name) and must raise clearly.
        """
        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery(  # no name on a non-final entry
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                ),
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                ),
            ]
            with pytest.raises(ValueError, match=r"must have a 'name'"):
                await engine.execute(queries, dry_run=True)
        finally:
            tmp.cleanup()

    async def test_named_stage_self_ref_raises_clear_error(self) -> None:
        """A stage referencing its own name must raise a clear error."""
        import re

        engine, tmp = await self._engine()
        try:
            queries = [
                SlayerQuery.model_validate({
                    "name": "a",
                    "source_model": {
                        "source_name": "customers",
                        "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
                    },
                    "dimensions": ["name"],
                }),
                SlayerQuery.model_validate({
                    "source_model": "a",
                    "dimensions": ["name"],
                }),
            ]
            with pytest.raises(ValueError) as excinfo:
                await engine.execute(queries, dry_run=True)
            msg = str(excinfo.value)
            assert "'a'" in msg, msg
            assert re.search(r"self|itself|prior", msg, re.IGNORECASE), msg
        finally:
            tmp.cleanup()

    async def test_non_final_stage_with_sibling_join_renders_stable_sql(self) -> None:
        """Re-running a non-final stage that joins a prior sibling produces
        the same SQL twice — pinning the engine's recursion guard so the
        sibling-aware codepath is idempotent. This is the regression
        coverage for the ContextVar-based forbidden-refs frame restoring
        cleanly between calls.
        """
        engine, tmp = await self._engine()
        try:
            queries = self._three_stage_dag()
            r1 = await engine.execute(queries, dry_run=True)
            r2 = await engine.execute(queries, dry_run=True)
            assert r1.sql is not None
            assert r1.sql == r2.sql, "SQL must be stable across calls"
        finally:
            tmp.cleanup()


class TestMultiStageMeasureRename:
    """DEV-1335: when an inner stage names an aggregated measure via
    ``{"formula": "col:agg", "name": "X"}``, the inner stage's emitted column
    must surface as ``X`` — not the canonical ``col_agg``. Outer stages
    referencing ``X`` must resolve cleanly. Stage-shape edits via the save
    path must produce a cache that reflects the new names.
    """

    async def test_inner_stage_aggregated_measure_honors_user_name(self) -> None:
        """Stage 1 measure ``{formula: 'amount:sum', name: 'rev'}`` must surface
        as ``rev`` on the virtual model, not ``amount_sum``. Stage 2 must be
        able to reference ``rev`` and the cached SQL must reflect the rename.
        """
        saved = SlayerModel(
            name="renamed_metric",
            data_source="ds",
            source_queries=[
                SlayerQuery(
                    name="raw",
                    source_model="orders",
                    dimensions=["region"],
                    measures=[{"formula": "amount:sum", "name": "rev"}],
                ),
                SlayerQuery(
                    source_model="raw",
                    measures=[{"formula": "rev:sum"}],
                ),
            ],
        )
        engine, tmp = await _engine_with_orders()
        try:
            # Save-time dry-run pipes through both stages. With the bug the
            # inner stage wraps `amount_sum` (canonical) and stage 2's
            # `rev:sum` raises during validation.
            await engine.save_model(saved)
            loaded = await engine.storage.get_model("renamed_metric")
            assert loaded is not None
            col_names = [c.name for c in loaded.columns]
            # Stage 2's outer measure is `rev:sum` → cache column `rev_sum`.
            assert "rev_sum" in col_names, (
                f"expected 'rev_sum' in cached columns, got: {col_names}"
            )
            sql = loaded.backing_query_sql or ""
            # Inner-stage wrap renames the column to ``rev``; DEV-1452 Stage B's
            # ``build_flat_rename_wrapper`` emits the alias quoted
            # (``AS "rev"``) — the legacy ``_query_as_model`` emitted it
            # unquoted (``AS rev``). Both forms satisfy the rename contract.
            import re
            assert re.search(r'\bAS\s+"?rev"?(?:\s|$)', sql), (
                f"expected inner-stage 'AS rev' rename in SQL:\n{sql}"
            )
            # The canonical name must not leak into the wrapped subquery's
            # exposed alias (quoted or unquoted).
            assert not re.search(r'\bAS\s+"?amount_sum"?(?:\s|$)', sql), (
                f"canonical 'amount_sum' must not be the surfaced inner alias:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_inner_stage_aggregated_measure_default_when_name_omitted(self) -> None:
        """Regression guard: when ``qfield.name`` is None, the canonical
        ``col_agg`` naming is preserved. Existing call sites rely on this.
        """
        engine, tmp = await _engine_with_orders()
        try:
            stage = SlayerQuery(
                source_model="orders",
                dimensions=["region"],
                measures=[{"formula": "amount:sum"}],  # no name
            )
            virtual = await engine._query_as_model(inner_query=stage)
            col_names = [c.name for c in virtual.columns]
            assert "amount_sum" in col_names, (
                f"unnamed measure must keep canonical 'amount_sum', got: {col_names}"
            )
        finally:
            tmp.cleanup()

    async def test_inner_stage_user_name_collides_with_canonical_is_one_column(self) -> None:
        """When the user picks a ``name`` that happens to equal the canonical
        form, the wrap must emit exactly one column (no duplicate-alias error).
        """
        engine, tmp = await _engine_with_orders()
        try:
            stage = SlayerQuery(
                source_model="orders",
                dimensions=["region"],
                measures=[{"formula": "amount:sum", "name": "amount_sum"}],
            )
            virtual = await engine._query_as_model(inner_query=stage)
            col_names = [c.name for c in virtual.columns]
            assert col_names.count("amount_sum") == 1, (
                f"name=canonical must not duplicate the column, got: {col_names}"
            )
        finally:
            tmp.cleanup()

    async def test_query_as_model_emits_user_alias_unit(self) -> None:
        """Direct unit test on ``_query_as_model``: the wrap subquery and the
        virtual model's ``columns`` must use the user-supplied ``name``.
        """
        engine, tmp = await _engine_with_orders()
        try:
            stage = SlayerQuery(
                source_model="orders",
                dimensions=["region"],
                measures=[{"formula": "amount:sum", "name": "rev"}],
            )
            virtual = await engine._query_as_model(inner_query=stage)
            col_names = [c.name for c in virtual.columns]
            assert "rev" in col_names, (
                f"user-supplied 'name' must surface as a virtual column, got: {col_names}"
            )
            assert "amount_sum" not in col_names, (
                f"canonical 'amount_sum' must be replaced when user supplies "
                f"'name', got: {col_names}"
            )
            import re
            assert re.search(r"\bAS\s+rev\b", virtual.sql), (
                f"wrapped SQL must rename to user alias 'rev':\n{virtual.sql}"
            )
        finally:
            tmp.cleanup()
