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
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="status", sql="status", type=DataType.STRING),
            Column(name="region", sql="region", type=DataType.STRING),
            Column(name="amount", sql="amount", type=DataType.NUMBER),
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
                dry_run=True,
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_by_region")
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
                dry_run=True,
            )
            resp = await engine.execute(q, variables={"r": "EU"})
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
                dry_run=True,
            )
            resp = await engine.execute(q, variables={"r": "EU"})
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
                dry_run=True,
            )
            resp = await engine.execute(q, variables={"unrelated": 99})
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
                dry_run=True,
            )],
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered", variables={"r": "US"})
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
                dry_run=True,
            )],
            query_variables={"r": "DEFAULT_R"},
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered")
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
                dry_run=True,
            )],
            query_variables={"r": "DEFAULT_R"},
        )
        engine, tmp = await _engine_with_orders(saved)
        try:
            resp = await engine.execute("rev_filtered", variables={"r": "US"})
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
                columns=[Column(name="x", sql="x", type=DataType.STRING)],
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


class TestCacheRefreshOnExecute:
    async def test_cache_refreshed_when_stored_model_has_no_cache(self) -> None:
        """Saving a query-backed model directly to storage (bypassing the
        engine's save_model) leaves cache empty; an execute call refreshes it.
        """
        # Build a model with empty cache and write via raw storage save.
        empty = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
                dry_run=True,
            )],
        )
        engine, tmp = await _engine_with_orders()
        try:
            await engine.storage.save_model(empty)
            stored = await engine.storage.get_model("rev_by_region")
            assert stored is not None
            assert stored.columns == []
            assert stored.backing_query_sql is None

            await engine.execute("rev_by_region")
            refreshed = await engine.storage.get_model("rev_by_region")
            assert refreshed is not None
            assert refreshed.backing_query_sql is not None
            assert any(c.name == "region" for c in refreshed.columns)
        finally:
            tmp.cleanup()

    async def test_model_extension_over_query_backed_model_adds_columns(self) -> None:
        """ModelExtension wrapping a saved query-backed model adds extra columns
        to the resolved virtual model — exercised through enrichment.
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
                "dry_run": True,
            })
            resp = await engine.execute(outer)
            assert resp.sql is not None
            assert "is_high_rev" in resp.sql
            assert "amount_sum" in resp.sql
        finally:
            tmp.cleanup()

    async def test_model_extension_over_named_query_stage_adds_columns(self) -> None:
        """ModelExtension wrapping a named-query stage in a runtime list adds
        extra columns to the resolved virtual model.
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
                    "dry_run": True,
                }),
            ]
            resp = await engine.execute(queries)
            assert resp.sql is not None
            assert "doubled" in resp.sql
        finally:
            tmp.cleanup()

    async def test_no_write_when_cache_unchanged(self) -> None:
        """When the cache matches the freshly-resolved virtual, no storage
        write happens.
        """
        engine, tmp = await _engine_with_orders()
        try:
            await engine.create_model_from_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                    dry_run=True,
                ),
                name="rev_by_region",
            )
            # Capture initial state
            before = await engine.storage.get_model("rev_by_region")
            assert before is not None
            initial_sql = before.backing_query_sql

            # Spy on storage.save_model writes
            write_count = 0
            real_save = engine.storage.save_model

            async def counting_save(m):
                nonlocal write_count
                write_count += 1
                return await real_save(m)

            engine.storage.save_model = counting_save  # type: ignore[method-assign]
            await engine.execute("rev_by_region")
            assert write_count == 0, "no write expected when cache matches resolved state"
            after = await engine.storage.get_model("rev_by_region")
            assert after is not None
            assert after.backing_query_sql == initial_sql
        finally:
            tmp.cleanup()


class TestBackingQuerySQLCacheHygiene:
    """``backing_query_sql`` is the canonical placeholder-fill render and must
    not capture per-request runtime variables.
    """

    async def test_runtime_variables_do_not_leak_into_persisted_sql(
        self,
    ) -> None:
        # Use engine.save_model so the canonical cache is populated up-front;
        # then assert that a request with overriding variables doesn't churn it.
        engine, tmp = await _engine_with_orders()
        try:
            await engine.save_model(SlayerModel(
                name="rev_filtered",
                data_source="ds",
                source_queries=[SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    filters=["region = '{r}'"],
                    dry_run=True,
                )],
                query_variables={"r": "DEFAULT_R"},
            ))
            initial = await engine.storage.get_model("rev_filtered")
            initial_sql = initial.backing_query_sql
            # Save-time canonical render should already have the default value.
            assert initial_sql is not None
            assert "'DEFAULT_R'" in initial_sql

            # Execute with a runtime variable that overrides the default.
            await engine.execute("rev_filtered", variables={"r": "REQUEST_VAL"})

            # Reload — the persisted backing_query_sql must NOT contain the
            # request-specific value (would leak per-request data through
            # inspect/export and cause cache churn).
            after = await engine.storage.get_model("rev_filtered")
            assert after is not None
            assert "'REQUEST_VAL'" not in (after.backing_query_sql or "")
            assert after.backing_query_sql == initial_sql
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
                dry_run=True,
            )
            resp = await engine.execute(outer)
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
                "dry_run": True,
            })
            resp = await engine.execute(outer)
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
                    dry_run=True,
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
                dry_run=True,
            )
            resp = await engine.execute(outer)
            assert resp.sql is not None
            assert "999" in resp.sql, (
                f"join target should use runtime threshold=999, got SQL:\n{resp.sql}"
            )
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
                    dry_run=True,
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
                dry_run=True,
            )
            resp = await engine.execute(outer)
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
