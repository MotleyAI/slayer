## 1. Tests (spec-tests stage)

- [x] 1.1 New `tests/test_drift_attribution_scope.py` covering every scenario in `specs/models/schema-drift/spec.md` (inline SQLite/DuckDB — unit, not `integration`); count calls on `_introspect_one_table`, the per-schema listing, and `_live_columns_for_sql_model`; inject the cache clock. Verify: the file collects and every new test fails for the right reason before implementation.
- [x] 1.2 Read-set ratchet test: render a corpus of query shapes (single-stage, multi-stage, query-backed splice, cross-model producer, filter semi-join, over-limit identifiers) and assert every model relation in the final AST carries the `slayer_model` stamp and stage relations do not. Verify: fails before 2.1.
- [x] 1.3 `tests/test_schema_drift_error.py` (user-approved): `test_validate_models_attribution_failure_re_raises_original` moves its patch target to the new attribution entry point, assertions unchanged; `test_healthy_query_does_not_call_validate_models` additionally patches the new entry point and asserts it is not called. Verify: both updated tests fail before implementation for the right reason.

## 2. Read set

- [x] 2.1 Stamp `meta["slayer_model"]` on the node `_emit_relation` returns (tables and embedded `sql` subqueries; not stage relations). Verify: 1.2 passes.
- [x] 2.2 Collect the read set from the final statement AST (post-pruning, pre-policy) plus surviving spliced query-backed model names; carry it as `_Prepared.touched`. Delete `_touched_models_for_plan`, `_collect_query_backed_base_names`, `_expand_join_graph`, `_load_join_graph_models` after an LSP references check. Verify: read-set scenarios in 1.1 pass.

## 3. Scoped validation

- [x] 3.1 `_collect_live_tables` / `_live_schema_for_datasource` accept an optional `wanted` set (unchanged enumeration/listing/keying); `validate_datasource` accepts the models to validate + `available_in_ds` + optional snapshot; scope `sql` trials and the SQLite probe. Verify: "Attribution inspects only read models" scenarios pass and existing `tests/test_validate_models.py` stays green.

## 4. Snapshot cache

- [x] 4.1 `LiveSnapshotCache` / `DriftSnapshot` in `schema_drift.py` per design D3 (TTL 60 s, per-key `asyncio.Lock`, LRU 256, absence rule, negative-cached `IntrospectionUnavailable`, injectable clock); one instance per `SlayerQueryEngine`. Verify: "reuses live-schema facts" scenarios pass.
- [x] 4.2 Unreachable datasource (review fix): a failed `sql` trial whose `SELECT 1` also fails raises the connection error (both paths); a connection failure marks the snapshot unreachable for the TTL, re-raised without requests. Verify: the two "unreachable datasource" scenarios pass and fail without the fix.
- [x] 4.3 Listed-but-unreadable objects give their models no verdict (both paths). Verify: the new scenario fails without the fix.

## 5. Attribution rewrite

- [x] 5.1 `_maybe_raise_schema_drift` validates `prepared.datasource` over the read set through the cache; `SchemaDriftError.models` = sorted distinct blamed model names; `invalid_sql` exclusion and swallow-and-re-raise unchanged; both call sites (data query, EXPLAIN). Verify: all of 1.1 and 1.3 pass, `tests/test_schema_drift_error.py` and `tests/test_api_server.py` green.

## 6. Docs and gates

- [x] 6.1 `docs/concepts/schema-drift.md` item 3: one sentence — attribution checks only the models the failed statement read, reuses live-schema facts for up to 60 s, and `models` lists the drifted models. Verify: docs diff is one sentence.
- [x] 6.2 Full unit suite (`poetry run pytest -m "not integration" -n auto`), `poetry run ruff check slayer/ tests/`, `uvx --no-build --from living-architecture==0.2.0 la-arch-check`, basedpyright baseline not grown. Verify: all green.
