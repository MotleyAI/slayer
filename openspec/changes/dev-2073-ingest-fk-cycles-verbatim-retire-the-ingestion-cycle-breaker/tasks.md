## 1. Failing test suite (pr-tests)

- [x] 1.1 Ingestion unit + SQLite/DuckDB execution tests for `models/fk-ingestion`: 2-cycle, 3-cycle, latest-child pointer (right customer per order), unrelated chains intact; verify they fail on the current code
- [x] 1.2 Tests for one edge per FK relationship: mutual-PK FKs (both models ingest, one edge, re-ingest clean), FK stored in reverse orientation (named and unnamed) not re-added
- [x] 1.3 Naming tests: billing/shipping addressable with edge-name result keys, 2-cycle names `coupon_usage`/`transaction`, singleton unnamed, hand-authored member of a FK parallel set named, purely hand-authored parallel set untouched, user-set name kept; collisions with a model name, an incident edge, a sibling's stem; composite stem; identical names across scan orders
- [x] 1.4 Re-ingest tests: reverse FK appears (stored edge on the other model named + saved, report lists `named_joins`), second FK to an existing target (new column merged too, no `IngestionError`), idempotent rerun, fault-injection parametrised over the failing save index (rerun equals uninterrupted run)
- [x] 1.5 Tests: `get_foreign_keys` failure on one table (table ingested, no joins, not skipped); new table named like a stored edge is skipped with the edge named in the reason; `save_model` of a model named like a stored edge is rejected
- [x] 1.6 `edit_model` tests (MCP + engine `edit_model_remove`): remove one of two named parallel edges, ambiguous target reference error listing candidates, name an unnamed parallel edge by (target, pairs) upsert, `join_edges` exact removal, unnamed second join rejected, legacy sole-join pairs edit unchanged
- [x] 1.7 Schema-drift tests: dropping `billing_address_id` keeps the shipping join; non-parallel drop reports the target as before; `RemoveSpec.join_edges` JSON replayed through MCP `edit_model` and `apply_drift_deletes` removes exactly the reported joins
- [x] 1.8 Surface tests with named parallel edges: inspect shows names, `models_summary` lists addressable tokens and marks ambiguous pairs, facade catalog exposes both orientations with names, translator matches a reverse-direction SQL JOIN to the declared edge
- [x] 1.9 Pins with a terse pointer to DEV-2074: rootless `{x.v, y.v, z.v}` over `x→y→z→x` fails with the no-viable-candidate error and succeeds with `source_model`; from `x`, `sum(z.amount)` binds the direct fanning edge
- [x] 1.10 Remove or retarget tests of deleted internals (`tests/test_ingestion_cardinality.py:146` `_build_fk_graph`, `tests/test_ingestion_name_sanitize.py:557`, `tests/test_ingestion_clickhouse.py:193` `_introspect_query_columns_via_inspector`) — mechanical only; verify the suite collects

## 2. Delete the cycle machinery

- [x] 2.1 Delete `RollupGraphError`, `_get_fk_relationships`, `_build_fk_graph`, `_check_acyclic`, `_compute_transitive_closure`, `_collect_fk_columns` and the `has_cycles`/`fk_graph`/`referenced_tables` plumbing; reduce `_introspect_query_columns_via_inspector` to the table's own columns and drop the `"."` skip in `_columns_to_model`; verify 1.1 passes
- [x] 2.2 Make `_get_fk_constraint_groups` tolerate `get_foreign_keys` failure (`[]` + debug log); verify 1.5's reflection-failure test passes

## 3. Identity concepts and storage

- [x] 3.1 Add the relationship signature and `JoinEdgeRef` (+ edge-reference string, resolver failing on >1 match) in one shared module; unit-test both
- [x] 3.2 Make `save_model` reject a model named like a stored edge in its datasource; verify 1.5's save test passes

## 4. Ingestion planning pass

- [x] 4.1 Implement the datasource-wide planning pass (inverse survivor, reconciliation by relationship signature, edge-name-collision skip) used by first ingest and re-ingest; remove `_merge_joins_strict`'s raise; verify 1.2 and 1.4 (non-naming parts) pass
- [x] 4.2 Implement the naming algorithm (design D3) incl. hand-authored members of FK parallel sets and reserved user names; verify 1.3 passes
- [x] 4.3 Apply name fills to stored models of other tables, save name-only updates first, add `ModelAddition.named_joins` and report joins by edge reference (MCP + CLI rendering); verify 1.4 incl. fault injection passes

## 5. Mutation surfaces

- [x] 5.1 MCP `edit_model` upsert matching (name → (target, pairs) → sole join) and removal by edge reference / `join_edges` (`VALID_REMOVE_KEYS`, widened `remove` type, descriptions within the tool-description budget); engine `edit_model_remove(remove_join_edges=…)`; verify 1.6 and the MCP tool-surface budget tests pass
- [x] 5.2 Schema drift keyed by `JoinEdgeRef`; `RemoveSpec.joins` edge references + additive `RemoveSpec.join_edges`; `DeleteReason.target` `join:<ref>`; `apply_drift_deletes` forwards `join_edges`; verify 1.7 passes

## 6. Inspection and facade

- [x] 6.1 `inspect` joins `name` column, names-only/skeleton edge references, `models_summary._join_targets` addressable tokens + ambiguity marker; verify 1.8 inspect/summary tests pass
- [x] 6.2 `FacadeJoin.name`, `FacadeTable.joins` from `neighbors()`, translator matching on oriented pairs with the canonical token; verify 1.8 facade tests pass

## 7. Docs

- [x] 7.1 Fix `docs/examples/05_joins/joins.md:7`, `docs/concepts/ingestion.md:23` and `:240-242`, `docs/examples/03_auto_ingest/auto_ingest.md:17`, `docs/concepts/terminology.md:62-64`; add one sentence on parallel-edge naming and one on literal-first binding; grep docs for `RollupGraphError`/`acyclic`/`transitive closure` returns nothing stale
- [x] 7.2 Rewrite `docs/examples/03_auto_ingest/auto_ingest_nb.ipynb` and `docs/examples/05_joins/joins_nb.ipynb` without `_build_fk_graph`/`_compute_transitive_closure`; re-execute both with `jupyter nbconvert --to notebook --execute --inplace` and verify they run clean

## 8. Gates and hand-off (pr-implement)

- [x] 8.1 Full unit suite, the CI integration invocation, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline) and `la-arch-check` all green
- [x] 8.2 Comment on DEV-2074 recording the deferred items (self-referencing FKs, literal-first/route rules, facade diagnostic for hand-authored unaddressable pairs) and the D/E pins it owns
- [x] 8.3 PR description says `Fixes #477` and credits the reporter; draft a closing comment for PR #479 (thanks, why back-edge dropping was not taken — table-name-order choice, latest-child silently re-keyed — link to the PR) and get the user's OK on the exact text before posting or closing anything

## 9. Reserved-word aliases (added in pr-implement with user approval)

- [x] 9.1 Per-dialect reserved-alias sets in `slayer/sql/reserved_keywords.py`, live alias probes in the SQLite/DuckDB unit tests and each engine's integration suite (Postgres, MySQL, ClickHouse, SQL Server, Trino, Snowflake, BigQuery)
