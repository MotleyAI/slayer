# Tasks

## 1. Tests first (TDD — all initially failing, mirroring the spec scenarios)

- [x] 1.1 Rewrite correctness tests: `gran(col)` in `dimensions` for all nine granularities (parametrized), case-insensitivity, dotted join paths, append order after explicit TDs, canonical serialization, legacy-version input; verify each maps to the spec scenario and fails before implementation — `tests/test_dev1883_functional_rewrite.py`
- [x] 1.2 Equivalence tests: functional vs explicit `TimeDimension` — identical model_dump, identical generated SQL, identical rows on a SQLite execution; plus transform-axis/`main_time_dimension`/`whole_periods_only` SQL parity — `tests/test_dev1883_equivalence.py`
- [x] 1.3 Error-surface tests: wrong-shape granularity calls (`month()`, `month(a,b)`, `month(upper(x))`, `month(*)`, kwargs), typo `mnth(created_at)` naming granularities + `partition_by=` hedge, `time_dimensions=["created_at"]` remedy error, `GranularityCallError` typed class; non-regression: `upper(region)` computed dim, `partition_by=` aggregate dim, bare `sum(price)` keeps its binding error, `month(x)` in measures/filters unchanged — `tests/test_dev1883_errors.py`
- [x] 1.4 Order-key tests: order by `"month(created_at)"` with matching projected TD sorts by bucket; missing TD and granularity mismatch produce remedy-naming errors — `tests/test_dev1883_order_keys.py`
- [x] 1.5 Collision tests: `month(created_at)` + `year(created_at)` yield `.month`/`.year` suffixed keys with correct values (SQL + execution); single TD keeps unsuffixed key; exact duplicates dedupe (incl. identical non-default metadata); same column+granularity differing date_range/label rejected — `tests/test_dev1883_collision_keys.py`
- [x] 1.6 Reserved-name test: custom aggregation named like a granularity (any case) rejected with the reserved set named — `tests/test_dev1883_reserved_agg_names.py`
- [x] 1.7 Surface tests: MCP `query` tool, REST `QueryRequest`, and stored-query documents accept string `time_dimensions` entries; MCP tool docs name all nine granularities and the functional form for both `dimensions` and `time_dimensions` — `tests/test_dev1883_surfaces.py`

## 2. Core implementation

- [ ] 2.1 `slayer/core/errors.py`: typed error class for granularity-call construction failures; verify 1.3 error-class assertions pass
- [ ] 2.2 `slayer/core/query.py`: parse-based classification helper + single before-validator (migrations then rewrite, explicitly sequenced) + `time_dimensions` string coercion + exact-duplicate dedupe + same-column+granularity metadata-conflict rejection; verify 1.1–1.3 and 1.5 construction tests pass
- [ ] 2.3 `slayer/core/models.py`: add granularity values to reserved aggregation names; verify 1.6 passes
- [ ] 2.4 Order-key resolution for granularity-shaped entries against projected TDs; verify 1.4 passes
- [ ] 2.5 `slayer/engine/stage_planner.py`: granularity-suffixed public names for same-column TD collisions; verify 1.5 passes
- [ ] 2.6 `slayer/mcp/server.py` + `slayer/api/server.py`: widen `time_dimensions` annotations to accept strings; complete the MCP granularity list and advertise the functional form in the `query` tool arg docs; verify 1.7 passes

## 3. Docs

- [ ] 3.1 One concise sentence each in `docs/concepts/queries.md`, `slayer/memories/help_content/01_queries.md`, `slayer/memories/help_content/05_time.md`, `.claude/skills/slayer-query.md` (checking the help-seed test still passes); verify by grepping for the functional form in each file

## 4. Verification

- [ ] 4.1 Full non-integration suite green: `poetry run pytest -m "not integration"`
- [ ] 4.2 Lint + conventions + architecture: `poetry run ruff check slayer/ tests/`, `poetry run lint-imports`, `poetry run python tools/arch_check.py`, `poetry run basedpyright`
