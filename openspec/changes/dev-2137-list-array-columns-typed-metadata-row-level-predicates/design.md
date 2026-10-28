## Context

See proposal.md for motivation. Current state (branch head after merging DEV-2120 and `origin/main`):

- **`DataType`** (`slayer/core/enums.py`) has no list member. `Column` (`slayer/core/models.py`), `StageColumn` (`slayer/core/scope.py`), `ValueSlot` (`slayer/ir/planned.py`) and aggregate result inference (`slayer/engine/key_metadata.py`) carry a scalar `type` only.
- **Ingestion** (`_sa_type_to_data_type`, `slayer/engine/ingestion.py`) maps `sa.ARRAY` to `TEXT` through the unmapped fallback. BigQuery `REPEATED` columns are stored as `TEXT` with `db_type` None. Snowflake `ARRAY` is stored as `TEXT` with `db_type` `"ARRAY"` (a JSON-family input for `json_value`). The DuckDB INFORMATION_SCHEMA fallback (`_info_schema_type`) reads `INTEGER[]` as `INT`.
- **To-many mechanisms correlate through `join_pairs`:**
  - the hop join (`_hop_join` in `slayer/sql/generator.py`; empty pairs render as `CROSS`);
  - the semi-join EXISTS (`_SemiJoinOps`, which indexes `eqs[0]`);
  - cross-model producers (`_synthesize_cross_model_producer` in `slayer/engine/compile/stages.py`, re-rooted with `bundle.rerooted`);
  - association, which requires a unique key on its root (`check_association_root_unique_key`).
- **Eligibility:** type defaults come from `DEFAULT_AGGREGATIONS_BY_TYPE`. Custom aggregations bypass type gates, and expression sources skip the per-column gates.
- **Facades** use exhaustive type maps (`slayer/facade/datatypes.py`, `slayer/pg_facade/types.py`, `slayer/flight/types.py`). The catalog drops any model with an unmapped column type.
- **Schema drift** (`slayer/engine/schema_drift.py`) would read stored `TEXT` vs live `ARRAY` as a type-bucket mismatch and propose deleting the column.

Applicable principles:
- semantics Axioms 1, 2, 3 (3.2 new), 4, 8, 9, 12 and 14;
- system P6, P10, P13, P14 (new exception) and P16;
- engine P2, P9 and P10;
- sql P1, P2, P5, P9, P10 and P12;
- core P3.

Arc42 edits are approved and applied: Axiom 3.2 (`[target: DEV-2137]`, flipped to `[enforced: test:tests/test_dev2137_list_children.py]` in this change) and the P14 exception. No c4 change is expected; `la-arch-check` is the gate.

## Goals / Non-Goals

**Goals:**
- List columns typed end to end.
- One portable meaning for the list functions and `collect`.
- Element access that reduces to the existing one-to-many semantics and needs no key.
- Stored models re-typed through the existing live-schema doors.
- Agents can discover all of it from the MCP surface.

**Non-Goals:**
- Lists of records (BigQuery `REPEATED RECORD`), nested lists, and list-valued dimensions.
- JSON-array emulation on SQLite, MySQL and SQL Server.
- Native array types on the facade wires.
- An element-position column.

## Decisions

### D1. The element type rides next to the type everywhere

`Column`, `StageColumn`, `ValueSlot`, bound key metadata and every type-inference function carry `element_type` beside `type`, in the same flat shape as the persisted `Column`. So a list typed anywhere stays typed through binding, staging, re-rooting and result metadata. `aggregation_result_type` returns the pair: `collect(x)` gives `(ARRAY, type of x)`, and `count` over a list gives `INT`. Validation: `element_type` is required iff the type is `ARRAY`, and is never `ARRAY` itself. No stored-model version bump: the field is optional and absent on scalars.

### D2. Elements are the non-NULL elements

One rule for every list construct. The emission of each construct filters NULL elements: the unnest relation, EXISTS forms, the non-NULL `length` count, the `contains_*` lowerings, and the input filter of `collect`. Literal NULL elements are rejected at plan time.

### D3. contains and length are overloaded by the checker's typing

The checker types the first argument; a list argument routes the call to the list lowering, while TEXT keeps the DEV-2120 path untouched.
- `contains_any` / `contains_all` are list-only library entries.
- List functions lower through a typed `ArrayFn` hook family on `SqlDialect`: the base form in the base class, and each Tier-1 file overriding its quirks (sql P1, P2).
- sqlglot's `ArrayOverlaps` / `ArrayContainsAll` are never used, because they emit `&&` / `@>` on non-Postgres dialects.
- A TEXT argument to a list-only function, or a list argument to a TEXT-only function, is a typed error naming the right function.

### D4. Array literals are a new key kind

The grammar admits a bracketed literal list as a primary expression. It binds to a new array-literal key (system P13: no existing kind has its shape and meaning), which implements `children()` / `map_children()` (core P3).
- Elements are non-NULL literals of one type family.
- `[]` takes its element type from the other operand and is rejected where nothing gives it one.
- A list-valued `{variable}` binds to the same key.
- Positions other than list arguments reject it through the checker's list-type rules.

### D5. The list child is a keyless lateral edge kind

The join walker exposes each list column `L` of `M` as an oriented hop `L` of a new lateral kind: one-to-many forward, and to-one back, by construction. Home, attributability, population and filter disposition read only cardinalities, so the planner is unchanged (reduction to Axiom 3.2). Only the places that build correlations from `join_pairs` gain a lateral branch, and it fails closed on any edge kind it does not handle:
- **Hop in FROM:** the lateral unnest of `M.L` into the alias the walker assigns, with NULL elements filtered.
- **EXISTS:** a correlated unnest subquery over the outer `M` row. ClickHouse, which lacks correlated subqueries, lowers the element predicate to `arrayExists(e -> …, M.L)`, and Trino, which rejects a correlated subquery over `UNNEST`, to `any_match(M.L, e -> …)`.
- **Producers rooted at the child:** `FROM M <lateral unnest>`. The hop back to `M` is the same row, so no join is emitted. The producer attaches back on its grain as any producer does.
- **Per-dialect relation:**

  | Dialect | Unnest form |
  | --- | --- |
  | BigQuery, Trino, DuckDB | `CROSS JOIN UNNEST` |
  | Postgres | `CROSS JOIN LATERAL unnest()` |
  | Snowflake | `LATERAL FLATTEN`, plus a cast of `value` to the element type |
  | ClickHouse | `ARRAY JOIN` |

  It is built by one dialect hook that returns AST (sql P1).

The element column is spelled `L.value`. A list column with `UNKNOWN` elements has no element access (typed error).

### D6. Keyless association across list hops

When every unattributable dimension of a `M`-homed aggregate is reached through list hops of `M`'s own rows, `associate` renders each `M` row once per cell. It does so as `M` × the distinct non-NULL elements of each list hop, with no join, so identical `M` rows stay distinct rows. Otherwise the existing unique-key requirement and kernel apply unchanged.

### D7. collect is a dialect-hook aggregation

`collect` joins `BUILTIN_AGGREGATIONS` with a new list value class, and its `AGG_REGISTRY` entry is a `dialect_hook`, `build_collect`. Every lowering filters NULL inputs explicitly:

| Dialect | Lowering |
| --- | --- |
| Postgres | `array_agg(DISTINCT x ORDER BY x) FILTER (WHERE x IS NOT NULL)` |
| DuckDB | `list(DISTINCT x ORDER BY x) FILTER (WHERE x IS NOT NULL)` |
| BigQuery | `ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x)` |
| Snowflake | `ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x)`, which skips NULLs |
| ClickHouse | `arraySort(groupUniqArray(x))`, which skips NULLs |
| Trino | `array_sort(array_agg(DISTINCT x) FILTER (WHERE x IS NOT NULL))` |

- The empty value is the empty list (Axiom 4): a cell with no rows, or whose inputs are all NULL, takes `[]` on every
  database. Postgres, DuckDB, BigQuery and Trino wrap the aggregate in `COALESCE(…, <empty list of the input type>)`;
  Snowflake and ClickHouse already return `[]`.
- It is eligible by default on the six scalar types, and the name is reserved like every built-in.

### D8. One list-type gate on every aggregation node

The checker inspects every aggregation node's typed source. A list-valued source admits only `count`. Built-ins, `collect` and custom aggregations over it are refused, each error naming the remedy (`count_distinct(L.value)`, `collect(L.value)`). This closes the custom-aggregation and expression-source bypasses.

### D9. List values cross one JSON boundary

- **REST / MCP:** driver array values are normalised to Python lists at result fetch, so REST and MCP JSON carry real lists, and CSV / markdown cells carry the JSON text.
- **Facades:** catalogs map `ARRAY` to the text type. One shared serialiser in `slayer/facade` turns list values into JSON array text for the pg wire, Flight and the JDBC path: strings escaped, booleans as `true` / `false`, dates and timestamps as ISO strings, NULL as SQL NULL.

### D10. Ingestion returns the element type

- **Type mapping:** the SQLAlchemy type mapping returns `(type, element_type)`. It recognises `sa.ARRAY` and each dialect's array class, unwraps ClickHouse wrappers inside the item type, and gives nested lists `UNKNOWN`. `db_type` keeps the raw string.
- **DuckDB:** the INFORMATION_SCHEMA fallback parses `T[]` / `LIST(T)`.
- **Snowflake:** untyped `ARRAY` columns get one inference query: distinct `TYPEOF` over `LATERAL FLATTEN`, limited.
- **Fallback warning:** it stops claiming arrays work as TEXT.

### D11. Stored lists are re-typed through the live-schema doors

- **Re-ingest:** the additive merge re-types a stored `TEXT` column whose live column is a list (P14 exception). Every other attribute is kept, and a stored `ARRAY`'s `element_type` is kept except a Snowflake `UNKNOWN`.
- **Schema drift:** a `TEXT`-vs-live-list pair is a new re-type finding kind in validation results and in the `SchemaDriftError` payload, never a bucket conflict. The cursor-based live typing of `sql` models recognises list values, so declared `ARRAY` columns match.

### D12. Name collisions are checked at save, from both sides

Save validation in `slayer/storage/base.py` computes the hop tokens reachable from a model: edge names, and the names of the models its incident edges reach. A list column sharing one is an error, both when saving the model with the list column and when saving a model whose join creates the token.

### D13. Sampling goes through the child

Profiling builds its categorical and min/max queries over `L.value` (with `count(L.*)` for frequency). Caching, failure bounding and logging therefore stay the one path. `UNKNOWN` elements are skipped.

### D14. The MCP surface teaches list columns

- `inspect`, `search` and `models_summary` render `ARRAY<T> (list)` with element samples and a one-line operation hint.
- The `filters` and `to_many_handling` field descriptions gain one clause each, within `tests/test_mcp_surface_budget.py`.
- `help.queries` lists the functions.
- `help.joins` explains `L.value`: the EXISTS filter, the `associate` histogram, and `contains_all` vs a single-filter `AND`.
- Every list-type error names its remedy.

### D15. Dialects without native lists refuse at plan time

`SqlDialect` declares whether it supports lists (SQLite, MySQL and SQL Server do not). The checker raises one typed error naming the construct and the database for every list construct, the same way DEV-2120 gates refuse functions.

### D16. Comparison matrix row Q27

- `examples/comparisons/dataset.sql` gains `customers.tags VARCHAR[]` with a repeated tag, a NULL element, `[]` and a NULL list. The column is additive, so existing truths hold.
- `probes.yaml` gains Q27 probes on all four engines: has tag x, has both x and y, customers per tag without double counting, distinct tags per region next to a customer count, and the tag set per region.
- `matrix.yaml` gains row Q27 in *Query semantics*, with verdicts and caveats taken from the probe results.
- `matrix.json` is regenerated.
- The website sync is DEV-2121.

## Risks / Trade-offs

- **BigQuery stores a NULL list as `[]`** → `length` reads 0 there. Documented as a storage property.
- **ClickHouse lacks correlated subqueries** → element filters lower to `arrayExists` lambdas; the live ClickHouse suite verifies them.
- **Overloading `contains` / `length` on stale `TEXT` list columns** returns the substring meaning on Postgres / DuckDB until re-ingest → drift reports the re-type, and the release note asks users to re-ingest.
- **Threading `element_type` through every typed carrier is wide churn** → accepted for the end state. Multi-stage tests prove it survives.
- **MCP schema budget** → the field-description clauses stay within the budget test.
- **Snowflake inference reads data at ingest** → one limited query per untyped `ARRAY` column, the same pattern as the existing ingest probes.
