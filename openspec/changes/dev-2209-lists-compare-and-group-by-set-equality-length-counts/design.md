## Context

See proposal.md › Why. Today every list rule is a refusal in the checker (`slayer/engine/elaborate_env.py`: `check_list_dimension`, `_check_list_comparison`, `_check_list_in`, `_check_list_aggregation`, `check_list_order_key`), driven from `bind_inputs.py`. Each of the six native-list dialects (Postgres, DuckDB, BigQuery, Snowflake, ClickHouse, Trino) implements `list_length` / `list_contains` / `list_overlaps` / `list_contains_all`; the base hooks refuse (system §3.10.2). `collect` already returns distinct sorted non-NULL lists on every dialect. Group keys are collected in `generator._build_base_select_for_planned` and several other GROUP BY sites, reusing the projected expression; grain join-backs go through `render/joins.build_null_safe_eq`.

Structural cause: the algebra has no equality for a whole list, yet Axiom 12's population needs one, so each equality-reading position refuses lists on its own. The fix gives a list one value for equality — its element set (semantics Axiom 3.2) — and routes every such position through one canonical form.

Element principles that apply: system §3.10.1/§3.10.2 (one meaning; per-dialect translations, base refuses), §3.13 (one key kind per meaning; dispatches fail closed), §3.17 (honest catalogs), §3.18 (one Mode B reader); core P3 (total traversal), P5 (structured warnings surfaced twice); engine P1 (typed pipeline), P4 (interning), P5 (stage schemas), P9 (checker owns typing); sql P1 (AST), P2 (quirks in `dialects/`), P5 (one ValueKey renderer), P7 (null-safe join-backs, one builder), P9 (fail closed). Specs attach to `models/list-columns`, `queries/list-functions`, `queries/expression-grammar` (cross-cutting `models` / `queries`) and `facade/projection`, `facade/list-values`.

## Goals / Non-Goals

**Goals:**
- One canonical element-set form, one canonicalisation pass, one dialect hook pair; every equality-reading position uses them.
- Lists group, compare, count distinctly and partition identically on all six native-list dialects.

**Non-Goals:**
- Ordering lists (`<`, `>`, `BETWEEN`, `ORDER BY`) — refused as before.
- Exposing a list's stored order beyond raw-rows projection.
- `collect` over a list (lists of lists), and min/max/custom aggregations over lists.
- Portable equality for opaque (`UNKNOWN`) elements — refused instead.
- Special-casing float NaN / signed zero / collation inside lists: lists take the database's own element equality and order, like scalar dimensions (semantics §4).

## Decisions

### D1 — `ListSetKey(source)` is a new key kind
The element set is the list's value in equality positions (Axiom 3.2), so it is a semantic value with its own identity: `ListSetKey(source)` in `core/keys.py`, implementing `children()` / `map_children()`, classified in `KIND_POLICY`, the reroot / prepend visitors, type inference (`ARRAY<element>`), phase inference and stage-schema inference; every kind dispatch fails closed on it, guarded by a key-kind exhaustiveness test. Alternative rejected: a reserved scalar-function name (`ScalarCallKey("__list_set")`) bends an existing kind's contract (§3.13); rendering-only metadata leaves identity ambiguous between raw and canonical uses of one list.

### D2 — One canonicalisation pass
After binding and coercion, before slot and grain construction, one idempotent pass rewrites every equality-reading occurrence of a list-valued key — a grouped dimension, a comparison / `IN` operand, a `count_distinct` source, a `partition_by=` member, a transform partition key — to `ListSetKey(L)`, and returns the complete updated bound query, so equal occurrences intern to one slot (engine P4). The checker functions stay pure validators. Canonicalisation follows only from an equality-reading position, never from list type alone: a raw-rows projection keeps `L`. `ListSetKey(ListSetKey(L))` collapses to `ListSetKey(L)`.

### D3 — Two dialect hooks: value and key
- `list_set(x, element)` → the canonical array, base refuses:
  - Postgres `CASE WHEN x IS NULL THEN NULL ELSE ARRAY(SELECT DISTINCT e FROM UNNEST(x) e WHERE e IS NOT NULL ORDER BY e) END`, the input cast to the element family's canonical array type before unnesting and both branches typed alike, so `VARCHAR[]` meets `TEXT[]` and integer widths meet;
  - DuckDB `list_sort(list_distinct(x))`;
  - Snowflake `ARRAY_SORT(ARRAY_DISTINCT(ARRAY_COMPACT(CAST(x AS ARRAY))))`;
  - ClickHouse `arraySort(arrayDistinct(arrayFilter(e -> e IS NOT NULL, x)))`;
  - Trino `array_sort(array_distinct(filter(x, e -> e IS NOT NULL)))`;
  - BigQuery `ARRAY(SELECT DISTINCT e FROM UNNEST(x) e WHERE e IS NOT NULL ORDER BY e)` (NULL reads as `[]`).
- `list_key(set_expr)` → the form a GROUP BY, comparison, `COUNT(DISTINCT)`, window `PARTITION BY` or join-back reads. Base refuses; each native-list dialect declares its own: identity on the five that group arrays natively, `TO_JSON_STRING(set_expr)` on BigQuery.
- `length(L)` renders as the size of `list_set(L)`, replacing the current non-distinct bodies.

### D4 — Representation contract
A relation (CTE, producer, stage) always carries the canonical **array**, never the key form. `list_key` is applied only at the consuming comparison / grouping / partition / join site, chosen from the slot's `ARRAY` type, never from SQL text — so it is never applied twice. `build_null_safe_eq` stays generic: callers hand it operands with `list_key` already applied.

### D5 — Grouped dimensions render as (display, group key)
Every GROUP BY site in `generator.py` consumes one structured rendered dimension `(display, group_key)`. For a `ListSetKey` dimension, `group_key = list_key(list_set(x))`; when it differs from `display = list_set(x)` (BigQuery), the SELECT projects `ANY_VALUE(display)` — deterministic, since every row of a cell has the same element set. `dimension_values` (WHERE / HAVING reuse) reads the display.

### D6 — Comparisons
List-vs-list `=`, `<>`, `IS [NOT] DISTINCT FROM` and `IN` render over `list_key(list_set(·))` of both operands after both are coerced to the element family's canonical array type (array literals, list variables and `[]` included, `[]` typed by the other operand). List-vs-scalar, ordering comparisons and `BETWEEN` stay `ListTypeError`s with today's remedies.

### D7 — `IN` grammar and binding
`core.syntax` (the one Mode B reader) accepts an `IN` right-hand side of all array literals and/or placeholders; mixing array and scalar literals is a parse error, and an `IN` over array literals is the `OR` of `=`. Variable binding writes a list variable as an array literal wherever it is one operand (`L = {v}`, an item beside array literals, a list function's argument), but leaves a placeholder forming the whole `IN` right-hand side unwritten: the owning query or measure copy carries its effective variables, and `core.syntax.parse_bound_expr` turns that placeholder into a `BoundVariable(name, value)` node. `_bind_in` binds the left side first and reads its type: a list left side takes the variable as one list operand (`L IN ({wanted})` ≡ `L = {wanted}`, `[]` included); a scalar left side expands it into the whole right-hand side, an empty one failing with the variable's name. A written array literal never expands: `team IN (['A', 'C'])` is a `ListTypeError`.

### D8 — Aggregations and catalogs
`DEFAULT_AGGREGATIONS_BY_TYPE[ARRAY]` becomes `{count, count_distinct}`; the save-time rule in `core/models.py` and re-ingest re-typing in `engine/ingestion.py` move with it. `count_distinct(L)` renders as `COUNT(DISTINCT list_key(list_set(L)))`. The facade catalog follows the type defaults, advertising `<list>_count_distinct` (§3.17).

### D9 — Warning
`WholeListWarning` (payload, `core/warnings.py`) + `SlayerWholeListWarning` (Python warning), emitted by the checker once per (operation, list key) at a grouped list dimension and at `count_distinct` over a list; the text says the operation reads whole lists as sets and names `<list>.value` for per-element results. It rides `SlayerResponse.warnings` (MCP renders it, REST carries it); the wire facades do not relay response warnings.

### D10 — Opaque lists
For `element_type: UNKNOWN`, every set-reading position (`length`, grouping, comparison, `IN`, `count_distinct`) and `.value` raise a `ListTypeError` naming the opaque element type; NULL tests and `count` stay.

### D11 — Facade
The translator's `_validate_group_by` / `_reject_lossy_cast_in_implicit_grouping` drop any list-specific grouping refusal; ordering by a list (name, alias or ordinal) stays refused through the engine's order-key check.

### Decisions log
1. A grouped cell shows the element set; raw rows show the stored list (option A over canonical-everywhere: stored order is otherwise unrecoverable). (user)
2. `count_distinct(L)` is legal — distinct element sets — with a structured warning; the MCP surface shows it, the facades do not relay it (over keeping it refused). (user)
3. Axiom 3.2 extension: "The list itself is its **element set**, its distinct non-NULL elements (`[b, a, b, NULL] ≡ [a, b]`): equality, grouping, distinct counting and `length` read that set; `D.L` keeps repeats." + `[enforced: test:tests/test_list_set_equality.py]`. (user, approved wording)
4. Whole-list `IN` is allowed as the OR of `=`, with a grammar extension (over keeping `IN` refused); list-vs-scalar `IN` stays refused. (user)
5. Opaque lists support only NULL tests and `count`; `length` over them is refused (over keeping `length` with a multiset meaning, or per-dialect support). (user)
6. Scope is native queries and the facade with one semantics; a facade-only rule would fork meaning (§3.10.1). (default, structural)
7. `length` = size of the element set through the one canonical hook. (default, reduction)
8. Canonical form = `collect`'s output shape: distinct, sorted ascending in the database's element order. (default)
9. BigQuery: JSON-text key, `ANY_VALUE` display. Live probes: Postgres 16, DuckDB 1.5.2, Snowflake and ClickHouse group, compare, `COUNT(DISTINCT)`, window-partition and null-safe-join on the canonical array; Postgres needs the canonical cast for `VARCHAR[]` vs `TEXT[]`. (default)
10. The warning also fires when grouping by a whole list. (default)
11. Ordering comparisons, `BETWEEN` and `ORDER BY` on a list stay refused; list-vs-scalar stays a type error. (issue)
12. `L.value` unchanged. (issue)
13. One idempotent canonicalisation pass before slot / grain construction; checkers stay validators. (codex)
14. Relations carry the canonical array; `list_key` applied only at the consuming site from slot type; `build_null_safe_eq` generic. (codex)
15. Every GROUP BY site consumes a structured `(display, group_key)` dimension. (codex)
16. Postgres: one canonical array type per element family; input cast before unnesting, both CASE branches typed alike, all operand kinds coerced. (codex)
17. `IN` contract: the left side's type decides whether a list variable is one operand or the whole right-hand side. (codex)
18. Grouped list stage outputs keep `ARRAY<element>`; re-canonicalisation is idempotent; multi-stage tests. (codex)
19. Window partitions read `list_key(list_set(·))`. (codex)
20. ClickHouse has no NULL list; an absent list reads as `[]` (probed live with `join_use_nulls = 1`), documented beside BigQuery. (codex)
21. `list_key` is a per-dialect capability; base refuses. (codex)
22. Facade ordering by a list stays refused; tests cover name, alias and ordinal. (codex)
23. Canonicalisation only in equality-reading positions; raw and grouped tests over the same list expression. (codex)
24. `enums.py`, `models.py` and `ingestion.py` change together; round-trip and catalog tests. (codex)
25. `ListSetKey` integrated into every kind classification, with an exhaustiveness test. (codex)
26. Spec deltas for `queries/expression-grammar` and the ClickHouse / BigQuery NULL exception. (codex)
27. Warning deduplicated per (operation, list key) and positions defined. (codex)
28. rejected (codex): removing the warning — the user required it.
29. rejected (codex): separate deltas for partitioned aggregates, transforms and stages — `models/list-columns` states the grain-member rule once, with scenarios; one enforcing test file per axiom clause.
30. rejected (codex): NaN / signed-zero / collation special cases — lists inherit the database's element equality, like scalar dimensions (semantics §4).
31. A whole-`IN` list variable stays a named `BoundVariable` until its left side is typed; text splicing cannot tell it from a scalar `IN` list. Full structural binding is DEV-2223. (user)

## Risks / Trade-offs

- [BigQuery float lists: `TO_JSON_STRING` may separate `0.0` from `-0.0` where scalar grouping merges them] → accepted, database-level (decision 30).
- [Postgres `GROUP BY` over a subquery expression across every grouping path] → probed for the base path; emission and execution tests on every GROUP BY site.
- [Display text order differs across collations, e.g. Snowflake sorts `B` before `a`] → same as `collect`; equality is unaffected.
- [`length` and opaque `length` are breaking changes] → called out as BREAKING in the proposal; docs, help topic and example notebook updated.
- [Metabase previews now advertise and may project `<list>_count_distinct`] → covered by the honest-catalog test (§3.17).
