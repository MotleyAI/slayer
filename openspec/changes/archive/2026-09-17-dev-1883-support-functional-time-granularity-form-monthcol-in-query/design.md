# Design

## Context

See proposal.md — Why. Today `_coerce_dimension_item` (`slayer/core/query.py`) turns any non-column dimension string into a `ComputedDimension`; `month(created_at)` then parses as an unknown-name `AggCall` candidate (`slayer/engine/syntax.py` unknown-callee branch) and dies at binding. Time-dimension public names are assigned in one loop in `slayer/engine/stage_planner.py` (flattened source column, no granularity), and ordering by a time dimension's column already wraps the bucket (DEV-1712 machinery).

## Goals / Non-Goals

- Goal: the functional form is pure sugar — construction-time canonicalization, byte-identical downstream behaviour to explicit `TimeDimension`s.
- Non-goals: granularity calls in Mode-B filters (deferred — follow-up issue filed); accepting the colon form `created_at:month` (collides with the `:aggregation` separator; scaffold-side only); granularity coarsening for order keys (`month(x)` over a projected `day(x)`).

## Decisions

1. **Rewrite at construction, not binding.** A single `mode="before"` model validator function runs `_migrate_schema` first, then the granularity rewrite, explicitly sequenced inside one function — Pydantic runs multiple model-before validators in reverse declaration order, so relying on declaration position would silently feed the rewrite pre-migration input (Codex finding). Alternative (binding-time rewrite) rejected: construction canonicalization gives every entry surface (REST, MCP, facades, stored queries) the feature for free and keeps serialization canonical.
2. **Classification reuses the existing expression parser.** The helper parses the string with `parse_expr` (already imported in `core/query.py` — no new architecture edge) and pattern-matches the resulting node: `AggCall` with granularity-named callee and plain single ref source → rewrite; granularity callee with any other shape → wrong-shape error; unknown callee (not scalar/transform/builtin) with single bare-column arg → typo error; everything else falls through untouched. Alternative (regex on the raw string) rejected: duplicates parsing, mishandles nesting/whitespace.
3. **Interpretation of the issue's error clause is deliberately narrow.** Only the `name(bare_col)` shape gets the granularity-naming error; multi-arg or nested unknown calls keep their existing typed errors, because legal computed dimensions are function calls too (`upper(region)`, `iif(...)`) and must not be captured. The typo error's `partition_by=` hedge keeps the `queries/computed-dimensions` "Bare aggregate in a dimension is rejected" scenario satisfied without modifying that spec.
4. **Reserved names over precedence rules.** Granularity values join the transform-only and scalar-allowlist rejections in `Aggregation` validation (`slayer/core/models.py`), making granularity-vs-custom-agg shadowing unrepresentable instead of documented. Accepted breakage: a stored model with an aggregation named like a granularity fails loudly at load; judged vanishingly rare, no migration.
5. **Collision-suffixed keys, computed where public names are assigned.** In the stage-planner declaration loop, when ≥2 selected time dimensions share a flattened column name, each gets `.<granularity>` appended to its declared/public name; singles keep today's key (preserves the DEV-1744 no-suffix contract). Exact duplicates dedupe at construction; same column+granularity with differing date range/label is rejected there (their keys would still collide). Alternatives rejected: always-suffix (breaks the single-TD key contract), rejection of all same-column TDs (changes legal explicit queries).
6. **Order keys resolve against projected time dimensions.** A granularity-shaped order entry is intercepted in order-item coercion/binding and resolved to the matching projected TD's bucket via the existing order-by-TD wrapping; missing/mismatched TD errors name the remedy. No hidden time-bucket order slots: a bucket of a column outside the grain is ill-defined, same as any non-grain field order key.
7. **API surfaces widen annotations only.** The MCP `query` tool's `time_dimensions` annotation becomes `list[dict | str]` (FastMCP derives the input schema from it); the REST `QueryRequest` gets the same audit. Validation stays in `SlayerQuery`.

## Risks / Trade-offs

- [Key shape `model.column.granularity` is new to consumers] → fires only in the previously ambiguous multi-granularity case, which produced colliding keys before; documented in the result-key docs sentence.
- [Construction error for `my_agg(col)`-shaped typos changes the error class for bare custom aggs in dimensions] → those were always illegal; the message names both readings (granularities + `partition_by=` hedge).
- [Reserved-name rejection breaks a hypothetical stored model] → fails loudly at validation with the reserved set named; accepted in review.

## Migration Plan

No data or schema migration. Rollback = revert; canonicalized stored queries remain valid either way (they contain only explicit `TimeDimension`s).
