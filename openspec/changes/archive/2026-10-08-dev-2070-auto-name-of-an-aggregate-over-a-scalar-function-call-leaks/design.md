## Context

See proposal.md › Why. The expression leaf (`expression_source_leaf`, `slayer/core/refs.py`) feeds the public stage-formula alias (`canonical_aggregate_alias`, called from `bind_inputs._canonical_alias_for_formula` on the BOUND key), the internal `cte_schema` / `cross_model_cte` aliases and the generator's `AggRenderSpec.name` (on the PLANNED key), and metadata lookups in `engine/key_metadata.py` / `engine/response_meta.py`. Its renderer `_value_key_display` duplicates `key_display` partially and falls back to `legacy_key_str`. `EXPRESSION_SOURCE_KINDS` (refs.py) omits `TimePointCmpKey`; `binding._BOUND_EXPRESSION_SOURCE_KINDS` re-adds it by hand. Date comparisons are lowered after naming (`resolve_time_points`, `bind_inputs.py` / `elaborate_env.py`): `==` becomes a two-bound `and`, relative points resolve against `bundle.now`.

Applicable principles: sql §3.3 (one naming authority), system §3.13 (one key kind per meaning; dispatch fails closed), system §3.9 (result names carry canonical paths). No arc42 / `.c4` edit.

## Goals / Non-Goals

**Goals:** one renderer for expression leaves (total over key kinds, fails closed); one expression-source kind set; home-relative operand spelling; the derivation owned by `slayer/sql/naming.py`.

**Non-Goals:**
- `_partition_key_display` and `_transform_key_canonical_str` keep their `legacy_key_str` fallbacks: they build internal alias / parameter identity only, `partition_by` rejects expressions from users, and the spelling is deliberately pinned (`tests/test_dev1871_alias_stability.py` module docstring).
- No migration for saved queries referencing old auto-names (not persisted).
- SQL operator spellings in measure formulas (infix `LIKE`, `=`, …) — DEV-2071.

## Decisions

**D1 — Leaf = sanitized `key_display` text.** `key_display` already renders every kind as formula text and raises on an unknown kind. `_value_key_display` is deleted; `key_display`'s docstring drops "never identity" (it now feeds names; collisions stay loud per spec). Alternative — extend `_value_key_display` with the missing kinds — rejected: keeps two renderers that must agree by hand.

**D2 — Home-relative rendering is structural.** The leaf renderer takes the anchor `source_anchor_path(source)` and, while recursing, strips that tuple from the path of each row-level leaf (`ColumnKey`, `ColumnSqlKey`, the column inside `TimeTruncKey`, `InKey` operands — exactly the leaves `source_leaf_paths` reads), stopping at `AggregateKey` / `TransformKey` (opaque attached constituents, rendered by plain `key_display`). Implemented as an anchor-aware variant of the display recursion (e.g. a rebase of the source's row-level leaves via `map_children` that stops at attached nodes, then `key_display`), never a string replace on rendered text. Root-homed sources (anchor `()`) render exactly as today. Rationale: the key's prefix already carries the anchor (`canonical_aggregate_alias` → `source_anchor_path`), matching `sum(customers.spend)` → `customers.spend_sum`.

**D3 — `TimePointCmpKey` joins `EXPRESSION_SOURCE_KINDS`**; `_BOUND_EXPRESSION_SOURCE_KINDS` is deleted and its users read the shared tuple. Planned keys never carry it (lowered first), so generator / metadata sites are unaffected; the public alias of a top-level date comparison now takes the expression path.

**D4 — The derivation lives in `slayer/sql/naming.py`** (sql §3.3). `expression_source_leaf` moves there with the D2 renderer; callers (`generator.py`, `engine/key_metadata.py`, `engine/response_meta.py`, `naming.canonical_aggregate_alias`) import it from naming (engine → sql.naming already exists). `refs.py` keeps `key_display`, `auto_name_from_expression`, `EXPRESSION_SOURCE_KINDS`.

**D5 — Public name from the bound key, internal aliases from the planned key.** Kept as today: the public alias derives from the bound (pre-lowering) key so a relative point names by its text and is clock-independent; internal aliases derive from the lowered key and only need to be unique within the plan. The executed reference tests (filter, order, downstream stage) guard that no lookup assumes the two coincide.

## Risks / Trade-offs

- [Lower injectivity: `key_display` omits an aggregate's locus and a transform's empty partition / time key, which the repr distinguished] → public collisions already fail with the duplicate-key error and hidden slots are uniquified (`engine/compile/projection.py`); a regression test pins "never silently shared".
- [Operator-only differences collide (`==` vs `!=`, `-` vs `+`)] → pre-existing sanitizer property, loud per spec; pinned by a scenario.
- [Renamed keys break saved references] → limited to pathed expression aggregates and top-level date comparisons; called out as BREAKING in the proposal.
- [Golden / alias pins move] → re-bless only the listed pins; every other golden stays byte-identical.
- [Downstream-stage respelling (`stages.py` `_respellings`) assumes path- then leaf-prefixed names] → home-relative leaves no longer embed the path, so only the prefix needs respelling; covered by the executed downstream-stage test.
