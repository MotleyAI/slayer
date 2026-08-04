# Spec: Cube → SLayer ingestion (DEV-1608)

Convert Cube (Cube.js / Cube.dev) data-model YAML into persisted `SlayerModel`s,
mirroring the existing `import-dbt` importer (`slayer/dbt/`). Two stages:

- **Stage 1 (this PR)** — everything that maps cleanly without the Tesseract
  engine: cubes, measures, dimensions, joins, segments, `extends`, and **views**
  (via facade models). Plus any Tesseract feature that turns out to be an easy
  win (assessment below: effectively none — see §9).
- **Stage 2 (follow-up issue/PR)** — the Tesseract-only feature set and the
  hard edges Stage 1 routes to the report. Fully designed in §9, not built here.

Conversion is **fully offline**: types come from Cube's declared dimension /
measure types; no database connection is required or used. Everything that does
not convert is captured in a **structured report** (Pydantic object + JSON file).

---

## 1. Package layout (mirrors `slayer/dbt/`)

```text
slayer/cube/
  __init__.py
  models.py      # Pydantic shapes for parsed Cube YAML
  parser.py      # walk dir, collect .yml/.yaml, Jinja-detect, parse cubes:/views:
  refs.py        # {CUBE}/{member}/{cube.member} → SLayer SQL / DSL translator
  extends.py     # extends-graph resolution + member flattening
  converter.py   # CubeToSlayerConverter → CubeConversionResult
  report.py      # CubeConversionReport / CubeConversionIssue / categories
```

CLI: `import-cube` subcommand in `slayer/cli.py` + `_run_import_cube`.

The converter never touches the DB and never needs an engine. Persistence is
`run_sync(storage.save_model(model))`, exactly as `_run_import_dbt` does for
table-backed models.

---

## 2. Parser (`parser.py`)

- Recursively collect `*.yml` / `*.yaml` (skip hidden dirs, `target`, etc.) —
  reuse the dbt parser's `_collect_yaml_paths` shape.
- **Jinja detection (Codex #6 — order resolved):** Cube's own SQL-ref syntax
  uses **single** braces (`{CUBE}`, `{cube.member}`) so it never collides with
  Jinja's double-brace / brace-percent. **Try YAML-parsing first.**
  - If the file parses as YAML: scan only the **templatable fields** of each
    member (`sql`, `filter`, `case` predicates, join `sql`) for `{{`/`{%`. A
    member with a Jinja marker is skipped + reported (`requires_templating`),
    keeping the rest of the cube. (Plain YAML containing one templated SQL member
    is thus mostly imported — the earlier "scan raw text, skip whole file" rule
    is replaced by this.)
  - File-level skip applies **only** when the file fails to parse as YAML
    *because of* templating directives (e.g. `{% for %}` generating list items) —
    then skip the whole file + `requires_templating` report.
- Parse the two top-level keys: `cubes:` (list) and `views:` (list). Tolerate
  single-object forms (wrap in a list), matching the dbt parser's leniency.
- Per-item `model_validate` into the `models.py` shapes; on `ValidationError`,
  log + emit a `parse_error` report issue and continue (never abort the run for
  one bad file).

### Parsed Cube shapes (`models.py`, Pydantic v2, no dataclasses)

```text
CubeMeasure:    name, type, sql?, title?, description?, public?=True, meta?,
                format?, filters?: list[{sql}], drill_members?, rolling_window?,
                multi_stage?=False, time_shift?, grain?, filter? (Tesseract),
                case? (Tesseract)
CubeDimension:  name, sql?, type='string', title?, description?, public?=True,
                meta?, format?, primary_key?=False, sub_query?=False, case?,
                granularities?, latitude?, longitude?  (geo)
CubeJoin:       name (target cube), relationship, sql (ON clause)
CubeSegment:    name, sql, title?, description?, public?=True, meta?
CubeCube:       name, sql_table?, sql?, sql_alias?, extends?, data_source?,
                title?, description?, public?=True, meta?, refresh_key?,
                calendar?, measures?, dimensions?, joins?, segments?,
                hierarchies?, pre_aggregations?, access_policy?
CubeViewCubeRef: join_path, includes?(list|'*'), excludes?, prefix?=False,
                 alias?, per-member overrides (alias/title/description/format/meta)
CubeView:       name, cubes: list[CubeViewCubeRef], extends?, title?, description?,
                public?=True, meta?, folders?, default_filters?, access_policy?
CubeProject:    cubes: list[CubeCube], views: list[CubeView]
```

Unknown keys are tolerated (Cube evolves); they are ignored unless listed in the
"unmapped infra" set (§7), which is stashed in `meta`.

---

## 3. Reference translator (`refs.py`) — the core correctness surface

Cube `sql`/`filter` strings use Cube's curly reference syntax. Translate to
SLayer SQL (Mode A, for `Column.sql` / `Column.filter` / model `filters`) or
SLayer DSL (Mode B, for calculated-measure `ModelMeasure.formula`). These are
**not Jinja** and are translated, not skipped.

| Cube ref | Meaning | SLayer rewrite |
|---|---|---|
| `{CUBE}.col` | own table column | bare `col` (SLayer auto-qualifies) |
| `{CUBE}` (bare) | own table alias | drop / context-specific (see joins) |
| `{member}` (same cube) | sibling member on this cube | bare `member` — SLayer inlines sibling derived columns recursively |
| `{other_cube.member}` | member on a joined cube | Mode A: `other_cube.member` (single hop) or `a__b.member` (multi-hop); SLayer inlines derived joined columns |
| `{measure}` inside a calc measure | another measure (post-agg) | Mode B: bare `measure` (resolves as `ModelMeasure` name) |

Mechanics:
- A single-pass regex over the string, **skipping SQL string literals**
  (reuse the `_STRING_LITERAL_RE` idea from `core/models.py`).
- Distinguish `{X}` (one dotless token → same-cube member or `CUBE`),
  `{X.Y}` (cube.member), and `{CUBE}` / `{CUBE.col}` specially.
- Multi-hop `{a.b.c}` → SLayer multi-dot `a.b.c`, which `Column.sql`'s own
  `_fix_multidot_sql` validator then converts to `a__b.c` (we lean on the
  existing model-side normalization rather than duplicating it).
- **Boundary / report rule (user-established):** after translation, if the
  result is not valid for its target mode, do **not** emit a broken member —
  route it to the report:
  - `Column.sql` / `Column.filter` / model `filters` are validated by Pydantic
    construction (Mode A `parse_sql_predicate`). A `ValidationError` → catch →
    `complex_sql` report issue, member dropped.
  - Calculated-measure `ModelMeasure.formula` is Mode B. If the translated
    formula is clean DSL (arithmetic / `||` / the `SCALAR_PASSTHROUGH` scalar
    set / measure refs) it converts; otherwise (`CASE WHEN`, raw SQL funcs Mode B
    rejects, unresolved refs) → `complex_measure` report issue, measure dropped.

---

## 4. Cube → model conversion (`converter.py`)

Top loop: **for each cube → one table-owning model; for each view → one facade
model** (§6). Both emit ordinary `SlayerModel`s. Order: resolve `extends` first
(§5), then convert cubes, then views (views need cube models to exist for
member/measure lookup and fan-out detection).

### 4.1 Cube → SlayerModel

| Cube | SLayer | Notes |
|---|---|---|
| `name` | `name` | |
| `sql_table` | `sql_table` | verbatim (Cube uses `schema.table`) |
| `sql` | `sql` | `{CUBE}`/`{member}` translated; if it references measures or is otherwise un-rewritable → `complex_sql` report and the cube is dropped (`no_source`), since a cube with no usable source can't be emitted (see the source rule below) |
| `data_source` (per-cube) | — | converter scopes ALL models under the single `--datasource`; per-cube `data_source` → `unmapped_infra` report + stashed in `meta.cube_unmapped.data_source` |
| `title` | — | no model title field; → `meta.cube_title` (info-level), not dropped silently |
| `description` | `description` | |
| `public: false` | `hidden: true` | |
| `meta` (incl `ai_context`) | `meta` | merged under the model's `meta` (preserved verbatim) |
| `sql_alias`, `refresh_key`, `calendar`, `hierarchies`, `pre_aggregations`, `access_policy` | — | §7 report + `meta.cube_unmapped.*` |
| `measures` | `columns` + `measures` | §4.2 |
| `dimensions` | `columns` | §4.3 |
| `joins` | `joins` | §4.4 |
| `segments` | `columns` (boolean) | §4.5 |

A cube must end with exactly one source. Normal case: `sql_table`. If only `sql`
is given and it translates cleanly → `sql`. If neither is usable, emit nothing
for that cube and report `no_source` (error severity).

### 4.2 Measures — Column + ModelMeasure split (same idiom as dbt)

Aggregating measures (`type` ∈ `count`, `count_distinct`, `count_distinct_approx`,
`sum`, `avg`, `min`, `max`):

- The `sql` expression → a `Column` (`DataType.DOUBLE`, `NumberFormat(FLOAT)`
  default unless `format` maps — §8). Bare-identifier `sql` → Column named after
  the column; non-trivial expression → Column named `<measure>_col`. Collisions
  resolved by `_col` suffix (reuse dbt's logic).
- **Column dedup key must include the filter + window state** (Codex #4). The
  dbt converter groups measures by `sql` expression alone (safe because dbt
  measures carry no per-measure column state). Cube measures put `filters` on the
  emitted `Column.filter`, so two measures sharing one `sql` but differing in
  `filters` (or `rolling_window`) **must** become distinct columns — the dedup
  key is `(translated_sql, translated_filter, window_spec)`, not `sql` alone.
  Otherwise a filter would bleed across measures.
- The measure → a `ModelMeasure` whose formula is `<col>:<agg>`.
- `type: count` with no `sql` → `ModelMeasure(formula="*:count")` (COUNT(*)),
  no Column needed. `count` **with** `sql` → `<col>:count`.
- Aggregation name map: `count_distinct`→`count_distinct`,
  `count_distinct_approx`→`count_distinct` (+ `lossy_mapping` info report: SLayer
  has no approximate distinct), others 1:1.

Calculated measures (`type` ∈ `number`, `string`, `time`, `boolean`) — these are
post-aggregation expressions referencing other measures (e.g.
`sql: "{revenue} / {count}"`):

- → a `ModelMeasure` whose formula is the Mode-B-translated expression,
  `type` set to the mapped `DataType`. Convert if clean DSL; else `complex_measure`
  report (§3 boundary).

Per-measure extras:
- `filters: [{sql}, …]` (conditional aggregation) → the Column carries a `filter`
  (the AND of the translated predicates). Same Column+ModelMeasure split; the
  filter lives on the Column (dbt simple-filtered-metric idiom).
- `format` → §8.
- `title`→`ModelMeasure.label`, `description`→`ModelMeasure.description`,
  `meta`→`ModelMeasure.meta`.
- `rolling_window` with a **finite `trailing`** and no `leading`/`offset` →
  windowed aggregation `<col>:<agg>(window='<dur>')` (Cube `1 month`→`1m`,
  `7 day`→`7d`, etc.). `unbounded` / `leading` / `offset` / `rolling_window` on a
  non-sum/avg agg → `unsupported_rolling_window` report, measure falls back to the
  plain aggregation (still emitted) with a warning.
- `drill_members` → §7 report + `meta.cube_unmapped` on the ModelMeasure.
- `multi_stage`, `time_shift`, `grain`, `filter` (Tesseract), `case` (Tesseract)
  → §9 (Stage 2). Measure is emitted as its plain aggregation if one exists,
  else routed to report as `deferred_stage2`.

### 4.3 Dimensions → Column

| Cube dim | SLayer Column | Notes |
|---|---|---|
| `type: string` | `type=TEXT` | |
| `type: number` | `type=DOUBLE` | Cube doesn't distinguish int/float; refine later via `slayer ingest` |
| `type: boolean` | `type=BOOLEAN` | |
| `type: time` | `type=TIMESTAMP` | |
| `sql` | `Column.sql` (translated); omitted when it's just `{CUBE}.<name>` | |
| `primary_key: true` | `Column.primary_key=True` | PK columns auto-restricted to count/count_distinct |
| `case:` (CASE-WHEN dim) | `Column.sql` built as `CASE WHEN … THEN … ELSE … END` from `when[].{sql,label}` + `else` | **Stage 1** — `case` *dimension* is not Tesseract |
| `title`→`label`, `description`, `meta`, `format` | direct | |
| `type: geo` (+ `latitude`/`longitude`) | — | §7 report + `meta.cube_unmapped.geo`; not split into lat/long columns in Stage 1 |
| `sub_query: true` | — | §7 report + `meta.cube_unmapped`; correlated per-row measure has no SLayer equivalent |
| `granularities:` (custom) | — | base time Column still emitted; custom grains → §7 report (SLayer granularity is query-time enum) |
| `type: switch` | — | §9 (Tesseract) |
| `links:`, `order:` | — | presentation; §7 report |

### 4.4 Joins

`CubeJoin.sql` is an ON clause like `{CUBE}.customer_id = {customers.id}`.

- Parse the ON into equality column pairs. Conjunctions (`A=B AND C=D`) →
  multiple `join_pairs` (SLayer supports composite keys). The qualifier matching
  `{CUBE}` is the source column; the qualifier matching `{<target>.…}` is the
  target column.
- **Resolve each ON side to a physical column name** (Codex #2 — verified: the
  SQL generator emits raw `alias.src = alias.tgt` from `join_pairs`; it does NOT
  expand `Column.sql`). `{customers.id}` means the `id` *member*, which may have
  `sql: "{CUBE}.customer_id"`. So follow each ON ref to its member's `sql`: if it
  is a bare physical identifier, use it; if the member's `sql` is a non-trivial
  expression (function, arithmetic, CASE), the column pair can't be expressed in
  `join_pairs` → `unsupported_join` report, join dropped. Same for a `{CUBE}.col`
  side that resolves to a derived dimension.
- `target_model` = the join `name` (the target cube).
- `relationship` (`many_to_one` / `one_to_many` / `one_to_one`, plus legacy
  `belongs_to`/`has_many`/`has_one`) is **not stored on `ModelJoin`** (SLayer
  joins are all LEFT). It IS recorded internally and used for view fan-out
  detection (§6). Emitted `ModelJoin.join_type = LEFT`.
- Non-equi ON (ranges, function calls, inequalities), or an ON that doesn't
  resolve to clean column pairs → `unsupported_join` report; the join is dropped
  (both cubes still exist as models, just not auto-joined).

### 4.5 Segments → boolean derived Column

Each segment `{name, sql}` → `Column(name=<name>, sql=<translated predicate>,
type=BOOLEAN)`. Filterable (`name = true`) and group-able. `title`/`description`/
`meta` carried onto the Column. Also recorded in the report (`segment_as_column`,
info). Name-collision with an existing column/measure → `_seg` suffix + warning.

---

### 4.6 Namespace allocation & emit-time safety (Codex #5, #7)

The core `SlayerModel` validators reject duplicate column names, duplicate
measure names, **any column↔measure overlap**, and `ModelMeasure` names that
shadow a built-in transform (`cumsum`, `rank`, `change`, `lag`, …). A naive
converter that lets these collide makes **whole-model construction throw**,
losing the entire model. So:

- **Preflight namespace allocator** per emitted model/view: allocate column and
  measure names against a shared seen-set (columns + measures share one
  namespace). Dimension/segment/measure/entity name clashes are resolved by a
  deterministic suffix (`_col` / `_seg`) **before** constructing the model.
- A Cube measure/metric name that shadows a SLayer transform, or a collision
  that can't be safely renamed, is routed to the report (don't emit it) rather
  than thrown.
- **Offline validation helper** (Codex #7 — verified: `Column.sql` is *not*
  SQL-validated at construction; only `Column.filter` / `SlayerModel.filters`
  parse a predicate, and `ModelMeasure.formula` only rejects raw `OVER`). After
  building each model, run an explicit offline pass: sqlglot-parse every
  translated `Column.sql`, `parse_sql_predicate` every filter, and formula-parse
  every `ModelMeasure.formula`. A parse failure routes that member to the report
  (`complex_sql` / `complex_measure`) and drops it — so a structurally-broken
  member never persists to fail later at enrichment.

## 5. `extends` (flatten) — `extends.py`

> **Approach for this PR: flatten.** Native persisted model inheritance
> (making `ModelExtension` a saveable source mode) is tracked separately in
> **DEV-1610** and is deliberately *not* a dependency of DEV-1608. When DEV-1610
> lands, `extends.py` swaps from flattening to emitting
> `SlayerModel(extends=ModelExtension(...))` — a ~10-line change. Flattening is
> faithful to Cube's own compile-time materialization of extended cubes, so it
> is not a stopgap-quality result.

- Build the extends graph across all cubes. Resolve transitively (multi-level),
  detect cycles → `extends_cycle` report (error) and skip the cycle members.
- Flatten: a child inherits the parent's measures / dimensions / joins /
  segments; **child members win** on name conflict. Child's own `sql_table`/`sql`
  override the parent's source.
- Every cube is still emitted as its own model (hidden iff `public: false`), so
  an abstract base (`public: false`, only extended) becomes a **hidden** model
  AND its members are flattened into children. Lossless, mirrors Cube's
  `public: false`, matches SLayer's hidden-model convention.
- Views can `extends` other views — same flattening over view member lists.

---

## 6. Views → facade models — the key structural mapping

A Cube view owns no table; it re-exports members from cubes along a `join_path`.
SLayer has no view type, so a view becomes a **thin regular `SlayerModel`
anchored on the join_path root cube's table**:

- **Source mode mirrors the root cube's emitted model** (Codex #3): copy
  `sql_table` *or* `sql` — whichever the root cube produced. If the root cube was
  not emitted (e.g. it failed conversion), drop + report the view
  (`ambiguous_view_root` / `disconnected_view`). Never hard-code `sql_table`.
- `joins` = the joins implied by the view's `join_path`s (reuse the root cube's
  existing join definitions by walking the path; each hop must correspond to a
  declared cube join).
- Each included **dimension** → a derived `Column` whose `sql` references the
  joined cube's column: `Column(name=<exported>, sql="customers.name")`
  (single-dot Mode A joined ref; multi-hop uses `a__b.col`). Root-cube dimensions
  reference their own column.
- Each included **measure** → a `ModelMeasure`, but it **must reference the
  underlying `Column`, not the Cube measure name** (Codex #1 — verified:
  `query_engine.py:2345` resolves cross-model aggs via
  `target_model.get_column(name)`, never `get_measure`). So for a Cube measure
  `revenue {type: sum, sql: amount}` whose emitted underlying column is `amount`
  (or `<measure>_col` for an expression):
  - **root-cube** measure → the facade also **carries the underlying `Column`**
    (its physical/derived expression), and the `ModelMeasure` is a local
    `amount:sum` (or `<col>:<agg>`). A root-cube measure can't be re-exported by
    bare name because the facade is a *separate* model that doesn't own the root
    cube's measures.
  - **joined-cube** measure → cross-model `customers.amount:sum`
    (`joinpath.<underlying_col>:<agg>`); the underlying column already lives on
    the joined cube's emitted model, so no copy is needed.
  - **filtered** Cube measure → the underlying column carries the `filter`
    (root: copied onto the facade; joined: already on the joined model), so
    `customers.<filtered_col>:<agg>` still applies the CASE-WHEN correctly.
  - **`count`** → `joinpath.*:count` (or local `*:count`); `customers.*:count`
    resolves via the `measure_name == "*"` path.
  - **calculated** (`type: number/…`, references multiple measures) re-export →
    inline the cross-model **column** refs if the result is clean Mode B
    (`customers.amount:sum / customers.*:count`); otherwise → `complex_measure`
    report, that measure dropped from the view.
- The converter therefore threads, from cube-measure conversion, the
  `(underlying_column_name, aggregation, filter?)` triple for every measure so
  the facade builder can synthesize the correct `<path>.<col>:<agg>` formula.
- `prefix: true` → exported names are `<cube>_<member>`. Per-member `alias` →
  the exported name. `title`/`description`/`format`/`meta` overrides → the
  Column/ModelMeasure fields.
- `default_filters` (`{member, operator, values, unless?}`) → model `filters`
  (Mode A SQL predicates built from operator+values; `member` resolved to its
  SQL column / joined ref). Operators that don't translate to plain SQL →
  `unsupported_default_filter` report, that filter dropped.
- `excludes` / `includes: '*'` honored when selecting members.
- `meta: {cube_kind: "view"}` stamped so the report and future round-trips can
  identify facade models.

**Common case built in Stage 1.** Routed to the report (not guessed) when:
- the view's members span cubes **not on one connected join tree** rooted at the
  join_path root (`disconnected_view`),
- the join_path root is **ambiguous** / not derivable (`ambiguous_view_root`),
- a hop in the path is `one_to_many` / `many_to_one`-reversed such that the
  facade would **fan out** the root and double-count a root measure
  (`view_fanout_risk` — detected via the recorded `relationship`),
- `folders` present → parked in `meta.cube_unmapped.folders` + `folders_unmapped`
  report (no SLayer hierarchy concept),
- any per-member override or operator that doesn't map → reported, that member /
  filter skipped, rest of the view still emitted.

---

## 7. "No SLayer home" features — report + stash in meta

For every Cube feature with no SLayer equivalent, emit a structured report issue
AND preserve the raw Cube fragment under a namespaced key on the owning entity's
`meta`: `meta.cube_unmapped.<feature>`. Set covers (per §4/§6):

`pre_aggregations`, `refresh_key`, `calendar`, `hierarchies`, `drill_members`,
`access_policy`, `sql_alias`, per-cube `data_source`, `geo` dims (+lat/long),
`sub_query` dims, custom `granularities`, dimension `links`/`order`, view
`folders`, `multi_stage`/`time_shift`/`grain`/Tesseract bits (§9).

Genuinely-semantic metadata is **not** in this bucket — `title`→`label`,
`description`, `meta`/`ai_context`, `format` always carry over directly to the
proper SLayer field.

---

## 8. Format mapping (`format` → `NumberFormat`)

`NumberFormat` has `type ∈ {percent, currency, integer, float}`, `precision`,
`symbol` (currency only). Map Cube formats:

- `percent` → `PERCENT`
- `currency` (+ `currency_symbol` if present) → `CURRENCY` (symbol)
- `number` / numeric d3-ish → `FLOAT` (with `precision` if a `_N` suffix is
  parseable)
- `accounting`, `abbr`, arbitrary d3-format strings, `imageUrl`/`link`/`id` →
  `unsupported_format` report; format dropped (the field/measure still emitted).
- **`NumberFormat.symbol` guard** (Codex #8 — verified `format.py:40`: `symbol`
  is rejected unless `type == CURRENCY`, and is auto-defaulted to `$` for
  currency). Never pass `symbol` for non-currency mappings. A Cube format payload
  carrying a symbol-like field on a non-currency type, or an otherwise invalid
  format, is reported + dropped rather than allowed to raise at model
  construction.

---

## 9. Stage 2 (Tesseract) — designed here, built in the follow-up issue

`CUBEJS_TESSERACT_SQL_PLANNER`-only features. **Assessment: none are easy wins**
— each lacks a clean SLayer mapping, so all are deferred. Stage 1 routes any cube
using them to the report as `deferred_stage2` (the cube's non-Tesseract members
still convert).

| Tesseract feature | Why no clean SLayer map | Proposed Stage-2 approach |
|---|---|---|
| `switch` dimension | Query-time selectable dimension; SLayer dimensions are static | No direct map. Possibly enumerate switch cases into N separate columns + a report note; needs design. |
| `number_agg` measure | Aggregates an expression that itself contains aggregations (multi-stage) | Multi-stage `source_queries` model: inner stage materializes the inner aggregation, outer stage re-aggregates. Needs the query-backed-model builder. |
| `case` measure | Conditional measure keyed on a `switch` dimension | Depends on `switch`; without it, a pure `CASE WHEN` over a condition → a filtered Column (the one borderline "maybe easy" — evaluate during impl, default to defer). |
| measure `filter` (`exclude`/`keep_only`/`mode`) | Grain manipulation at aggregation time | No SLayer grain-override; candidate for a multi-stage rewrite. |
| `multi_stage` + `time_shift` + `grain` (non-Tesseract but same family) | Cube's measure-level time-shift/grain grammar ≠ SLayer's query-time `time_shift`/transform model | Map finite `rolling_window` trailing in Stage 1 (§4.2); defer `time_shift`/`grain` to the multi-stage rewrite. |

Plus the Stage-1 "hard edges" routed to the report: disconnected/ambiguous/
fan-out-risk views, non-equi joins, complex SQL/measures, custom granularities,
geo split, sub_query dims. The follow-up issue tackles these alongside Tesseract.

---

## 10. Structured report (`report.py`)

```python
class CubeIssueCategory(str, Enum):  # requires_templating, parse_error,
  complex_sql, complex_measure, lossy_mapping, unsupported_join,
  unsupported_rolling_window, unsupported_format, unsupported_default_filter,
  segment_as_column, unmapped_infra, geo_unmapped, subquery_unmapped,
  granularity_unmapped, disconnected_view, ambiguous_view_root, view_fanout_risk,
  folders_unmapped, extends_cycle, no_source, deferred_stage2

class CubeConversionIssue(BaseModel):
  category: CubeIssueCategory
  severity: Literal["info","warning","error"]
  cube: str | None;  view: str | None;  member: str | None
  message: str
  raw: str | None            # raw Cube fragment when useful

class CubeConversionReport(BaseModel):
  issues: list[CubeConversionIssue]
  model_count: int;  hidden_count: int;  view_count: int
  # counts derived; helpers to filter by category/severity

class CubeConversionResult(BaseModel):     # converter return
  models: list[SlayerModel]
  report: CubeConversionReport
```

(No `Dict`-typed LLM-output fields; this is internal, so plain fields are fine.
No dataclasses.)

---

## 11. CLI

```text
slayer import-cube <cube_project_path> --datasource NAME [--storage PATH]
                   [--report PATH] [--include-hidden]
```

- Recursively parse, convert, `storage.save_model` each model, print a console
  summary (imported models with column/measure counts + `[hidden]`, then issues
  grouped by severity), and **always write the JSON report** to
  `<storage_dir>/cube_import_report.json` (override with `--report PATH`).
- `_run_import_cube` mirrors `_run_import_dbt` structurally.
- `--datasource` is just the SLayer datasource name to file models under; it need
  not exist or be reachable (offline). (`--include-hidden` reserved for parity;
  cubes are already emitted hidden when `public: false`, so it mainly governs
  whether hidden models print — keep minimal.)
- The `slayer/cube/` converter API stays importable for programmatic use.

---

## 12. Tests (TDD — full suite first, per `feedback_tdd_style.md`)

Mirror `tests/test_dbt_*`. New files:

- `tests/test_cube_parser.py` — YAML collection; Jinja file-skip + member-skip +
  report; single-object tolerance; malformed-file `parse_error` continue.
- `tests/test_cube_refs.py` — `{CUBE}.col`→`col`; `{member}`→bare; `{cube.member}`
  → joined ref; multi-hop; string-literal skipping; calc-measure Mode-B
  translation; un-rewritable → report boundary.
- `tests/test_cube_converter.py` — cube→model 1:1; measure Column+ModelMeasure
  split (count/count_distinct/approx/sum/avg/min/max); `*:count`; calc measures;
  measure `filters`→Column.filter; finite rolling_window→`window=`;
  dimensions (string/number/bool/time, `case` dim, primary_key); joins (single +
  composite + non-equi→report); segments→boolean column; format mapping;
  unmapped infra→report+meta stash.
- `tests/test_cube_extends.py` — single + multi-level flatten; child-wins; abstract
  base emitted hidden; cycle→report.
- `tests/test_cube_views.py` — facade model: dims→derived columns, measures→
  local/cross-model ModelMeasures, prefix/alias/overrides, default_filters→model
  filters, excludes/`*`; disconnected/ambiguous/fanout/folders→report.
- `tests/test_cube_report.py` — categories, severities, counts, JSON round-trip.
- `tests/test_cube_cli.py` (or fold into converter) — `import-cube` writes models +
  JSON report; offline (no datasource needed); console summary.
- `tests/fixtures/cube_project/` — hand-written sample `.yml` (cubes, a view,
  extends, segments, a Jinja file, a Tesseract cube) covering the above.
- **`tests/test_cube_smoke.py` — enrich/execute converted models, not just
  assert converter output** (Codex test-gap). Build a tiny SQLite datasource +
  converted models and actually run queries: (a) a view cross-model measure that
  references the underlying joined column (`customers.amount:sum`), (b) a view
  rooted on a `sql`-backed cube, (c) a multi-hop facade dimension, (d) two
  filtered same-`sql` measures returning different values, (e) `show_sql` on a
  facade measure. These catch the §6/§4.4 mapping breaks at the SQL layer.
- **Negative validator-boundary construction tests** (Codex test-gap): a converter
  input that would yield column↔measure namespace overlap, a measure named after
  a transform (`cumsum`), a Cube name containing `.`/`:`, an ON that yields empty
  `join_pairs`, a facade whose root cube wasn't emitted, and a non-currency
  format carrying a symbol — assert each is routed to the **report** (model still
  emitted where possible), NOT raised as an unhandled `ValidationError`.

Run the full non-integration suite after implementation; fix all failures.

---

## 13. Docs (update on user-facing change)

- New `docs/cube/cube_import.md` (mirror `docs/dbt/dbt_import.md`): mapping
  tables, the non-mapping catalog, the report, CLI usage. Link from `mkdocs.yml`.
- `CLAUDE.md` — short note under an importer/CLI section.
- `.claude/skills/` — mention `import-cube` where `import-dbt` is referenced.

---

## 14. Explicit non-goals (Stage 1)

- No live DB connection / type refinement / sample profiling (run `slayer ingest`
  afterward).
- No Jinja/Python template rendering.
- No Tesseract features built (designed in §9).
- No MCP/REST surface (CLI + importable API only), matching `import-dbt`.
- No round-trip SLayer→Cube export.
