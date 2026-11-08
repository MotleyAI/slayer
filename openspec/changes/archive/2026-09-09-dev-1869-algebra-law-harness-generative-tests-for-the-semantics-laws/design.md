# Design: Algebra law harness + guard ratchet

## Context

Laws live in `architecture/semantics.arc42.md` §3 as ∀-quantified equations; the fixture stack (`tests/_dev1739_fixtures.py` → `_dev1824_fixtures.py` → `_dev1837_fixtures.py`) provides the orders→customers→regions models, the 7-row dataset, dimension families (`DIM_FAMILY_DIMS`: col/expr/band/bare/rank/mixed), a 14-measure pool (`ATTACH_MEASURES` + `TRANSFORM_FORMULAS`), and the SQLite+DuckDB executed-engine harness. The DEV-1838 sweep meta-test scans `slayer/sql` only. Every DEV ref in today's deferral guard messages points at a closed issue.

## Goals / Non-Goals

**Goals:** each immediate law (split-invariance, grain union, broadcast coherence, lowering soundness) directly tested; guard list enumerated, issue-ref'd, shrink-only; failures name the law.

**Non-Goals:** `/` operator in coherence pairs (dialect division noise); order/limit shapes and measure-typed filters in the generator (position-parity territory, DEV-1865); position parity / dice–slice / population invariance (post-DEV-1865/1841/1866 increments tracked on the issue); Hypothesis variant (DEV-1877); golden-baseline slimming (possible follow-up).

## Decisions

### D1 — Seeded deterministic generator (`tests/_law_harness.py`)

- `LawShape` (Pydantic): `dim_family`, `measure_keys` (2–4 of the 14-measure pool), `filter` ∈ {None, `region = 'North'`, `status = 'ok'`, `customers.tier = 'gold'`}; time dimension auto-forced when a measure needs it. Chosen over Hypothesis (DEV-1877) for determinism, async compatibility, zero new deps.
- `sample_shapes()`: deterministic covering core first — every dim family, every measure key, every filter kind, ≥1 cross-model, ≥1 windowed — asserted at collection time; `random.Random(LAW_SEED=1869)` fills to `N_SHAPES = 40`. Stable param ids. DuckDB runs an explicit shape-id slice (`N_DUCKDB = 8`); execution-count arithmetic kept in a module comment.
- Expected-raise registry keyed per executed query variant (shape id + dropped measure), each entry matched against a `DEFERRAL_SITES` message fragment — never a bare `NotImplementedError` catch. No silent skips.
- Keyed-rows helper asserts `len(rows) == len(keys)` (duplicate group keys are themselves a grain violation) before any set/value comparison; NULL-safe, `pytest.approx` for floats.
- Assertion convention: `LAW <name> violated — <detail>; shape=<descriptor>`.
- Engine fixture variant exposes `(dialect, db_path)` for raw-oracle SQL.

### D2 — Split-invariance (compositionality: single-measure splits)

Run Q; for each measure m run Q∖{m}; identical group-key sets and per-cell equality on shared measures.

### D3 — Grain union (standalone)

Two halves: (a) population — measure-bearing group set ≡ dims-only group set; (b) grain — the direct law form: formula `x = a ⊕ b` over operands of different explicit grains (e.g. `part(region) ⊕ part(city)`) queried at a finer grain (`region, city, month`); `x` constant within each union-grain cell. Broadcast constancy per operand is asserted within the operand's full semantic grain — partition keys plus the time bucket for `window=` operands; `customers.spend:sum` constant everywhere, `== 350` only on filterless shapes.

### D4 — Broadcast coherence

`N_PAIRS = 24` seeded (a, b, op) triples over {plain, part(region), part(city), last_part(region), win_part(region), cm} × {+, -, *}, at grains (`region, city`) and (`region` + month). Left: one formula measure `a ⊕ b`. Right: `a`, `b` separate in the same query, combined client-side with SQL NULL propagation (the combine step is thereby an outside oracle). Plus one named three-grain chain case: a region-grain operand read at (`region`), (`region, city`), (`region, city, month`) must agree along the chain (coercions compose).

### D5 — Lowering soundness (named) + `force_unfused` seam

- Assoc-inline: SLayer filter `customers.tier = 'gold'` (to-one hop proven via `customers.id` primary key) vs raw qualified inline-WHERE JOIN SQL executed on the same DB file; region grain + month-grain variant; keyed compare via `month_key`.
- Fusion parity: `SQLGenerator(force_unfused: bool = False)` — constructor-set, immutable; in the no-transform branch the `fusion_blockers()` list gains `"forced unfused (test seam)"`. That branch is the only point where a fusion *choice* exists (transform/combined paths have no fused alternative), which scopes the test's claim. Test builds `plan_query` + bundle directly (the `TestCteBodyArms` pattern), asserts fused text has no WITH / unfused took the wrap, executes both raw, compares keyed rows; renders twice per mode (stability). Default-path invariance is pinned by the existing golden suite.

### D6 — Guard ratchet (`tests/test_law_guard_ratchet.py`, absorbs `test_dev1838_sweep.py`)

- AST scan `slayer/sql` + `slayer/engine`. Message classes: **coexistence** (unchanged markers) → empty; **deferral** (`not yet supported|deferred|not supported (DEV-`) → must carry `DEV-\d+` AND appear in the exact `DEFERRAL_SITES` enumeration; count == `guards.baseline` in `architecture/index.yaml` (only ever lowered; raising requires explicit review — same social contract as the import-contract baselines); **expressiveness** → regex allowlist (minus reclassified deferrals, plus engine's typed-residue arms). A raise with no literal message text is red unconditionally.
- Re-pointing (baseline = 10): stage_planner cross-model partition_by family (389/399/405), regroup mixed-filter (353), generator AGGREGATE-phase composite (2496), transform-as-dimension (3648) → **DEV-1868** (detailed parking comment posted there); nested-attach (1080/1092/3685) → **DEV-1847**; query-backed render (5472) → **DEV-1878**. Time-axis arm (287): drop `(DEV-1839)` — typed residue per DEV-1873 — and move to expressiveness.
- Message edits re-bless recorded-raise golden keys per-key via each module's `ALLOWED_DELTAS` + `SLAYER_UPDATE_GOLDEN` (dev1740_regroup / dev1824 / dev1839 baselines).

### D7 — arc42 tag flips (`semantics.arc42.md` §3)

Law 1 → `[enforced: test:tests/test_law_grain_union.py]`; Law 2 → `[enforced: test:tests/test_law_broadcast_coherence.py]`; Law 3 full-law clause split — single-measure splits enforced (`test_law_split_invariance.py`), measure-typed filters + order entries `[review]`; Law 6 split — assoc-inline + fusion enforced (`test_law_lowering_soundness.py`), filter-as-hidden-measure `[review]`.

## Risks / Trade-offs

- [Sampled shapes hit fail-closed guards] → per-variant expected-raise registry pinned to enumerated sites; drift is loud.
- [Float/NULL comparison flakiness] → keyed NULL-safe approx compares; dataset values chosen integral.
- [Message edits break recorded-raise goldens] → per-key `ALLOWED_DELTAS` re-bless, the sanctioned loop.
- [Baseline raised alongside a new guard in one PR] → enumeration diff + review policy; accepted (matches existing ratchets).
- [DuckDB/SQLite representation drift] → `month_key` normalization; DuckDB limited to the spot slice.

## Migration Plan

Pure addition + in-place meta-test absorption; revert = delete files, restore sweep, revert message/tag/baseline edits.
