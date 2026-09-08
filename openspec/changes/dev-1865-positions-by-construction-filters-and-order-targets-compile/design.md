# Design: Positions by construction

## Context

See proposal.md for motivation. Current machinery this replaces or reshapes:

- Filters: predicates render inline via `FilterPhase`/`filters_by_phase` (`slayer/engine/planned.py:137`, assembled at `stage_planner.py:3097-3126`, rendered at `generator.py:6034`), with parallel routing in `conjunct_scope` / `classify_regroup_filter` (`regroup_planner.py:318/347`), `_split_partitioned_filter_conjuncts` → `outer_where_filter_ids`, and `_windowed_phase` POST reclassification. Guards: cross-model partition_by filter (`stage_planner.py:404-409`, DEV-1824), mixed-grain OR (`regroup_planner.py:331-343`), mixed computed-dim-aggregate predicate (`regroup_planner.py:352-358`, DEV-1825).
- Order: targets already compile through hidden `ValueSlot`s (`order_key_remap` MIN/MAX wraps at `stage_planner.py:3048-3061`, order-only composite slots at `planning.py:552-560`, `HOST_BASE_HIDDEN` trim at `generator.py:3250-3275`); plan-side `_classify_order_scope` (`stage_planner.py:3974`) stamps `OrderEntry.scope`; emission goes through the one resolver (`slayer/sql/render/order_terms.py`, DEV-1747).
- Cross-root propagation (DEV-1840): `_conjunct_disposition` / semi-join planning in `stage_planner.py`, `filter_reachability.py`, `SemiJoinFilter` — behavior frozen by axiom D.

Architecture constraints: ratchet forbids new `ignore_imports` edges (no new `sql → engine` module pair); system principles 6/8/13 and sql principles 1/5/9 apply; `queries` cross-cutting spec already mapped in `architecture/index.yaml` (no index change).

## Goals / Non-Goals

**Goals:** one resolve-then-type pass for filter conjuncts and order targets; hidden-slot compilation through the one render pipeline; emission-side lowering with byte-identical golden SQL for currently-legal shapes; retire the three filter-lag guards.

**Non-Goals:** new measure shapes (DEV-1841/1847/1859), population semantics (DEV-1866), traversal (DEV-1853), `slayer/ir` extraction (DEV-1872), any DEV-1840 disposition change, `Grain` (DEV-1867), Mode-A model-filter semantics.

## Decisions

1. **Typing rule — resolve then "contains no aggregates", never synthesizing.** Field iff the resolved expression is aggregate-free AND legal as a projected field (same validation a computed dimension passes — covers unmaterializable targets like bare literals); resolution uses existing bindings only, with computed-dim-aggregate refs canonicalized to their row-attached slots (today's `RegroupPlaceholderRegistry` substitution), so those count as aggregate-free. Else measure iff legal as a declared measure (combined-consumer keys, unsafe-input fail-closed, etc.). Else the typing error naming both failures. Alternative rejected: field typing that synthesizes attachments — golden-divergent and lets a filter change surviving cells' values, violating the stratification corollary.

2. **Plan representation (Option A).** Each conjunct compiles to a hidden `ValueSlot`; `PlannedQuery.masks: list[MaskEntry]` (stable slot id, field/measure typing, stratum). Retired: `FilterPhase`, `filters_by_phase`, `outer_where_filter_ids`, `combined_filter_indices`, `conjunct_scope`, `classify_regroup_filter`, `_split_partitioned_filter_conjuncts`, `_partitioned_conjunct_scope`. Alternative rejected: keeping `filters_by_phase` as pre-lowered plan output — retains the position-specific representation the issue exists to kill.

3. **Mode-A model filters keep a dedicated text carrier** (they are model definition, not query positions; not `ValueKey`-representable). Same base-WHERE emission via `_enter_mode_a_predicate`, byte-identical.

4. **Reachability plumbing migrates, logic doesn't.** `filter_reachability.py` and semi-join disposition keep their decision rules; inputs move from `filters_by_phase`/filter ids to stratum-0 mask entries keyed by slot id.

5. **Lowering lives in `slayer/sql/generator.py`**, clearly sectioned (a new sql module importing `slayer/engine/planned` would grow the ratchet baseline; split lands with DEV-1872). Rules reproduce today's placement byte-for-byte: stratum-0 field mask → inline base WHERE + unchanged producer inheritance; attached-ref field mask → inline at the re-aggregation point; measure mask → HAVING (plain local) / outer WHERE (attached, combined, cross-model) / outer wrapper (windowed/POST). Fallback for any unlowered shape: materialize the hidden boolean column and filter on it — correctness never depends on a lowering rule existing.

6. **Order unification (all four).** Same typing pass; direction-dependent MIN/MAX desugar stays as pre-typing order-position sugar; the host-grain wrap keeps its plan shape (acceptance parity is one-directional); `OrderEntry` drops `scope` and `_classify_order_scope` relocates into the lowering section, feeding `resolve_order_term`/`OrderEnv` unchanged.

7. **Three-valued masking.** A mask keeps rows where the predicate IS TRUE; FALSE and NULL drop — standard WHERE semantics, asserted by the parity tests.

## Risks / Trade-offs

- [Golden drift from relocated placement logic] → lowering rules keyed to reproduce existing emission; full golden suites run unmodified; any divergence individually approved or fixed.
- [DEV-1840 regression via reachability re-plumbing] → dispositions logic untouched; plan-level test pins `semi_join_filters` identical on the dev1840 fixture shapes; existing dev1840 golden+executed suites stay authoritative.
- [Newly-legal shapes hit untested producer paths] → executed-value tests (SQLite + DuckDB) for both shapes; the materialize-the-boolean fallback bounds the failure mode to correct-but-unlowered SQL.
- [Large mechanical diff in `stage_planner`/`generator`] → staged tasks with the full non-integration suite green at each stage; `SLAYER_VALIDATE_SCOPES=1` backstop.

## Migration Plan

Pure internal representation change behind byte-identical SQL; no storage, API, or model migrations. `test_dev1747_order_entry.py` and siblings asserting `OrderEntry.scope` update mechanically (approved). Stale `docs/architecture/planning.md` / `sql-generation.md` references updated only if those files still exist at merge time (DEV-1870 dissolves them).
