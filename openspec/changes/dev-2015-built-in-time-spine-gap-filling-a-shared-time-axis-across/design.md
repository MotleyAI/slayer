## Context

See proposal.md › Why. Verified on origin/main: a hand-made `calendar` model joined from each fact by date and used as the population already gives exact, filled shared-axis results (population, attribution, join-back and empty values all work); three gaps showed up: a region × month grid infers `orders` / `returns` (a tie) instead of a grid, a fact filter restricts the calendar's *instants* by association (months vanish, other facts lose values), and a cross-model trailing window is NULL at a month with no home rows. Adding calendar joins to every fact also made existing cross-fact queries fail with "ambiguous route" (the calendar becomes a second route between facts). There is no series-generation primitive in `slayer/sql`; `TimeGranularity` is a closed enum recognised at query construction (`core/query.py` functional form) and reconstructed at render sites; population inference is single-dataset (`engine/population.py`). DEV-1999 (time points, frame bounds, engine clock, typed `whole_periods_only`) lands first.

Normative: semantics Axioms 1–14 (new Axiom 15), system P6/P8/P10/P13/P15, engine P3/P5/P10, sql P1/P2/P7/P10/P12, core P1–P3, ir P1/P4.

## Goals / Non-Goals

**Goals:** reduce the spine to existing concepts — a model, to-one joins, a population — so every behaviour follows from the axioms; one granularity concept (built-in or datasource-defined) everywhere.

**Non-Goals:** a declared spine table; a per-measure axis override; a `fill` flag or fill-value parameter; retail / irregular calendars (DEV-2020); time zones (DEV-2000); comparison-suite probes (DEV-2019); dbt/Cube custom-granularity import (DEV-1938); changing ordinary (non-spine) routing tie-breaks.

## Decisions

**D1 Spine = a virtual model of all instants.** The bundle builder synthesises `time_spine` per datasource (one PK `time` column `timestamp`) and, for each dataset with an axis, a many-to-one edge `axis = timestamp`. Instants, not a day grain: equality with any DATE/TIMESTAMP axis is then plain to-one, sub-day custom grains need nothing finer, and "the spine's grain" is not a concept. Alternative (a day-grain calendar): needs a truncation in the join and breaks sub-day buckets.

**D2 Lowering: logical instants, physical buckets.** Grouping instants by a granularity yields exactly the buckets overlapping the bounds, so the spine emits a bucket series, never instants (Law 6 lowering soundness). A fact row attributes to `bucket(axis)`, equal to its spine instant's bucket; bucket comparands pass through `bucket_comparand` so DATE/TIMESTAMP/BigQuery types align.

**D3 Series generation.** n (an over-estimate) is computed in Python from the resolved literal bounds; SQL emits `i` in `0..n-1` via one new dialect hook (integer sequence: Postgres `generate_series`, DuckDB `range`, SQLite recursive CTE, MySQL recursive CTE with the `SET_VAR(cte_max_recursion_depth=…)` hint, ClickHouse `numbers`, T-SQL `GENERATE_SERIES` — 2022+ already required by `DATETRUNC` — BigQuery `UNNEST(GENERATE_ARRAY)`, Snowflake `GENERATOR` + `ROW_NUMBER`, Tier-2 recursive CTE), then `bucket_i = date_add(bucket(lower), i × multiple, base)` through the existing `build_date_add` / bucket hooks, filtered to `bucket_start < upper_exclusive AND bucket_end > lower`. Bucket arithmetic stays in SQL so it cannot disagree with the facts' bucketing; Python only sizes the sequence. Alternative (a Python-generated `VALUES` list): exact-agreement risk between Python and every dialect's truncation, and large SQL text for fine grains.

**D4 Product population.** `PlannedQuery` gains a spine factor (granularities + resolved bounds) beside the P root; the FROM of the population is `series CROSS JOIN (SELECT DISTINCT <P grain> FROM P WHERE <P filters>)` (P = unit → the series alone). Every producer keeps its own home, filters and null-safe complete-grain LEFT JOIN (sql P7/P10), so filter disposition per home is exactly today's; only P's projection sets the non-time coordinates. Result keys: the spine factor keys `time_spine.<col>`, P's fields and every measure key from P's root (from `time_spine` when P is the unit).

**D5 Three uses of the spine bounds.** (a) bucket existence (D3 overlap); (b) a row filter on each fact's axis in ordinary producers (the association of the bound through the to-one axis edge, lowered inline); (c) frame bounds — stripped from window and `time_shift` source frames by the existing frame-bound handling. Bound recognition is DEV-1999's frame-bound predicate (`core/time_bounds.py`), never a second recogniser. The implied upper bound is the `this <finest spine granularity>` time point from the engine clock (cache-stable within the bucket).

**D6 Spine routing.** Spine routes are found over the same executable, oriented, provably-to-one hop tokens the binding walker uses (named parallel edges, shadowing), BFS by hop count; ≥2 distinct shortest routes → the typed ambiguous-route error with canonical paths. Every other router (dimension routing, association, population probes) excludes spine edges except as a path's first or last hop — enforced in one place (the join graph's traversal), not per caller.

**D7 Granularity as a typed reference.** `TimeGranularity` becomes a union: built-in member or `GranularityRef(name)`; the bundle resolves refs against `DatasourceConfig.granularities` into a resolved definition `{base, multiple, origin}` (built-ins resolve to `multiple=1`, natural origin), and everything downstream (keys, stage schemas, `Column.granularity`, transform steps, IR, dialect bucketing) consumes the resolved form. The construction-time functional-form rewrite keeps built-in callees; a non-built-in `name(col)` stays an expression call and the binder resolves a callee naming a granularity as a `TimeTruncKey` / time dimension (engine P2: parser stays pure syntax; P3: resolution from the bundle).

**D8 Custom bucketing.** `bucket(ts) = date_add(origin, multiple × floor_div(k, multiple), base)` with `k = date_diff(base, origin, ts)` corrected by one when `date_add(origin, k, base) > ts` (boundary-count vs full-period semantics; negative k floors toward −∞), reusing DEV-1737's exact `date_diff` / `date_add` dialect hooks. Built-ins keep their native truncation.

**D9 Nesting.** A pure function over two resolved definitions implementing the spec's arithmetic rule; `enums.nests_into` becomes its built-in special case, so re-bucketing, `whole_periods_only` and `Column.granularity` all consult one relation.

**D10 Windowed producer at population cells.** The windowed producer stays rooted at its home, but its evaluation endpoints are the population's distinct cells at the producer's grain (from the host), range-joined to the home rows `[bucket_end − window, bucket_end)` with home row filters applied; the result attaches by the usual null-safe grain join. Today's endpoints are the home's own buckets, which is the bug. Applies to local and cross-model homes alike.

**D11 Checker placement.** Every new user-facing error (missing lower bound, spine aggregation, plain spine dimension, non-bound spine filter, ambiguous spine route, reserved-name clash, unknown granularity, re-bucketing through the spine) raises in the checker as a `QueryTypeError` subclass (engine P9); datasource/model save-time validation errors are typed `SlayerError`s.

**D12 Approved arc42 edits (user OK 2026-09-30; any other wording needs a fresh OK).** `architecture/semantics.arc42.md` gains:

> 15. **Time spine**: the spine is the dataset of all instants, keyed by its instant. A dataset's axis — its declared default time column, else its sole one — joins it to-one; a dataset reaches the spine by its nearest axis on a to-one chain, equal distances failing loudly, and a path may begin or end at the spine but never cross it. The spine has no countable rows. A query with spine dimensions has population `spine × P`, P inferred per Axiom 12 from its other dimensions and field filters, and must bound the spine from below. [enforced: test:tests/test_dev2015_time_spine.py]

and Axiom 12's "inferred from dimensions and row-level filters only, never measures" gains "; spine dimensions factor out (Axiom 15)". Land them with the implementation, when the enforcing test file exists.

## Risks / Trade-offs

- [Dense grids can be huge (`customer_id` × day over years)] → documented; the grid is the output, not an intermediate; fact rows never meet the cross join.
- [Auto-wiring could change existing queries] → the sink rule lives in the graph traversal itself; a regression test runs the existing cross-fact queries with spine edges present.
- [Sole-temporal-column inference wires dimension tables] → harmless by the nearest-axis rule; adding a second temporal column un-wires a model (documented).
- [Custom bucketing exactness across eight dialects] → reuse of DEV-1737's parity-tested hooks; executed parity tests on four server dialects; golden SQL for the rest.
- [Python-sized sequence too small] → n is sized from the resolved literal bounds as exact-or-over (never under), and the SQL overlap filter trims the excess, so no bucket is ever silently dropped.
- [Opening `TimeGranularity` touches many sites] → the enum-construction sites are enumerated in tasks and each gets a custom-granularity test.

## Migration Plan

Additive: `DatasourceConfig.granularities` is optional (no migration). Error-timing change for unknown `name(col)` callees (construction → binding) is flagged BREAKING in the proposal. Rollback = revert; no persisted data depends on the spine.
