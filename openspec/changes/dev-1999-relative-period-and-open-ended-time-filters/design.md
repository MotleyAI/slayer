## Context

See proposal.md — Why. Today a `date_range` binds to a `BetweenKey` rendered as inclusive `BETWEEN`; user comparators bind to `ArithmeticKey` over a bare `LiteralKey(str)`; `whole_periods_only` is a text rewrite in `SlayerQuery.snap_to_whole_periods` using `datetime.date.today()`; `gran(col)` exists only as a whole-entry dimension rewrite and an order key. Frame-bound analysis (`slayer/core/time_bounds.py`) keeps literal relational bounds on a time dimension's raw column out of windowed/shifted CTEs. Binding cannot import the checker (engine P9 / model law: `binding → syntax` only; `bind_inputs → elaborate_env` is permitted), and every user-facing type error must raise in the checker.

## Goals / Non-Goals

**Goals:** one canonical time-point meaning in every position and on every dialect; the whole bound class lowered once, typed, never by text; frame-bound status inherited, not special-cased.

**Non-Goals:** time zones and "now" in a query zone (DEV-2000, recorded there); SQL-side `current_date` and DEV-1737 date functions; ordering by an unprojected `gran(col)`; arc42 or LikeC4 edits (none needed).

## Decisions

1. **"Now" resolves in Python** (not SQL `CURRENT_DATE`, not a DB round trip). The engine takes an injectable `clock` (default host-local `datetime.now`); `execute` reads it once, before any time rewrite, into `ResolvedSourceBundle.now`, and every derived bundle (reroot, named stages, splices, dry-run, cache evict/refresh re-prepare) carries that same value. Rationale: resolved bounds are literals, so the query cache (keyed by SQL text) never serves a stale relative range, frame-bound analysis needs no new shape, and tests pin the clock. Alternative SQL-side resolution: per-dialect date arithmetic, stale cache hits, non-literal frame bounds — rejected.

2. **Pure time-point module.** `slayer/core/time_points.py` (dependency-free, beside `time_bounds.py`) parses a string into `Instant | Period` (period literals, the closed relative grammar) and resolves a relative period against a `now`; `start`/`next_start`/`floor_g` arithmetic lives here and reuses `TimeGranularity.period_start`. Naive wall-clock arithmetic.

3. **Transient `TimePointCmpKey` + one checker-owned resolution pass.** The binder, which cannot know operand types, emits `TimePointCmpKey(op, operand, point_text, literal_on_left)` for any comparison or single-string `in`/`not in` whose string literal parses as a time point, and for each `date_range` (as one key per range with optional lower/upper). A pass in `bind_inputs` walks every bound expression (filters, measures, dimensions, order, date ranges) and calls a checker function in `elaborate_env` that types the operand via one recursive `temporal_type(key, scope, bundle) -> DATE | TIMESTAMP | None`, raises the typed errors (`QueryTypeError` subclasses), and rewrites the key into its lowering — or back to a plain `LiteralKey` comparison when the operand is not temporal and the literal is a plain date string. Planning fails closed if a `TimePointCmpKey` reaches compile. Alternative (binder resolves in place): needs `binding → elaborate_env` or duplicated typing, and raises outside the checker — rejected. Every total walker gets the new kind (core P3); after the pass it exists only as a raise tail.

4. **`TemporalLiteralKey(value, kind=date|timestamp)` for resolved bounds.** Values are canonical (`YYYY-MM-DD`; `YYYY-MM-DD HH:MM:SS[.f][offset]`). `time_bounds.is_temporal_literal` accepts it alongside `LiteralKey(str)`. The renderer hands a comparison with a timestamp-kind literal to one dialect hook: SQLite renders `DATETIME(x) op DATETIME('…')` (the `frame_time_operand` precedent), making results independent of `T` vs space storage; other dialects render the plain literal, which each coerces (BigQuery's documented literal coercion included). Date-kind comparisons render plainly everywhere and stay sargable. Alternative (plain `LiteralKey`): wrong rows for `T`-stored SQLite timestamps at sub-day bounds — rejected.

5. **Lowering shapes.** Relational comparisons over `TemporalLiteralKey`, combined with `and` / `or` (table in `specs/queries/time-points`). A `date_range` stays ONE bound filter (an `and`, or a single comparison when one-sided), so `n_date_range` / `n_date_range_masks` bookkeeping is unchanged. `BetweenKey` loses its last producer and is deleted with all its dispatch arms.

6. **`gran(col)` binds to `TimeTruncKey` in `_bind`** for every position; `row_expr.py` gains a `TimeTruncKey` arm and every dispatch site is audited. A comparison of `TimeTruncKey(c, g)` with a time point lowers to the exact bound on `c`: lower `>= P` → `c >= ceil_g(start(P))`, `> P` → `c >= ceil_g(next_start(P))`, `< P` → `c < ceil_g(start(P))`, `<= P` → `c < ceil_g(next_start(P))` (`ceil_g(t)` = `t` when aligned, else the next bucket start); `=`/`!=` compose these. Non-literal uses keep the trunc emission.

7. **Parser.** `in` / `not in` with a single string RHS parses to a distinct node (not `TupleLit`), so tuple membership keeps its meaning.

8. **`whole_periods_only` is a post-resolution typed pass.** After resolution, per raw column carrying time dimensions (model or stage), the frame-bound conjuncts (per `time_bounds.is_frame_bound`) are rewritten: lower `L` → `>= min_g floor_g(L)`; upper (exclusive `U`, or `now` when absent, clamped `min(U, now)`) → `< min_g floor_g(U)`, over the granularities of that column's time dimensions; an absent upper bound adds one conjunct. Non-nesting granularity pairs emit a structured warning. `snap_to_whole_periods` and its textual check are deleted.

9. **Construction vs planning validation.** `TimeDimension.date_range: str | list[str | None] | None` — a before-validator coerces a string to a one-element list; construction rejects `[]`, 3+, `[None, None]` and syntactically invalid elements. Type-dependent checks run in the checker pass. `MALFORMED_DATE_RANGE` is removed from `normalization.py`.

10. **Facade instant-ization on the sqlglot AST** (`slayer/facade/translator.py`): date-only literals, including typed `DATE` / `TIMESTAMP` literals, in lifted `BETWEEN` bounds and verbatim comparators become midnight instants.

## Risks / Trade-offs

- [Behaviour change on TIMESTAMP columns for date-only bounds and `whole_periods_only`] → documented in `docs/concepts/time.md` and release notes; goldens re-blessed; spec deltas state the new rows.
- [New key kinds must be handled by every total walker] → the existing totality tests (`test_dev1827`, `test_dev1842`, `test_dev1747`, `test_dev1825`, `test_dev1745_reachability`) get samples for both kinds; the fail-closed compile assertion catches an unresolved `TimePointCmpKey`.
- [SQLite sub-day comparisons are non-sargable] → only timestamp-kind literals are wrapped; day-aligned bounds stay plain.
- [Temporal typing gaps for complex expressions] → unknown type means non-temporal (today's plain-literal meaning) except relative tokens / single-string `in`, which fail closed.
- [Host date ≠ database date] → deferred to DEV-2000 (recorded on that issue).
