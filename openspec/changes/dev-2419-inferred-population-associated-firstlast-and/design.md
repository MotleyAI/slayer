## Context

See proposal.md — Why. Two deciders disagree today:

- **Row supply** (`slayer/engine/population/row_supply.py` `_operand`) decides by aggregation kind whether an
  aggregate associates: `window=` and `first` / `last` (`_BROADCAST_ONLY`) are classified coarse, grain = the
  determined dimensions, so their rows come from the dimension anchor. It also reads `to_many_handling` raw
  (`== "broadcast"`) in `classify_row_sources` and `_spine_witnesses`.
- **Binding / compile** (`slayer/engine/compile/stages.py`) associates every aggregation by the mode alone
  (`ModePolicy.associates`, the one reader of the raw field; semantics Axiom 8, DEV-1914).

And the windowed producer (`slayer/sql/generator.py`: the windowed branch of the producer body builds `_base` from
`_build_windowed_grain_base`, or `_window_endpoints_base` when the kernel carries endpoints, then
`_render_window_metric_cte_from_planned` range-joins `_base LEFT JOIN _src`) evaluates at the grain cells the
**host's** rows reach (`FROM orders LEFT JOIN customers` in the probe), while the plain associated producer is
home-rooted. A cell only the home's rows or a sibling supplies is then absent from the producer and reads the empty
value through `COALESCE`.

Probes (this branch, pre-change, SQLite and DuckDB): dropping the `window=` exclusion alone makes every windowed
metric supply its unwindowed twin's rows, and the 2433 tests touching windows, the row supply, the spine and the laws
still pass; the host-derived base then shows wrong values in the NULL tuple of a spine query with no siblings, and
already does on `main` beside a plain sibling (`count(customers.*)` 1 vs windowed 0 in the NULL-status cells, 1 under
`population: customers`).

Applicable principles: semantics Axiom 8 (every aggregation associates, a windowed one per bucket), Axiom 2.9 (home
is not population), Axioms 12.3–12.4 (supply), Axiom 15 (spine), Law 3 (compositionality), Law 7 (measure
monotonicity), Law 8 (home-row preservation); `queries/attribution-modes` › "The buckets are the population's cells".
`system.arc42.md` §3: one reader of a raw field. No LikeC4 or import change.

## Goals / Non-Goals

**Goals:**
- One association decision for the row supply and the binder: the mode.
- One evaluation domain for a windowed producer: the stage's rows, so its value is a function of the cell.
- The metrics-only rule stated positively in Axiom 12.4 and the spec.

**Non-Goals:**
- `first` / `last` and `_BROADCAST_ONLY` — DEV-2436 (PR #665) removes them; this change leaves that line as is.
- The spine's in-frame witness rule (`queries/time-spine` › A windowed measure supplies the groups its window
  reaches) is unchanged; an associated windowed metric now follows it like a determined one.
- Broadcast and error modes keep today's supply and refusals.

## Decisions

1. **Windowed aggregates supply their unwindowed twin's cells** (issue acceptance; Axiom 8; Law 8): `_operand` no
   longer excludes `window=`. On the spine the full-grain branch keeps the windowed witness (`_witness` already
   carries the window). Alternative — cells only where the frame holds a home row — rejected: it diverges from the
   determined windowed rule off the spine and breaks Law 8 for the NULL tuple. (default)
2. **Windowed values on cells the host does not reach are fixed here**, not filed or xfailed: the row-supply fix
   alone turns a missing row into a visible wrong value. (user)
3. **The windowed producer's domain is the stage's rows** — after field filters (and model filters / RLS), before
   measure-typed filters, order, limit and offset — never the host's grain cells. Home-rooted cells alone were
   rejected: a bucket a sibling supplies (a frame-only bucket) would lose its window value. A domain taken after
   measure-typed filters or order was rejected: a windowed aggregate in a filter or order key would shrink or reorder
   its own domain (position parity, Law 4). (default; codex)
4. **The row supply reads the mode through `ModePolicy`** — no raw `to_many_handling` comparison and no aggregation
   kind named in `_operand`. (default)
5. **`first` / `last` stay out** (scope split to DEV-2436). (default)
6. **Branch from `main`; reconcile with DEV-2436 at merge.** Both changes MODIFY the same requirement. If DEV-2436
   lands first, merge `origin/main` and rebuild this delta on its text; its "ranked-supply seed" and this change's
   "association seed" are the same data — keep one paragraph under one name and point every scenario at it. (default)
7. **Axiom 12.4 wording** (user, approved wording): "**12.4 Inferred.** An omitted population is the **row supply**:
   the union of the query's metrics' supplies — `A`'s cells with no metrics, the unit with no dimensions; an aggregate
   in a filter, order key or computed dimension reads those cells and supplies none. Field filters restrict each
   supply before it is taken (Axiom 14) and never choose one; a query needing an undefined `A` fails closed."
8. **Spec sentence for the metrics-only rule** (user, approved wording, after Codex): "The query's metrics SHALL
   define the row supply: an aggregate in a filter, an order entry or a computed dimension SHALL read the rows it
   defines — the dimension anchor's cells when there are no metrics — and SHALL add none." (codex)
9. **Law harness guards the class**: `tests/_law_harness.py` `ROOTLESS_METRICS` gains an associated and a determined
   windowed operand, so Laws 7–8 and the root-less row laws cover them. (default)
10. **Test matrix spells star counts per home** (`count(<home>.*)`): an unowned `count(*)` has no home in a root-less
    query. (codex)
11. **Sibling independence tested directly**: each windowed metric alone vs beside a supplier from another home,
    including one contributing the NULL tuple, equal on every shared cell (Law 3). (codex)
12. **Filters on the new domain tested**: a home field filter, a cross-dataset filter on the time axis's branch (the
    same related row places the entity in the frame), a model filter and an RLS policy. (codex)
13. **Codex finding rejected — spine oracle**: Codex expected Eve in May's and Fay in June's frame. The frame is
    `[bucket_end − window, bucket_end)` (`aggregations/trailing-window`): May's 60-day frame starts 2024-04-02 and
    June's 2024-05-02, so NULL reads 1 in April and May and 0 in June, as `population: customers` returns. The scenario
    states the frame. rejected (codex)
14. **Codex finding rejected — Axiom 12.4 approval**: the wording was approved by the user during planning (entry 7).
    rejected (codex)
15. **A windowed aggregate inside a transform's operand takes its own cells** — those of the same aggregate without
    `window=` — for every transform (`time_shift`, `change`, `lag`, `lead`, `cumsum`, …), never the cells the hosting
    dataset or the stage's rows reach (semantics Axiom 11.6; `queries/transforms` › Transforms evaluate over their
    operand's domain). Found in review round 1: with Decision 1 in place, `time_shift` / `change` over a windowed
    aggregate raise an internal error in an inferred query (the shifted producer nests the windowed answer yet still
    carries a trailing-window kernel), and `lag` / `cumsum` read NULL beside a sibling in cells where they read a value
    alone. Rejected: the shifted producer reading the stage's rows (the query's rows become the domain, which Axiom
    11.6 forbids; values would follow siblings and a named population); fixing the crash alone and filing the rest
    (ships a visible wrong value, against Decision 2). Decision 3 is unchanged: it fixes where a windowed *value* is
    evaluated for the query's rows; this fixes which cells form a transform's series. (user; codex)
16. **A bucket only a sibling supplies stays a gap for a transform over a windowed aggregate**, as for a plain one
    (Axiom 11.3): its row shows the windowed value, `time_shift` from the next bucket reads NULL; a spine query fills
    it. (default)

## Risks / Trade-offs

- [Re-rooting the transform producer's windowed operand at its home touches the shifted answer, grain keys, join pairs,
  frame-free filters and `shifted_spine_bounds`] → probe first whether feeding the windowed producer the unwindowed
  twin's home-rooted cells as its cells relation (the hook Decision 3 added) gives the same result without re-rooting
  every key; the tests of tasks §3 decide.

- [The stage's row cells are not a relation the producer body can see today] → the inferred stage already builds its
  keys (`_keys` / `_base` in the emitted statement); the producer reads that relation, or the same row-supply query
  rendered for it, never a second derivation. Lowering soundness (Law 6) forbids a value change from the reuse.
- [Re-blessed SQL goldens] → only goldens whose statement carries a windowed producer change; each diff is reviewed as
  a `_base` swap with unchanged values.
- [Merge conflict with DEV-2436 on `_operand` and the population requirement] → Decision 6.
