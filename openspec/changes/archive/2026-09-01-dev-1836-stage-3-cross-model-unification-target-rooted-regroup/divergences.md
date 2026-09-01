# DEV-1836 — divergence ledger (design D10)

Running enumeration of the executed-value and error-surface flips the TDD suite
pins, with today's values (measured on `tests/_dev1836_fixtures.py`, SQLite),
for per-class approval. Classes per design D10: (a) byte-identical, (b)
SQL-shape only, (c) deliberate value changes, (d) new errors.

## Class (c) — value changes pinned by the TDD suite

| Shape (fixture query) | Today | Pinned (new) |
|---|---|---|
| `customers.spend:sum` by `status` (host dim) | ok=370, new=140 — joins through and fans (sum 510 from a 350 pool) | broadcast 350/350 + `broadcast` warning |
| `customers.spend:sum` by `customers.segments.label` (unproven hop) | joins through the undeclared hop | broadcast 350 per row + warning |
| `customers.spend:sum` by `customers.tier` + `status` | per-cell via join | exact per tier, broadcast across status |
| `customers.spend:sum` by `status`, filter `customers.tier = 'gold'` | ok=160, new=100 (c1's 100 counted in both rows — 260 from a 160 pool) | 160 broadcast on both rows |
| `customers.spend:sum` by `customers.tier`, filter `channel = 'app'` | inherited through the reverse hop (fan risk: c3's two app orders) | filter dropped from producer + warning; cm = per-tier 160 |
| Aggregate-phase filter `customers.spend:sum > 100` by `customers.tier` | rows preserved; cm NULL for failing groups (bronze/NULL kept) | rows restricted to gold/silver — uniform with local aggregate filters |
| Filter-only `customers.spend:sum > 100` (not selected) | silently inert (all rows) | restricts rows like a local aggregate filter |
| `customers.regions.pop:sum` by `customers.tier` (intermediate hop) | raises NotImplementedError | executes; broadcast 300 + warning |

### Scope narrowing (post-reconciliation): D3 host-hop/sibling reroot rules

The class-(c) flips above apply only where the reverse/sibling hop is NOT
provably to-one. When the producer's root declares a safe path back to the
host (or directly to a host-sibling model) — every hop provably many-to-one —
the grain member re-roots (host-local → prepend host hop; sibling → the
via-host path when provable — the instance-exact route, binding the HOST's
join instance — falling back to the root's own join only when via-host is
unprovable) and the value stays EXACT, and reachable filters inherit instead
of dropping. Found via the Q9 integration regression
(`test_cross_model_measure_with_target_join_filters`); pinned there plus the
un-xfailed `test_rerooted_local_filter_remapped_to_source` and the rewritten
`test_rerooted_cte_includes_reachable_and_sibling_filters`.

## Class (d) — new errors (today's silently-computed values shown)

| Shape | Today | Pinned (new) |
|---|---|---|
| `customers.seg_label:count` by `status` (Mode-A source over unproven hop) | ok=4, new=1 | hard error naming the hop + remedy |
| `customers.vip_spend:sum` by `status` (`Column.filter` over unproven hop) | ok=220, new=100 — c3's spend double-counted through its two ok orders | hard error |
| `customers.spend:sum(partition_by=status)` (explicit key over 1:N) | ok=370, new=140 — same fan | hard error naming key + remedy |
| `customers.spend:last(ordered_at)` (ranking key over 1:N) | ranks by host time through the join | hard error |
| `customers.spend:sum(window='1y')` with unattributable TD | blanket DEV-1836 guard | specific error naming the TD |
| strict mode: any implicit broadcast / dropped producer filter | n/a (no `strict` field) | error naming metric/filter + remedy |

## Behavior-preservation pins (would otherwise regress)

- A plain cross-model measure renders inside a CTE body today
  (`generate_from_planned(..., as_cte_body=True)`); after migrating onto the
  combined-attach primitive it must NOT fall into the DEV-1838 CTE-body arm
  (`tests/test_dev1836_total_routing.py`).
- Aggregate-phase HAVING inside the producer keeps metric values (gold=160,
  silver=150) — only the row-restriction semantics change (above).
- Guard message joining the residue scan: `stage_planner.py:372` "Windowed
  cross-model aggregates … (DEV-1836)".

## Task 1.13 — fixture audit result

Shared fixture modules (`tests/_*.py`): 31 join edges, 25 structurally proven
(id PKs already declared), 0 needing PK additions — prior goldens untouched.
Genuinely-1:N edges (class-(c) watch candidates at implementation time, when
the safe-grain flip lands):

- `_dev1747_fixtures` / `_dev1748_fixtures`: `orders → order_tags on [id, order_id]`
- `_dev1750_fixtures`: `orders → line_items on [id, order_id]`
- `_dev1836_fixtures`: `customers → orders` (declared `one_to_many` on purpose)
  and `customers → segments` (deliberately unproven)

Inline per-test models are not audited here: their divergences surface as
golden re-blessing batches (class (b)/(c)) during implementation, per D10.

## Step-5 Codex review of the tests — triage record

Accepted (folded into the suite): fan-out oracle 470→510; D7 sentinel via a
blinded `combined_partitioned_aggregates` seam (today it trips the
`partition_by not a query dimension` backstop — the pin is that blinded
discovery must NEVER plan silently, surviving the classifier deletion);
remedy-word asserts on unproven-hop errors; rank×cm + mixed×cm cells;
nested-attach D8 key-shape assert; nested-only dropped-filter traversal test
(pop-band dim, regions-rooted producer); keyless-attach row-count control;
broadcast reason classes (unreachable vs hop) distinguished; "detect" remedy;
executed values through a stamped query-backed model's grain; duplicate-row
guards in windowed dicts; NULL-position-agnostic order-only pin.

Rejected, with rationale:
- "generic kwargs unsafe input uncovered" — SLayer has no column-valued
  aggregation kwarg surface beyond `partition_by`/`window`/the ranked time
  arg, all covered.
- "`NotImplementedError` is an internal error class" — it is the repo's
  established public not-yet-supported contract (every pinned guard arm).
- "CTE-body guard scenario uncovered" — pinned by
  `tests/test_dev1837_guards.py` (ARM_ROW_CTE_BODY / ARM_COMBINED_CTE_BODY).
- "safe goldens hold uncovered" — that scenario IS the existing golden
  corpus (class (a)/(b) tripwire per D10), re-blessed per approved batch.
- "expression over two different partition sets uncovered" — an unchanged
  scenario of the MODIFIED requirement, already covered by the existing
  dev1740/1824 suites.

## Spec deltas re-baselined while writing the tests

- `computed-dimensions`: the windowed/ranked coexistence deferral scenario was
  stale (lifted by DEV-1835 stage 2) — replaced with the CTE-body deferral +
  the cross-model CTE-body preservation scenario.
- `cross-model-aggregates`: "aggregate-phase routing unchanged" made explicit —
  uniform row-restricting semantics with local aggregate filters (the flip is
  enumerated above).
