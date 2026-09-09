# DEV-1853 divergence ledger (D10 protocol)

Every behavior flip is enumerated here for approval; provably-safe shapes stay
byte-identical (unnamed paths — named-edge paths are new surface). Newly
reachable unattributable dimensions are described in the DEV-1841 mode
vocabulary (broadcast = default mode).

## Class (b) — SQL-shape (goldens re-blessed)

- Previously-mirrored INNER traversals: byte-identical on unnamed paths —
  verified by `test_dev1853_mirror_parity.py` (`sql_f == sql_r` across
  LEFT/INNER × forward/reverse declaration, four query shapes) and by the
  dev1836/dev1842 suites passing unchanged after the mirror halves were
  stripped from their fixtures. Tripwire held: no golden for a plain forward
  path changed.
- Semi-join EXISTS correlation over a formerly-declared reverse edge: the
  computed inverted edge yields the same oriented pairs, so the emitted EXISTS
  is unchanged (dev1840 `exists/declared_reverse` goldens; fixture now
  forward-only).

## Class (c) — value changes

- Broadcast → exact attribution where an inverted declared `one_to_many`
  yields a provable `many_to_one` hop:
  `test_dev1853_traversal_execution.py::TestOrientedCardinalityValues::
  test_inverted_declared_one_to_many_gives_exact_attribution` — chain fixture,
  only stored edge `customers → orders (one_to_many)`, query
  `customers ⊳ dims=[tier], measures=[orders.amount:sum]`: today broadcast
  total 100.0 repeated per tier → pinned exact {gold: 30.0, silver: 30.0},
  no broadcast warning.
- Dimension-level cases over the inverted orientation (all pinned by executed
  values in `test_dev1853_traversal_execution.py` on the chain dataset):
  selected reverse dims (LEFT keeps Cara with NULL status; INNER drops her),
  reverse-LEFT null buckets (`CHAIN_REVERSE_DIMS_LEFT`), reverse-INNER row
  removal (`CHAIN_REVERSE_DIMS_INNER`), grain-safe local measure grouped by a
  reverse fan-out dim (`CHAIN_SPEND_BY_REVERSE_STATUS` — never
  join-multiplied), multi-hop reverse (`CHAIN_MULTIHOP`), and per-backend
  execution on sqlite/duckdb/postgres
  (`tests/integration/test_dev1853_reverse_hop.py`).
- Drop+warn → semi-join pushdown where the correlation path resolves over an
  inverted unambiguous edge (dev1747 family): `test_dev1747_reroot_filter_routing.py::
  test_host_local_filter_pushes_down_by_semi_join` (was
  `…_is_dropped_and_warned`) and `test_an_excluded_filter_still_narrows_the_host`
  — the producer CTE gains `WHERE EXISTS(SELECT 1 FROM orders WHERE
  customers.id = orders.customer_id AND …)` and the warning disappears;
  goldens `reroot/host_local_filter::*` and `reroot/unreachable_filter::*`
  (5 dialects each) re-blessed with `_base` byte-identical. A conjunct whose
  cross-path refs span two branches with no root-local ref pushes as a
  multi-branch EXISTS; only root-local + cross mixing under OR/NOT still drops.

- Facade browse-mode `SELECT *` scope pinned to row-preserving paths: catalog
  dims across a fan-out hop (declared 1:N, inverted m2o/unknown, or any path
  through one) carry `row_preserving=False` and are excluded from star
  expansion — unbounded expansion across the now-reachable inverted edges
  join-multiplied the root grain (jaffle demo: orders × items × tweets ×
  supplies, OOM). Fan-out dims stay individually addressable.
  (`test_catalog.py::test_row_preserving_flags_by_direction_and_cardinality`,
  `test_translator.py::test_select_star_browse_mode_skips_fanout_paths`.)

## Class (d) — new errors / newly legal shapes

- Parallel forward edges: silent first-match → `AmbiguousJoinPathError`
  naming both candidates (`test_dev1853_ambiguity.py::
  test_forward_dimension_no_longer_first_match`). Same error in reverse.
- Ambiguous filter-pushdown correlation hop: drop+warn (`kind:
  "unreachable_filter_dropped"`, strict errors) → `AmbiguousJoinPathError` in
  BOTH modes (`test_dev1853_pushdown.py::TestAmbiguousCorrelationFailsClosed`).
  Reworked dev1840 cases assert the same: `test_dev1840_disposition.py::
  TestExcludedConjuncts::test_ambiguous_measure_hop_fails_closed` +
  `test_ambiguous_filter_hop_fails_closed` (were `…stays_dropped`), the four
  `test_dev1840_strict_metadata.py` strict/lenient ambiguous cases (lenient
  warned → now errors), `test_dev1840_fixture_smoke.py::
  test_ambiguity_graph_executes` (`agents.score:sum` executed 60.0 via silent
  first-match → raises), and golden `excluded/ambiguous_inversion` × 7 dialects
  (emitted SQL whose producer CTE silently lacked the dropped filter → recorded
  `AmbiguousJoinPathError`). The dev1747/dev1745 dropped+warned cases were
  re-anchored on the still-dropping D2 mixed-OR shape (per-(location,text)
  dedup identity and agreeing reasons unchanged).
- Newly resolving reverse refs: a query rooted at the declared target now
  resolves paths over the inverted edge — previously `UnknownReferenceError`
  ("has no join to …") for default LEFT joins, which were never mirrored
  (`test_dev1853_traversal_execution.py`, whole suite; reverse saved-measure
  refs and reverse cross-model aggregates included).
- `recommend_root_model` reachability growth: reverse-only targets become
  recommendable (`test_dev1853_join_graph.py::test_reverse_reachability_growth`
  — `a → b ← c` now roots anywhere); recommendations across ambiguous unnamed
  pairs report unreachable instead of emitting a path that would fail
  (`test_unnamed_ambiguous_pair_reports_unreachable`). Downstream flips in the
  pre-existing suites: the mentioned-candidate tiebreak now beats the
  unmentioned bridge for `{customers, products}`-style item sets, so the
  recommended root flips `orders` → `customers` (7 tests across
  `test_recommend_root_model.py` / `…_surfaces.py`, REST/CLI/client
  included); the coverage Pareto frontier grows (`customers`/`products`
  survive alongside `orders`); infeasible-hint probes moved to
  cross-component hints since in-component infeasibility no longer exists.
- `JoinGraph` directedness retired: `reachable_from`/`count_simple_paths`/
  `shortest_path` operate on the undirected multigraph; parallel edges count
  as distinct routes and unnamed parallel pairs are not executable
  (`shortest_path` → None) (`test_dev1853_join_graph.py`; flipped
  `test_join_graph.py::test_left_join_reaches_both_ways` — was
  `test_left_join_is_directed`; rebuilt `test_dev1780_missing_join_path.py`
  route counts, all values unchanged except the retired symmetric-pair
  encoding).
- OSI import re-anchoring: cross-dataset metrics `cust_reach`/`bridge_metric`
  re-anchor `orders` → `customers` with reverse-hop dotted formulas
  (`orders.amount:sum / customer_id:count_distinct`;
  `orders.products.price:sum + customer_id:count_distinct`) — hand-derived,
  confirmed by execution (`test_osi_converter.py`).
- dbt import: no reverse mirror edge is emitted; the inverted forward edge
  answers the reverse orientation (`test_dbt_converter.py`,
  `test_join_cardinality_producers.py`).
- Save-time: newly declared exact-inverse joins rejected ("reverse traversal
  is automatic"); edge-name collisions rejected; unnamed parallel edges warn
  (`test_dev1853_validation.py`).
- Audit shape: one finding per declared edge with
  `forward_provably_to_one`/`reverse_provably_to_one`; forward-proven edges now
  appear as info rows instead of being absent
  (`test_dev1853_inspect.py::TestAuditReportsBothOrientations`,
  reworked `test_dev1836_validation_pressure.py`).

## Checker contract

Each flipped test cites its ledger entry (comment `DEV-1853 divergences.md
class (…)`); the ledger is complete when no test changes behavior without an
entry. The dev1836 and dev1842 families migrated with ZERO flips — the
inverted forward edge reproduces the declared-1:N semantics exactly (same
broadcast/dropped-filter behavior, same values).
