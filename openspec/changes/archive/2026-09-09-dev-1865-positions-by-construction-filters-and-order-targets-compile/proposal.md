# Proposal: Positions by construction — filters and order targets compile as hidden fields/measures

## Why

Filters and order targets carry a third, parallel semantics (scope-intersection placement, `classify_regroup_filter`, phase classification) that must be re-extended every time measures learn a new shape — and lags when it isn't: "Filtering on a cross-model partition_by aggregate is not yet supported", the mixed-grain OR guard, and order targets supporting shapes measures don't. Value parity between a measure and the same expression in a filter is maintained by parallel code, not guaranteed.

## What Changes

- One **resolve-then-type pass** types every query-filter conjunct and order target as a **field** (aggregate-free after resolution, legal as a projected field) or a **measure** (legal as a declared measure in the same query); an expression valid as neither fails with a typing error naming both failures.
- Filter conjuncts and order targets compile into **hidden projection slots** planned through the one render pipeline; the value is used as a mask / sort key and dropped at the existing trim boundary. Lowering (field mask → base WHERE; simple aggregate mask → HAVING / outer WHERE) becomes a pure emission optimization — golden SQL stays byte-identical for currently-legal shapes.
- Newly legal: filtering on a cross-model `partition_by` aggregate; one predicate mixing a computed dimension's aggregate with another reference.
- Error-message changes only: the mixed-grain OR and unsupported ORDER BY targets fail as the typing error.
- `PlannedQuery` drops `FilterPhase`/`filters_by_phase`/`outer_where_filter_ids`/`combined_filter_indices` in favor of typed mask entries; `OrderEntry` drops plan-side `scope` (classification moves to emission-side lowering). Mode-A model filter texts keep a dedicated carrier. Cross-root filter propagation (semi-join/EXISTS, drop+warn, strict mode) is unchanged in behavior; filter-reachability plumbing migrates to mask IDs.

## Capabilities

### New Capabilities
- `queries/positions`: how filter and order-target expressions are typed (field vs measure), where each masks/sorts, the stratification of filter conjuncts, value parity with the same expression in measure position, and the typing error.

### Modified Capabilities
- `queries/partitioned-aggregates`: filter-routing requirement restated in typing language; the no-common-scope failure becomes the typing error; the "put them in separate filters" limitation on predicates mixing a computed dimension's aggregate with other references is lifted.
- `queries/cross-model-aggregates`: filtering on a cross-model partitioned aggregate now executes (scenario added to the expression-composition requirement).

## Impact

- `slayer/engine`: `stage_planner.py` (filter routing, order-slot synthesis, guard retirement), `regroup_planner.py` (`conjunct_scope`, `classify_regroup_filter` retired), `planned.py` (mask entries; `OrderEntry.scope` removed), `planning.py` (whole-predicate hidden slots), `filter_reachability.py` (input plumbing only).
- `slayer/sql`: `generator.py` gains the sectioned lowering stage (placement + `OrderScope` assignment); `render/order_terms.py` unchanged as the emission arm. No new modules, packages, import edges, or contract changes.
- Tests: existing golden suites must pass unmodified; new golden + executed-value suites for the newly-legal shapes; `test_dev1747_order_entry.py` (and siblings asserting `OrderEntry.scope`) updated mechanically.
- Docs: `docs/concepts/queries.md` filter/order semantics; stale references in `docs/architecture/planning.md` / `sql-generation.md` updated if those files still exist at merge time (DEV-1870 dissolves them).
