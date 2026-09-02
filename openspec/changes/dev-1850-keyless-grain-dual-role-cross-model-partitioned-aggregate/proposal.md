# Proposal: dev-1850-keyless-grain-dual-role-cross-model-partitioned-aggregate

## Why

A cross-model partitioned aggregate whose partition key is absent from the query
dimensions crashes with an internal RuntimeError ("Combined regroup attach is
missing a host / producer grain slot for its join-back") whenever it is ALSO
consumed by a computed dimension — while the identical local shape raises the
clean plan-time error "partition_by column ... is not a query dimension", and
the keyless filter-reference / ORDER-BY-name sibling shapes that WORK locally
crash cross-model too. DEV-1850 originally proposed synthesizing a hidden host
grain slot to make the keyless shape execute; that was rejected at review
because a hidden GROUP BY key refines the result grain (adding the measure
changes the row count and other measures' values), violating the
cardinality-invariance requirement. The resolution is exact local parity:
clean errors where local errors, working row-routed shapes where local works.

## What Changes

- The strict partition-key validation covers cross-model partitioned aggregates
  consumed in combined positions (measure, composite operand, raw ORDER BY
  target, filter-only reference): the keyless dual-role and keyless
  ORDER-BY-raw shapes fail at plan time with the established
  "not a query dimension" ValueError instead of an internal RuntimeError.
- The cross-model combined discovery gains the local walk's row-routing
  exclusions: a filter referencing the computed dimension's own aggregate and
  an ORDER BY naming the computed dimension execute keyless (row-routed),
  exactly as they do locally, instead of crashing.
- The local and cross-model combined-consumer discoveries consolidate into one
  walk in `regroup_planner` with shared routing asymmetries; bare cross-model
  aggregate discovery, public-alias and declared-type extraction keep their
  behavior.
- The DEV-1838 negative pin expecting the internal RuntimeError is retired in
  favor of clean-error pins; the two keyless-grain carve-out exception
  sentences in the spec corpus are replaced by one uniform requirement.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: adds the uniform combined-consumer
  partition-key requirement (query-dimension keys, local and cross-model
  alike); the coexistence requirement drops its cross-model keyless-grain
  exception sentence.
- `queries/cross-model-aggregates`: the expression/dimension composition
  requirement drops its keyless-grain exception sentence; the keyless
  row-routed shapes (filter reference, ORDER-BY-name) and the clean
  ORDER-BY-raw error are specified.

## Impact

- `slayer/engine/regroup_planner.py`: `combined_partitioned_aggregates`
  generalizes into the unified combined-consumer discovery (explicit buckets:
  local partitioned, cross-model partitioned, cross-model bare, alias map,
  declared-type map).
- `slayer/engine/stage_planner.py`: `_discover_cross_model_combined` retires
  into the unified walk; the bind-time `_combined_consumer_keys` set unions in
  the cross-model bucket; `_plan_regroups` consumes the excluded-and-bucketed
  results. Renderer untouched (its RuntimeError stays as a backstop).
- Tests: new `tests/test_dev1850_keyless_grain.py`; the DEV-1838 negative pin
  is removed; the DEV-1836 total-routing blinding tests re-point their
  monkeypatches at the unified discovery seam (same invariant).
- Docs: `docs/concepts/formulas.md` partition_by section wording.
- No golden SQL changes expected (every changed shape starts from an error).
