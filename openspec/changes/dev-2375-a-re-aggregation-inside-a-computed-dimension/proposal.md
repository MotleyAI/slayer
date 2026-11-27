## Why

A computed dimension holding a re-aggregation — `CASE WHEN max(sum(rate, partition_by=[team, name]), partition_by=team) > 50 THEN 1 ELSE 0 END` as `band` — fails with a database binder error beside any metric read across a relationship, as a `partition_by=` key even on one dataset with no join, and beside a windowed measure; and, when the dimension reaches its data across a fanning hop from the population, it silently double-counts instead of associating. All of it has one cause: the re-aggregation's placeholder is spelled as a column of the query root whose reserved name changes its meaning, so re-rooting moves it, producers carry it only where a synthesis site remembers to, and home and attributability judge it host-homed.

## What Changes

- An attached value — what a synthesized producer contributes to the plan reading it — becomes its own key kind, identified by its producer's answer and carrying its join to that plan's own coordinates; re-rooting rewrites only that join.
- Every plan that reads an attached value attaches it (one rule for the query and every producer, whatever it is rooted at); a plan-time check refuses a plan reading one it does not attach.
- Home, attributability and dependency closures read an attached value as its grain members: a re-aggregation dimension unattributable from a home associates (or, in error mode, is refused) like any other dimension. **BREAKING** (results): such queries returned multiplied values before.
- A substitution records the aggregates it rebuilds, so two re-aggregation dimensions in one `partition_by=` plan.
- The reserved `__regroup__` field-name guard retires: a field may carry that prefix.
- The placeholder typing context variable retires; typing reads the statement's attach table.
- Golden SQL text is unchanged.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/computed-dimensions`: a re-aggregation inside a computed dimension evaluates in every plan reading it as the re-aggregation joined on that plan's path to its grain members, with its grain members' home and attributability.

## Impact

- `slayer/core/keys.py` (new kind, traversal, kind policy), `slayer/engine/compile/` (`regroup.py`, `stages.py`, `shift.py`, `projection.py`, `discovery.py`), `slayer/engine/key_metadata.py`, `slayer/engine/elaborate_env.py`, `slayer/engine/population/` (home, attributability), `slayer/ir/planned.py`, `slayer/sql/scope.py`, `slayer/sql/generator.py`.
- Delivers DEV-2017's scope (regroup placeholders as their own key kind); DEV-2017 closes with this change.
- Tests: the strict xfails in `tests/test_dev2367_list_elements_across_joins.py` flip; tests constructing reserved-leaf placeholders or pinning the retired guard are aligned.
