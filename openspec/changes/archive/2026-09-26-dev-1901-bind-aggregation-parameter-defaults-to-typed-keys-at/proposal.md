## Why

An aggregation definition default (`AggregationParam.sql`) and an explicit string-fragment argument are raw text that flows past the bind boundary and is re-analysed independently by the planner (home, input safety, closure, parameter typing, three kernels) and the renderer — nine sites with different root frames, parse dialects and failure policies. Every disagreement is a silent-SQL hole (HAVING-seam defaults unexpanded, fail-open render re-resolution, quoting loss, a swallowed root-frame circular default, a default and its identical explicit spelling computed twice).

## What Changes

- Binding resolves every aggregation parameter once — non-overridden definition defaults and explicit string fragments — onto the aggregate's key: a column reference as a column key, a literal as a scalar, any other expression as a new typed SQL-fragment key (template + typed column references). Nothing downstream reads parameter text.
- A definition default and its identical explicit spelling are one value: one key, one slot, one producer; they differ only in the public auto-name, which follows the user's spelling.
- A default whose resolved path crosses a fanning hop homes exactly as its explicit twin does (Axioms 2.4, 2.7) — **BREAKING**: `customers.regions.countries.gdp:wsum_fan` now executes (home `region_events`) instead of failing closed.
- An explicit string argument naming a formula placeholder resolves like its unquoted twin.
- Unanalysable parameter text (unparseable, or containing an aggregate, window or subquery) fails at query binding with a typed error; a root-frame circular default is refused with the circular-join error.
- **BREAKING**: `window` is a reserved aggregation parameter / placeholder name, like `value`.
- The renderer receives the aggregation formula only, never its parameter definitions.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: the home-dataset requirement — a default homes identically to its explicit twin (the fanning-default scenario flips), plus shadowed-name, quoted-identifier, one-producer and root-circular scenarios.
- `aggregations/formula-templates`: parameter values bind once (default ≡ explicit twin, string fragment ≡ unquoted twin, typed bind error for unanalysable text) and `window` is reserved.

## Impact

- `slayer/engine`: `binding.py` (+ a binder module for parameter resolution), `reference_closure.py`, `home.py`, `compile/stages.py`, `agg_registry.py` (dead `merge_agg_params` removed).
- `slayer/core/keys.py` (new `SqlFragmentKey`, aggregate kwarg union), `slayer/core/refs.py` (canonical encoding), `slayer/core/errors.py` (new typed error).
- `slayer/sql`: `generator.py` (one parameter render path; fragment/default re-resolution removed), `column_expansion.py` (default resolvers move to the binder), `sql_template.py` (`window` reserved), `render/` (the new kind), `naming.py`.
- `slayer/ir/planned.py` (`PickedParam.sql` removed).
- Internal CTE / hidden-slot names change where defaults now sit on the key (goldens re-blessed); public result names unchanged.
- `architecture/system.arc42.md` P13 (reworded) and `architecture/engine.arc42.md` P1 (enforced tag); `docs/concepts/models.md`.
