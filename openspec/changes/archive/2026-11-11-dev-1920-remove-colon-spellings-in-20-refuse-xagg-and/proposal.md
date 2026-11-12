## Why

SLayer 2.0 has one aggregation spelling, the functional `agg(col, args)`, and one bucket spelling. The legacy colon spellings `x:agg(args)` and `col:gran` survive as a second accepted form, as the text SLayer itself emits (canonical measure text, facade formulas, probes, memory tokens) and as text in stored documents. 2.0 removes them outright, with no deprecation period.

## What Changes

- **BREAKING** Fresh input using a colon spelling — `x:agg(args)`, `*:count`, `path.*:count`, a join-path source, a colon order key, or `col:gran` in a time dimension — is refused with a typed error naming the span and its functional twin, in every Mode-B position, on model and saved-query saves, and in entity references (`recommend_root_model` items, memory `linked_entities`).
- Stored Mode-B text is migrated on load (one versioned, stored-only step): every colon span becomes `name(source, args)`, the rest of the text stays byte-identical, and stored order items take the shape a fresh functional key produces. Old saved queries return identical rows and result keys.
- SLayer emits no colon text: canonical measure text, the facade catalog and translator, the live-schema probe and the memory resolver all use the functional spelling. Result keys are unchanged (`sum(a) / count(*)` keeps `a_sum_count`).
- The colon readers are removed: the accepting colon grammar production, the colon order-key normalisation, the legacy `col:gran` branch, and the colon string helpers (`AGG_REF_RE`, `split_agg_suffix`, `strip_agg_suffix`).

## Capabilities

### New Capabilities

### Modified Capabilities
- `aggregations/functional-form`: the colon-equivalence requirement is removed; the colon spelling is refused; "SLayer emits only the functional spelling" extends to the facade, probes and memory tokens, with result keys unchanged.
- `queries/expression-grammar`: "Legacy spellings keep their meaning" no longer lists the colon form.
- `compatibility/removal-tripwire`: the colon aggregation spelling leaves the not-marked list, since it is removed rather than deprecated.
- `models/stored-upgrade`: stored colon spellings are upgraded to the functional spelling on load.
- `models/aggregation-names`: the colon spelling leaves the spellings that resolve to a model's own aggregation.
- `queries/semantics`: a ranked-measure example is respelled functionally.

## Impact

- Code: `slayer/core/syntax.py`, `slayer/core/query.py`, `slayer/core/refs.py`, `slayer/sql/naming.py`, `slayer/sql/sql_predicate.py`, `slayer/engine/bind_inputs.py`, `slayer/engine/query_engine.py`, `slayer/storage/` (new colon migration step, version bump), `slayer/facade/catalog.py`, `slayer/facade/translator.py`, `slayer/live_schema/broken_dependents.py`, `slayer/memories/resolver.py`.
- Stored versions: SlayerModel 18 → 19, SlayerQuery 9 → 10, Memory 7 → 8.
- Clients sending colon text get a typed error with the functional remedy. BI clients of the facade see no change beyond one error message's wording.
- Tests: about 6,600 lines of incidental colon input are respelled; colon-subject tests become refusal tests or are deleted.
- Docs: one sentence in `docs/concepts/formulas.md`; `examples/trino/verify.py` and the perf corpus are respelled.
