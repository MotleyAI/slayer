## Context

The date-only upgrade (`slayer/storage/date_only_migration.py`) types an operand with `StoredModels`: the datasource's raw stored model dicts, plus the document's stage names. `host()` resolves a query's source; `column_type()` walks the path's qualifiers with `_hop()` over `_incident()` (stored joins on either side, plus the query's extensions) and reads the leaf's stored `type`. The built-in spine (`slayer/core/time_spine.py`: `time_spine`, one TIMESTAMP column `timestamp`) is not stored, so it is absent from this universe.

The engine's universe differs in three ways. It resolves a model-name spelling against stored models plus the spine: an exact stored name or a stage wins, else a spine spelling is the spine, and a stored case variant of the spine makes it ambiguous (`_respell_spine`, `_check_spine_spellings`). It reaches the spine through virtual edges (`join_walker._virtual_spine_edges`, `spine_joins`/`axis_join`, the population factor's product edge). And a path may end at the spine but never cross it (`join_walker._walk`).

A second host resolver exists. The legacy time-literal repair (`StorageBackend._repair_query_filters` → `_query_host`) looks a query's source up by exact name with `_cached_raw_model_dict`, so it misses the spine and case variants independently of `StoredModels`.

**Symptom → structural cause.** The symptom is a spine operand left untyped. The cause is that the upgrade's typing universe is not the engine's model universe, and that two host resolvers must agree by hand. Grounding: `architecture/system.arc42.md` §3.10 *One typing*, under which a value's type is derived from its key in one place.

Principles that apply: system §3.10 (the spine's column types come from `spine_model()` in core, never a TIMESTAMP literal copied into storage); §3.8 *One reader of Mode B* (texts are scanned only through `core.syntax` / `core.date_only`, as today); §3.5 async-first; §3.6 Pydantic, no dataclasses; storage §3.3 *Versioned persistence* (no version bump, because the upgrade is unreleased). No arc42 or `.c4` edit is needed: storage → core is already modelled. The spec stays where it is (`models/stored-upgrade`).

## Goals / Non-Goals

**Goals:**
- Every operand the engine resolves to the spine's column types as the spine's column (TIMESTAMP) in every stored-upgrade path (SlayerModel texts, nested source queries, extensions, memory queries, the legacy-literal repair), under every spelling the engine resolves to the spine.
- A query or extension rooted at the spine types its operands on the datasource's models.
- One typing universe and one host resolver for stored typing.

**Non-Goals:**
- Inferring the datasource of a memory rooted at the spine that names no datasource, in a store with several datasources. Its spine operands are typed, because the spine is the same in every datasource. Its other operands stay untyped and logged, as today (see D7).
- Reproducing the engine's exact spine reachability (axis edges, detached datasets, product edges) on raw dicts (see D2).
- New document versions (see D5).

## Decisions

**D1. The spine is one more declared model of `StoredModels`.** Model-name resolution runs `match_name` over the stored names plus `time_spine`, unless a stored model is named exactly `time_spine` (grandfathered from before the name was reserved, as `builtin_models` treats it). This gives the engine's precedence with no special casing. `time_spine` exactly names the spine. Another case names the spine when no stored model has it exactly; a stored model with that exact spelling wins; a spelling that case-matches both a stored variant and the spine is ambiguous, so the operand is untyped and logged, matching the engine's `AmbiguousReferenceError`. A stage name still wins in `host()`. The spine's dict is derived from the spine's core definition (its name and column, from which `spine_model()` is also built). Options weighed: the issue's suggested "fall back to the spine when no stored model matches" (it would type `TIME_SPINE` as a grandfathered `Time_Spine`, where the engine refuses) versus declared-universe membership (taken: exact engine precedence). (default)

**D2. Spine reachability is broad, but no path continues past the spine.** In `_incident`, every stored model gets an unnamed virtual edge to the spine, and the spine gets one to every stored model. These are matched at the target-name tier, so a stored join named `time_spine` (edge-name tier) still wins. They are not gated on a model's time axis. `column_type` returns `None` (untypable) when a qualifier follows a hop that landed on the spine, which mirrors the engine's "a path may end at the spine, never cross it". Rationale: the leaf type is the spine's column whichever route reaches it. For any path the engine resolves, the extra edges change nothing: they match only spine spellings, where a stored-variant ambiguity refuses in the engine too. A path the engine cannot resolve fails every query before and after the rewrite. Reproducing the axis, detached-dataset and product-edge rules would re-derive `effective_default_time_dimension`, cardinalities and keys on raw pre-migration dicts, a second derivation of the axis that §3.10 forbids, while reusing `join_walker` needs validated current-version `SlayerModel`s that a mixed-version raw store cannot provide. Alternatives: (a) broad edges plus the crossing guard (taken); (b) a faithful re-derivation on raw dicts (rejected: second derivation, fragile); (c) validated models plus `join_walker` (rejected: not available at the stored-dict stage). The crossing guard was folded in from Codex. (default; crossing guard (codex))

**D3. Stage spellings beat the spine in a hop.** `_hop` treats a token naming one of the document's stages as untypable before it tries target names, as the engine's `resolve_hop` checks a stage's spelling before the opposite model's name. Without this, a sibling stage named `time_spine` would be typed as the spine. (default)

**D4. One host resolver.** `StorageBackend._query_host` keeps only what `StoredModels` lacks: it accepts an inline dict source, and it finds the datasource when the caller has none (`resolve_model_identity`). It resolves the source NAME through `(await self._stored_models(ds)).model_copy(update={"stages": stage_names}).host(name)`. `_stored_column_type` uses the same `StoredModels`. As a side effect, a legacy-literal query rooted at the spine or at a case variant of a stored model is repaired and upgraded too. Options: keep `_query_host`'s exact-name lookup and special-case the spine there (rejected: a second resolver kept in step by hand) versus one resolver (taken). (default)

**D5. Amend the upgrade in place, with no version bump.** The v24/v15/v11 upgrade has not shipped (it ships with 2.0 alongside DEV-2280), so the fix changes it in place. Dev stores already upgraded on `main` since #593 keep their spine comparisons at midnight; this is accepted because they are not released stores. (default)

**D6. No docs change.** `docs/upgrading/2.0.md` already says stored comparisons are rewritten to the whole-day spelling and that only a column with no stored `type` is left. The spine's column has a type, so the docs already describe the fixed behaviour. (default)

**D7. A store's only datasource types a memory that names none.** When a stored memory names no datasource, its source names no stored model, and the store holds exactly one datasource, the upgrade types the memory with that datasource. The engine's `spine_datasource` can pick no other. Options: keep the Non-Goal (rejected: a one-datasource store, the common case, would lose the last day for good although the engine's answer is certain); re-derive `spine_datasource`'s anchor inference on raw dicts (rejected: a second copy of the engine's anchor logic, as in D2); the sole-datasource shortcut (taken). (user; CodeRabbit review)

**Codex plan review:**
- Make reachability match the engine's axis, detached-edge and product rules: rejected (codex), for the reasons in D2. Outcomes are identical on every query the engine resolves, and a faithful copy would be a second derivation of the axis on raw dicts.
- Never type a path that crosses the spine: folded into D2 (codex).
- A test matrix over a model with an axis, an axis-less model with a to-one chain to an axis, a detached axis-less model, and a spine crossing: folded into tasks 1.3 and 1.4, pinning D2's behaviour (codex).
- Extension cases (`source_name: TIME_SPINE`, extension column filters, an extension join in a multi-hop path): folded into the spec scenarios and task 1.2 (codex).

## Risks / Trade-offs

- [Broad edges type a path the engine would reject, for example `customers.time_spine.timestamp` from an axis-less, detached `customers`] → That query fails both before and after the rewrite. If the model later gains an axis, the rewritten text already means the whole day the 1.x author meant.
- [The legacy repair now also resolves case-variant sources] → It repairs comparisons that the exact-name lookup used to skip. This matches what the engine resolves, so query results are unchanged.
- [`StoredModels` with no datasource (a memory on the spine with no datasource, in a store with several datasources) still declares the spine] → Only spine operands get typed; that is correct in every datasource.
