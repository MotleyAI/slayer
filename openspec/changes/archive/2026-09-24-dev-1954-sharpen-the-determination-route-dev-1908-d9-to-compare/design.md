## Context

Path tokens resolve through the shared walker (`join_walker.resolve_hop`: edge name first, else the
opposite-endpoint model name), but every door that stores a path keeps the typed token. Probe on
f7329eef (graph `orders→customers→regions` with the `customers→regions` edge named `hr`,
`regions→region_events` 1:N): mixed spellings double-join `regions`, and `customers.regions.pop:max` /
`customers.hr.region_events.value:max` against a dimension spelled the other way broadcast a grand total
("unreachable"). Save validation already rejects an edge name equal to any model name, so
"edge name, else target model" is an unambiguous canonical form.

Principles in force: system §3.8 (adding a measure never changes other values), §3.9 (dotted-canonical
references — amended, approved), engine §3.2/§3.4 (spelling-insensitive downstream; interning),
§3.7 (slack pass — amended, approved), §3.8 (models persist verbatim), §3.10 (dependencies are closures),
semantics Axiom 2.7 (spelling-invariance).

## Goals / Non-Goals

**Goals:**
- One canonical path spelling at every resolution door; the whole spelling-sensitivity class gone by construction.
- Result keys follow the canonical path.
- Stored downstream flat references in the old spelling keep resolving, with a typed warning.

**Non-Goals:**
- Rewriting persisted models or stored query text (models persist verbatim).
- Changing the D9 route / home / prefix algorithms themselves.

## Decisions

1. **Structural fix, not the issue's band-aid** (user decision). Canonicalize at resolution rather than make
   `_common_prefix_len` / `_route_via_common_prefix` / `home._longest_common_prefix` model-aware; those stay
   token-based because on canonical paths token equality ≡ edge identity (docstrings state the contract).
   The issue's direct-call repro (mixed spelling passed straight to `attributable_from_root`) is no longer a
   reachable input; acceptance is at the query level, plus a unit pin on the canonical-spelling repro.
2. **Canonical helper takes a resolved chain only**: `join_walker.canonical_path(chain: Sequence[OrientedJoin])
   -> tuple[str, ...]` = per edge `edge.name if edge.name is not None else edge.target_model` (reverse unnamed
   hop → the oriented target). Never takes raw tokens.
3. **Doors that store a path** all return `canonical_path(chain)`:
   `binding._walk_join_chain` (effective path, incl. `_route_edgeless_hop` auto-routes; covers dims, measures,
   filters, order, time dims, stars, `ColumnSqlKey`); `bind_inputs._resolve_saved_measure_ref` (its
   `canonical_ref` from the resolved chain, so saved measures surface canonically); `column_expansion.
   _resolve_qualifiers` and BOTH walked branches of `_lenient_path` (exact dotted and naive `__` split — opaque,
   length-fitted or collision-mangled allocator aliases stay unparsed), `resolve_default_qualifier_path`,
   `resolve_default_reference_paths`; `join_walker.walk_cancelling` appends canonical tokens (owner_path is
   canonical already). `reference_closure` paths follow from these. Lookup-only consumers are untouched.
4. **Naming = canonical path** (user decision A). `_canonical_if_routed` fires whenever the bound path differs
   from the typed hop path (respelling or routing); measure auto-names come from the canonical key. Explicit
   `name` overrides. Two spellings of one dimension are pinned as parity with requesting the identical
   dimension twice (not an incidental later collision).
5. **Stale-spelling slack rule** (user decision M2; engine §3.7 amended):
   - Respellings are computed at plan time against the CURRENT graph from each path-derived output's
     canonical key: substitute every non-empty subset of named hops with their target-model token and
     re-derive the flat name with the SAME naming function (dims, time dims, colon/functional aggregates,
     stars, saved measures; explicit names have none). Captured in `stages._emit_stage_schema` while the slot
     key and naming function are available, as an immutable tuple on `StageColumn`. 2^k−1 per column for k
     named hops — k is tiny in practice; no cap, no truncation.
   - A stage column that passes an upstream stage column through (a flat reference, or an auto-named
     aggregate over one) inherits that column's respellings, re-derived by substituting the upstream name in
     its own auto-name, so a chain of stages (or a query-backed model over another) keeps every stale
     spelling resolvable; explicit names have none.
   - Query-backed models are re-expanded from their stored query at execute time, so the runtime virtual
     model's generated columns carry the respellings from the fresh stage schema (current graph → covers an
     edge named later). Not persisted; `build_flat_rename_wrapper` stays canonical-only (`expected_columns`
     unchanged, no alias columns). If implementation finds a consumer that binds against the persisted
     `columns` cache instead of the expanded model, STOP and ask.
   - One explicit resolver (e.g. `resolve_flat_name(name) -> (column, respelled: bool)`) over a reverse index
     built once per schema/virtual model: exact name first; else exactly one respelling match; several →
     no match (ordinary unknown-reference error). `StageSchema.get` / `__getitem__` and
     `SlayerModel.get_column` stay exact-only; the resolver is called only at the stage-boundary doors:
     `binding._resolve_ref` (StageSchema arm and the query-backed model's local columns),
     `binding._resolve_terminal_leaf` (query-backed terminal), `column_expansion._process_reference_site`
     (another model's `Column.sql` into a query-backed model). On a match the key / expansion is built from
     the matched column's canonical name (never the stale leaf), and the resolver is gated to generated
     query-backed columns — authored model columns stay exact.
   - Warnings: the resolver returns match metadata; binding accumulates a typed `NormalizationWarning`
     (new rule id `STALE_PATH_SPELLING`, original = stale flat name, normalized = canonical) carried through
     `plan_stages` into the prepared pipeline's slack warnings, deduplicated per referencing position.
     On a result-cache hit the response's normalization warnings are replaced by the requesting query's own
     (cacheable execution warnings recombined), so a spelling twin neither loses nor leaks the warning.
6. **Errors quote the typed spelling**; persisted models are never rewritten.

## Risks / Trade-offs

- [Visible rename for a named edge typed by its model name] → narrow (non-parallel named edge + model-name
  spelling); documented; stored downstream references covered by the slack rule; external client code
  reading result keys must update.
- [Existing tests pin typed-spelling keys] → e.g. `tests/test_dev1853_named_edges.py`, 1–3 goldens; each
  changed assertion is listed for the user's OK in spec-tests.
- [Respelling combinatorics] → exponential only in named hops per path; bounded in practice.
- [Warning/cache interplay] → explicit cache-hit warning recombination + tests.
