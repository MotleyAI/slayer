## Context

See proposal.md — Why. Substrate facts that shape the design (merged tree, DEV-1900 +
DEV-1931 landed):

- Default resolution is owner-first with a query-root fallback through one shared door
  (`sql/column_expansion.py::resolve_default_qualifier_path` /
  `resolve_default_reference_paths`), consumed by `engine/reference_closure.py`
  (`default_param_value_key`, `expr_default_ref_keys`, `_frame_dotted_key`) for keys
  and by `sql/generator.py::_default_frag_entry` for a MIXED-frame fragment. Two
  `_forward_valid` copies (one per layer) re-walk the absolute path from the root to
  reject a resolved-but-revisiting owner reference; `_dotted_key_legacy` and the
  no-root-frame branch of `expr_default_ref_keys` (over `compute_expr_reference_columns`)
  survive for the re-aggregation caller only.
- `core/join_walker.py::walk` refuses revisits; binding raises "Circular join detected"
  for a query-typed revisit.
- `engine/join_safety.py::attributable_from_root`, `_reroot_leaf_via_host` and
  `broadcast_reason` compose the route from a producer root (at `target_path`) to a
  host-coordinate `host_path` as `(*_back_path(host, target_path), *host_path)` — the
  full round trip through the query root — which `walk` refuses whenever the two paths
  share a prefix.
- Two render doors read definition defaults: the fragment door
  (`_register_fragment_kwarg_joins` → `_default_frag_entry`, raw model text) and the
  picked-parameter door (`_render_picked_param_value`, raw owner-relative
  `PickedParam.sql` entered at `PickedParam.anchor_path`) used by the association,
  trailing-window and second-order kernels.
- Save-time validation (`storage/base.py::_validate_join_edges`) rejects an edge name
  equal to any model name in the datasource, both directions; hand-built bundles and
  extension-added joins are not validated.
- Alias allocation is keyed by resolved path; the Mode-A door re-parses fragment text
  into AST before emission.

## Goals / Non-Goals

**Goals:**
- Cancellation as the resolution rule for definition defaults, one resolver for every
  caller, canonical fragments at both render doors, and a correct determination route.
- DEV-1892 / DEV-1900 / DEV-1931 goldens byte-identical; their suites green.

**Non-Goals:**
- Binding defaults once at elaboration into typed keys with no render-time
  re-resolution at all (DEV-1901); this change narrows render-time work to
  "requalify through the same door".
- Bundle-level validation of edge-name / model-name collisions for hand-built bundles
  or extension-added joins (precedence makes a collision deterministic instead).
- Cancellation for query-typed paths or Mode-A fragments (cancellation is not extended
  to them; a query-typed revisit stays refused with the circular-join error).
- Refusing a `Column.sql` derived column that itself revisits a dataset on its path; it
  silently returns a wrong value today and is DEV-1952's fix, not this change's. arc42
  Axiom 2.4 states that refusal as the target, tagged `[target: DEV-1952]`.
- The "resolved through the root's own join to a sibling" branch and the self-named
  host guard in `attributable_from_root` (untouched).

## Decisions

- **D1 — Name-based cancellation anywhere on the path (over backtrack-only or
  home-only).** The frame is the absolute model chain from the query root through the
  owner path. A token equal to the name of a dataset already on that chain truncates
  the chain to it, keeping the original path tokens (an edge-name spelling on the
  source path survives into the key). Backtrack-only would give the root and a middle
  model different rules for the same author spelling; home-only is circular (the home
  is computed from the resolved defaults). The rule generalises DEV-1931's Reading A
  ("root-name wins") to every path model, so the two leading-name special cases
  (root, owner) collapse into the degenerate cancel-to-self.
- **D2 — Token precedence: incident edge name, then a model name on the path
  (cancel), then a model-name hop.** Edge names keep the precedence they have in
  `resolve_hop`, so an existing definition never changes meaning; an edge-name token
  never cancels, and a hop through one onto a visited model is a miss (as in `walk`).
  Ambiguity raises. Save-time validation already forbids the collision in stored
  models; for unvalidated bundles the order makes it deterministic.
- **D3 — Root fallback and partial chains.** A clean first-token miss from the owner
  retries from the root frame with the same walk (a root child not on the path, such
  as `stores.rent` in a `regions` default). A chain whose first token resolves (by
  cancellation or hop) but a later one misses fails closed with the existing
  unresolvable-join error, never re-anchoring at the root — DEV-1931's rule with
  "resolves" now including "cancels". Reachable from neither frame fails closed.
- **D4 — The cancelling walk lives in `core/join_walker.py` beside `walk`.** It is a
  traversal variant, and the walker is the one substrate both `sql` and `engine` may
  import. It takes the root, the prefix tokens and the tokens to resolve, resolves
  the prefix into a model stack first, and returns the absolute token path or `None`.
  `walk` is untouched. Alternative rejected: next to the door in
  `sql/column_expansion.py` (traversal logic in a Mode-A expansion module).
- **D5 — Forward-validity guards retire.** A cancelled path is forward-valid by
  construction (the walk never yields a revisit), so both `_forward_valid` copies are
  deleted rather than kept as a filter — the invalid shape is unrepresentable.
- **D6 — One resolver for every caller (over keeping the legacy routes opt-in).**
  `_dotted_key_legacy`, the no-root-frame expression branch and
  `compute_expr_reference_columns` (dead once that branch goes; verify no other
  caller) are deleted; the root frame becomes required on `default_param_value_key`,
  `expr_default_ref_keys`, `resolve_aggregation_params` and their private helpers
  (`home._default_param_keys`, `reference_closure._default_param_spec` /
  `_default_param_specs`). Callers: `home._default_home_candidate_paths`,
  `reference_closure._default_param_specs`, and the three `resolve_aggregation_params`
  sites in `compile/stages.py` — `_trailing_window_kernel` and `_association_arm`
  already pass the root; the re-aggregation caller passes the host as owner and
  root (its `_ProducerSynthesisContext.host_model` is non-optional). Behaviour change
  there: a dotted default naming a non-join qualifier fails closed at typing.
- **D7 — Canonical fragments at the fragment door.** `_default_frag_entry` resolves
  every reference through the shared door; a fragment whose every reference is
  owner-forward (absolute path extends the owner path) enters raw at the owner path
  (byte-identical by construction); otherwise the fragment is requalified to absolute
  paths and entered at the root. `_default_frag_owner_path`'s forward/opaque probe is
  deleted. Parse failure keeps entering the raw fragment at the source owner
  (unreachable in practice: typing already fails closed on an unanalysable default).
  Re-serialisation cannot leak into emitted SQL unless the AST changes, since the
  door re-parses the text; the seven-dialect goldens are the check.
- **D8 — Canonical picked parameters (Codex finding, folded).** The picked-parameter
  door is the second reader of raw default text. The planner requalifies each
  expression default once into query-root coordinates when it builds the parameter
  spec (it already has the absolute reference paths there); each kernel reroots the
  expression's reference keys into its producer root exactly as it reroots the bound
  key, regenerates the fragment in producer coordinates, and stores it with no anchor
  path; `_render_picked_param_value` enters it at the root. `PickedParam.anchor_path`
  is deleted if nothing else reads it. This closes the latent DEV-1931 hole for
  root-frame expression defaults in the kernels as well.
- **D9 — The determination route steps back only to the common prefix.**
  `join_safety` gains one primitive: walk the full target path from the host once (as
  `_back_path` does), slice the reversed per-hop tokens (edge name else source model)
  past the longest common prefix of target and host paths, and append the host suffix.
  It replaces `(*back, *host_path)` in the via-host branch of the three sites; branch
  order and the two untouched branches stay. Non-sharing pairs are byte-identical;
  sharing pairs go from refused-as-revisit to judged on the reverse suffix, so no
  accepted case flips. Reverse tokens render through the bidirectional join builder
  as today. Axiom 1 is unchanged: it already reads existentially over chains, and the
  round trip was an implementation artefact.
- **D10 — Reverse hop consumed as a value.** Cancellation consumes the qualifier; the
  leaf is read at the truncated frame's row. It never produces a join-valued
  (fanning or picked) expression: a reverse hop to a dataset not on the path is the
  fanning shape and fails closed at input safety as before.
- **D11 — Errors unchanged.** Unresolvable default → `UnresolvableDimensionJoinError`;
  cancelled path crossing a fanning hop → the input-safety error naming the hop;
  query-typed revisit → the circular-join error; ambiguity → `AmbiguousJoinPathError`.
- **D12 — arc42.** Only `architecture/semantics.arc42.md` changes: Axiom 2.4 and 2.6
  gain the cancellation clause and Axiom 2 gains the DEV-1908 enforced tag (exact
  diffs approved 2026-09-21). Axiom 1 stays.

## Risks / Trade-offs

- [Route change touches every attributability decision] → sharing routes are refused
  today, so no accepted case can flip; the broadcast reason for prefix-side dimensions
  changes from "unreachable" to the fanning hop, which the Broadcast-metadata
  requirement already demands; any existing test pinning the old reason needs the
  owner's consent to re-point.
- [Always-canonical fragments move a golden] → D7's owner-forward raw entry keeps the
  common case byte-identical; a moved DEV-1892/1900/1931 golden is a finding to bring
  back, not a re-bless.
- [Retiring the legacy routes changes re-aggregation typing] → only a dotted default
  naming a non-join qualifier changes (bogus key → fail closed); the DEV-1847 / 1859 /
  1928 / 1942 suites are the harness.
- [Edge-name / model-name collision in an unvalidated bundle] → D2's precedence keeps
  today's meaning (edge wins); never a silent cancel.
- [Kernel rerooting of expression references diverges from key rerooting] → both use
  `reroot_from_root` leaf-wise; a per-kernel executed test with an expression default
  pins each.

## Migration Plan

Behaviour-only change to default resolution, attributability and rendering; no data
or config migration. Rollback is a straight revert. arc42 edits are applied with the
recorded per-change approval.
