## Context

See proposal.md — Why. The one traversal substrate is
`slayer/core/join_walker.py::walk` (strict face: raises
`AmbiguousJoinPathError`, returns `None` on a miss AND on a revisit). Its direct
consumers: `terminal_model`; `join_safety.safe_reachable`, `_back_path`,
`_hop_walk_reason`, `_path_grain_determined`; `compile/stages._reverse_hops`,
`_canonical_path`; `sql/column_expansion._walk_exact`;
`engine/column_dependency._classify_hop_path`. The STRICT expansion door
`column_expansion._resolve_qualifiers` classifies a Mode-A reference as a join
walk / opaque / broken; its `_raise_if_broken_join_walk` re-walks hop by hop, so
a revisit (every hop resolves) passes as opaque. Definition defaults resolve
owner-first / root-fallback through `resolve_default_qualifier_path`. The
closure's dialect loops in `engine/reference_closure.py`
swallow `Exception`, re-raising only `ColumnCycleError`. Two save doors:
`StorageBackend.save_model` → `validate_derived_columns`; the engine's
`save_model` → `_validate_mode_a_join_paths` (expands every surface through the
door) BEFORE `storage.save_model`. Composed via-host reroot paths
(`join_safety._reroot_leaf_via_host`) legitimately revisit today and rely on
`safe_reachable` answering `False`.

## Goals / Non-Goals

**Goals:**
- One typed revisit signal from the one walker; the two consumers that classify
  a user-typed definition fail closed on it; every best-effort consumer and the
  definition-default machinery keep today's answers.
- One identical save-time error from both save doors.

**Non-Goals:**
- Any cancellation rule (`a.b.a ≡ a`) — DEV-1908 owns definition-default paths.
- The hand-rolled `resolve_hop` loops (`ir/prebound.walk_key_path`,
  `binding.py` terminal walk, `population.py`, `memories`, `schema_drift`,
  `response_meta`, `sql/generator.py`): they never call `walk`, never see the
  raise, and keep their own `None` semantics.
- Richer broadcast reasons for a via-host revisit (`_hop_walk_reason` keeps
  returning `None`; goldens unchanged).
- The saved-measure round-trip raise (`binding.py` "Round-trip reference")
  keeps its own class and message; `validate_models` untouched.

## Decisions

- **Raise from the walker (B), not a classifier-local re-walk (A) or a typed
  outcome object (B′).** A revisit is a definite malformation of the path, like
  ambiguity, which the walker already raises; `None` is reserved for "no such
  edge / target not loaded". This makes the revisit-as-unknown conflation
  unrepresentable and gives the query-time backstop for free from the same
  signal. A was rejected (a fourth hand-rolled loop, no backstop); B′ was
  rejected (leaves the lossy `walk` as the default API).

- **Error family.** `CircularJoinPathError(SlayerError, ValueError)` with
  keyword fields `reference` (the complete user spelling, leaf included where
  known), `root_model`, `revisited`, `hop` (token), `via` (the model the hop
  leaves), optional `column`; message `Circular join detected resolving
  {reference!r} from {root_model!r}: hop {hop!r} revisits model {revisited!r}.`
  plus ` (in the definition of column {column!r})` when set — the "Circular
  join" / "revisits model" substrings existing tests match survive.
  `DerivedColumnCircularError(CircularJoinPathError)` adds `column`, `model`,
  `kind` and the remedy: `Derived column {column!r} on model {model!r} has a
  {kind} reference {reference!r} that revisits model {revisited!r} (hop {hop!r}
  from {via!r}): a join path never revisits a model on it, so this is not a
  column of {model!r}. Reference the column on {revisited!r} directly if you
  mean this row's value, or declare the aggregate on {via!r}, which reaches
  {revisited!r} forward.` Subclassing keeps one `except` for the family and the
  `ValueError` contract REST/storage callers rely on.

- **Consumer handling (catch → today's answer).** `terminal_model` → `None`;
  `safe_reachable` → `False`; `_back_path` → `(host_name,)`;
  `_route_via_common_prefix` (DEV-1908) → `(host_name, *host_path)`;
  `_hop_walk_reason` → `None`; `_path_grain_determined` → `False`;
  `_canonical_path` → `tuple(path)`; `_reverse_hops` sets `fwd = None` inside
  the `except` so its existing ledgered `_PushBlocked` raise stays one site;
  `resolve_ref_target`, `_lenient_path` → `None`.

- **The door fails closed.** `_walk_exact` propagates; `_resolve_qualifiers`
  catches and re-raises with `reference` built from the ORIGINAL qualifiers
  plus the leaf (pre-strip spelling), `root_model=source_model.name`, and an
  optional `column` that `_process_reference_site` supplies from its innermost
  `visited` entry (the definition being expanded) when there is one.
  `_raise_if_broken_join_walk` is untouched — unreachable for a revisit.

- **Closure passthrough.** `fragment_closure`, `fragment_null_propagates` and
  `_expand_derived_refs_any_dialect` re-raise `CircularJoinPathError` next to
  `ColumnCycleError`, so the checker never reports a revisit as "no supported
  dialect can analyse". `compute_expr_reference_columns` and the generator's
  default-fragment helpers use the LENIENT scanner (`None`) and stay as they are.

- **Definition defaults unchanged (Codex, high).** A default can be circular
  from the speculative owner frame yet valid from the root (owner `regions`,
  root `customers`, default `customers.regions.pop`).
  `resolve_default_qualifier_path` resolves through DEV-1908's
  `walk_cancelling`, which cancels a revisit (`a.b.a ≡ a`) and never raises
  the circular error, so default-path semantics stay DEV-1908's. Alternative
  (propagate from the root attempt) rejected: it moves default-path semantics,
  which DEV-1908 owns.

- **Save time, two doors, one error.** `_classify_hop_path` catches the walker
  error and `_check_reference_arity` raises `DerivedColumnCircularError`, with
  the raw qualifiers threaded through `_iter_arity_refs` so `reference` is the
  complete spelling; circular precedes any arity verdict by construction (the
  walk aborts before arity is judged). `_validate_mode_a_join_paths` keeps
  `(column, kind, fragment)` per column surface instead of a flat list and
  converts a caught `CircularJoinPathError` into the same
  `DerivedColumnCircularError`; a model-filter surface propagates the base
  error. Both sites build the derived error from the caught error's fields.

- **Unloaded intermediate (Codex, medium).** A revisit needs every model on the
  path up to it loaded (the return hop resolves against the intermediate's
  joins), so a known-but-unloaded intermediate is unprovable at save time and
  skips under DEV-1930's unloaded rule; the query-time door already refuses it
  as `UnresolvableDimensionJoinError` ("not in the resolved bundle"). No
  special-casing of an immediate back-hop.

- **Binder parity.** The query-typed circular raise adopts
  `CircularJoinPathError` (fields from the binder's own walk); still a
  `ValueError`, substrings preserved.

- **No raise-ledger row.** None of the raising modules (`core/join_walker.py`,
  `core/errors.py`, `sql/column_expansion.py`, `engine/column_dependency.py`,
  `engine/query_engine.py`, `engine/binding.py`) is in the DEV-1871 scan, and
  `tests/test_dev1871_raise_parity.py` rejects rows for unscanned modules — the
  conclusion DEV-1930 reached for its save-time error.

- **Normative harness / docs.** The approved Axiom 1 clause with
  `[enforced: test:tests/test_dev1952_derived_revisit.py]`; one sentence in
  `docs/concepts/models.md`.

## Risks / Trade-offs

- [A missed `walk` consumer turns a benign composed-path revisit into a raise]
  → the explicit consumer list above, a direct regression test of the via-host
  reroot path (`attributable_from_root` / `reroot_from_root`), and the full
  suite (DEV-1900/1910/1911 exercise those paths).
- [A revisit through an unloaded intermediate is not refused at save time] →
  best-effort by design (DEV-1930); the query-time door refuses it as
  unresolvable; pinned by a scenario at both times.
- [Definition-default probing newly raising] → the default door resolves through
  DEV-1908's `walk_cancelling`, which cancels a revisit and never raises the
  circular error; pinned by scalar and expression default regressions in the
  DEV-1900 back-hop shape and a revisiting default probed from the owner frame.
- [Binder message wording changes] → existing tests only substring-match
  "Circular join" / "revisit"; both preserved.
- [Self-join edges] → a real self-join is rejected at model construction
  (`SlayerModel._reject_self_joins`), so save/query paths never see one and the
  revisit rule never has to; the walker's own first-hop revisit guard is still
  covered by a walker-only unit test built via `model_construct`.

## Migration Plan

Additive gate; no data migration. Migration write-back (`_validate=False`)
bypasses validation, so stored models load unchanged and are refused only when
queried. Rollback = revert.
