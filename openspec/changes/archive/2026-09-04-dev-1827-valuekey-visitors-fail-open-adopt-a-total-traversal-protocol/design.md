# Design: ValueKey total traversal protocol

## Context

See proposal.md — Why. Current state: `ValueKey = Union[...]` of 11 frozen Pydantic kinds
in `slayer/core/keys.py` (`SqlExprKey` outside the union but traversable — it appears as
`AggregateKey.column_filter_key` and standalone). Three fail-closed visitors exist
(`_map_value_key`, `substitute_value_keys`, `filter_reachability`); ~28 fail-open
isinstance chains across `engine/` and `sql/`. No type checker runs in CI, so enforcement
must be runtime + tests. Existing rewriters are identity-preserving (`is`-checks, return
the original object when unchanged).

## Goals / Non-Goals

**Goals:** generic traversal total by construction; every remaining kind-dispatch site
fail-closed; adding a union member breaks tests until consciously handled; golden SQL
byte-identical; full non-integration suite green.

**Non-Goals:** `walk_parsed_refs` (different union, syntax layer); wholesale
`match`/`assert_never` conversion (no CI type checker — runtime raise tails carry the
guarantee); any user-facing behavior change beyond the one latent-bug fix below.

## Decisions

1. **`children(self) -> tuple[ValueKey, ...]`** on every kind — every directly embedded
   Mode-B key. Per kind: leaves (`ColumnKey`, `ColumnSqlKey`, `StarKey`, `LiteralKey`,
   `SqlExprKey`) → `()`; `TimeTruncKey` → `(column,)`; `AggregateKey` → source +
   key-valued args/kwargs values + `partition_keys` (if not None); `TransformKey` → input
   + `partition_keys` + `time_key` (if not None); `ArithmeticKey` → operands;
   `ScalarCallKey` → key-valued args; `BetweenKey` → column/low/high; `InKey` → column +
   values. Scalars are never children.
   - **A1:** `AggregateKey.column_filter_key` is excluded — Mode-A fragments are opaque;
     the rule is documented once on `children()`; a visitor needing the field handles it
     explicitly (reroot's invariance then comes free).
   - **B1:** `TimeTruncKey.column` is included even though `walk_value_keys` treats
     `TimeTruncKey` as a leaf today; call sites audited, override only on demonstrated
     breakage. Alternative (mirror old reach) rejected: `children()` would mean "whatever
     the old code did" — the trap this change removes.
   - **Ordering:** deterministic within an instance/process (field order; set-valued
     fields in iteration order). No cross-process guarantee — order-sensitive consumers
     sort, as today.

2. **`map_children(self, fn) -> Self`** — shallow, one level; rebuilds the same fields
   `children()` names; change detection by **identity** (`is`), returning `self` when no
   child changed and keeping equal-but-distinct replacements; preserves `partition_keys`
   None-vs-empty; never touches scalars or `column_filter_key`. Recursion order (pre/post)
   belongs to the caller. Alternative (deep transform with hooks) rejected: pre-order
   (substitute's match-before-recurse) and post-order rewriters both compose on the
   shallow primitive.

3. **Fail-closed defaults:** `_FrozenKey` implements both methods raising
   `NotImplementedError` with a message naming the protocol. A kind missing an override
   fails loudly with a diagnostic, not an incidental `AttributeError`; the totality test
   asserts every union member overrides both.

4. **Kind-policy registry (C2):** `KIND_POLICY: dict[type, KindPolicy]` in
   `core/keys.py`, `KindPolicy` a frozen Pydantic model with **consumer-named** flags —
   `slottable` (planning `_SLOTTABLE_KIND`), `slot_composite` (generator composite slot
   routing, exactly `{ArithmeticKey, ScalarCallKey}`), `materialised_order` (generator
   ORDER BY materialisation). Membership is exactly today's tuples; tests assert explicit
   expected membership per flag (the conscious-classification record) plus registry keys
   == `get_args(ValueKey)`. Generic structural flags rejected as too coarse — the
   generator's tuples are consumer policy, not structural compositeness. The two
   `_VALUE_KEY_TYPES` copies become one constant derived from `get_args(ValueKey)`.

5. **Per-rewriter traversal contracts** (migration must not silently widen semantics):
   - `lower_sugar_transforms`: post-order; today descends only `TransformKey.input` and
     composite operands/args. Full-`children()` traversal additionally reaches transform
     `partition_keys`/`time_key` and aggregate internals — a no-op on legal trees (phase
     rules forbid transforms there), asserted by tests with transforms nested in aggregate
     sources, partition keys, and time keys.
   - `rewrite_rank_partition_keys`: post-order; `rewrite_fn` keeps receiving the
     **pre-rebuild** node, matching today.
   - `substitute_value_keys`: pre-order, match-before-recurse, matched keys replaced
     atomically.
   - `_map_value_key`: keeps its leaf arms (path-mapping on `ColumnKey`/`ColumnSqlKey`/
     `StarKey`, standalone `SqlExprKey` mapping); composite arms collapse onto
     `map_children`.

6. **Asymmetric visitors** keep explicit dispatch + a comment naming the asymmetry + a
   fail-closed raise tail: `_iter_slot_deps` (stops at `AggregateKey`, skips
   `TimeTruncKey.column` — a time dimension must not auto-add the raw column),
   `filter_reachability` (already fail-closed, untouched), renderer dispatch in
   `value_expr.py` (unhandled `_FrozenKey` subclass → raise; scalars still render as
   literals).

7. **Latent bug fix:** `lower_sugar_transforms`' ScalarCallKey arm omits `InKey` from its
   hand-listed recurse tuple, so a `change`/`change_pct` nested under an `IN` inside a
   scalar call silently escapes lowering. The migration fixes it; a regression test must
   fail on the pre-change code.

## Risks / Trade-offs

- [B1 widens `walk_value_keys` reach] → audit each call site; focused unit assertions
  that the wrapped raw column is not auto-added/materialised (slot deps, filter phase,
  HAVING, join discovery); goldens arbitrate rendering.
- [Registry moves policy away from use sites] → consumer-named flags + explicit
  membership tests keep intent legible.
- [Frozenset child order is process-dependent] → protocol makes no cross-process ordering
  promise; order-sensitive consumers already sort (goldens stable today).
- [Rewriter collapse could change identity/rebuild behavior] → identity-preservation in
  the `map_children` contract + per-rewriter tests.

## Migration Plan

Single change, no deploy/rollback concerns (library-internal). Implementation order:
protocol + registry + tests in `core/keys.py` first; then rewriters in `keys.py`; then
engine walkers; then generator/renderer; audit sweep last. Golden suite after each step.
