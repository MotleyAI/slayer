## Why

A re-aggregation whose operand is a ranked (`first`/`last`) or windowed aggregate fails with an internal error instead of computing. `sum(last(balance, partition_by=[account_id, customer_id]))` — the one-expression semi-additive measure — raises a raw `RuntimeError` from the plain-renderer guard; `sum(sum(balance, window='30d'))` raises an internal rendered-schema `ValueError`. Both are well-typed terms (Axioms 6 and 9), and sql P10 requires an aggregate with its own ordering or frame to compile as its own producer. The re-aggregation carrier compiles its constituents at exactly its own grain, where the producer nesting rule keeps a root inline, so the ranked/windowed constituent lands in a plain grouped SELECT.

## What Changes

- One invariant: a **kernel-requiring** aggregate (ranked or windowed) is always the kernel answer of its own producer and never renders inline in another producer's grouped SELECT. One typed kernel decision (association → windowed → ranked precedence) is shared by every kernel site; the nesting rule consults it instead of two ad hoc carve-outs.
- Re-aggregation over `first`/`last` and windowed operands computes in every consumer position (measure, filter, order key, arithmetic, transform input, computed dimension, aggregation parameter, nested re-aggregation), local and cross-model, with an explicit or implicit ranking column, with or without a query time dimension, and under every `to_many_handling` mode.
- A plan-time invariant check catches a kernel-requiring aggregate left inline; the renderer's guard stays as the SQL node's fail-closed backstop.
- Docs: one sentence in `docs/concepts/formulas.md`.
- Architecture: `sql.arc42.md` P10 gains `[enforced: test:tests/test_dev2006_kernel_operands.py]` (approved).

Out of scope: a ranked/windowed pick as the associated aggregate in the association kernel (DEV-1914, stays a typed `AssociationError`); a cross-model re-aggregation grouped by a joined to-many model's time bucket, including a cross-model windowed inner (DEV-2007).

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/partitioned-aggregates`: adds the requirement that re-aggregation over ranked and windowed operands computes.

## Impact

- `slayer/engine/compile/stages.py` — the kernel decision, `ProducerContext.kernel_answer`, `_producer_nesting_rule`, the three kernel sites (local regroup, cross-model target-rooted, shifted), the plan-time invariant in `_emit_planned`.
- `slayer/sql/generator.py` — the plain-renderer guard is kept unchanged.
- Sequenced after DEV-1976 and DEV-1994 (implementation starts once both are merged).
- `docs/concepts/formulas.md`, `architecture/sql.arc42.md`.
