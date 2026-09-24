## Context

Two save doors raise the derived-column errors: the storage door
(`column_dependency.py::_check_reference_arity`) walks each saved column's
**direct** references; the engine door (`query_engine.py::_validate_mode_a_join_paths`)
**recursively expands** each column's `sql`/`filter` via
`expand_derived_refs_sync`, catching `CircularJoinPathError` and rebuilding a
`DerivedColumnCircularError`. The caught error already carries the innermost
column (`exc.column`, from the expansion's `visited` stack) and that column's
declaring model (`exc.root_model`, the expansion scope's `source_model`) — the
engine door discarded `root_model` and substituted the outer surface's `model`
and `kind`. Principles: semantics.arc42 Axiom 1 (unchanged rule, only its
reporting); core.arc42 §1 (errors.py is the typed vocabulary); the
Column.filter pure-value-mask rule (why `kind` is meaningless). No arc42 / `.c4`
file mentions `kind`; none is edited.

## Goals / Non-Goals

**Goals:** one source of truth per reported fact, so the two doors cannot
disagree on a label.

**Non-Goals:** making the storage door expand transitively (it keeps judging
direct references only — the cross-model attribution scenario is engine-door
only); any change to which definitions are accepted or rejected.

## Decisions

1. **Drop `kind`** from both error classes, the unproven warning, and the
   `(kind, fragment)` plumbing (`_arity_reference_sources` → `list[str]`,
   `_iter_arity_refs` → `(column, hop_path, leaf, quals)`, engine loop over
   `(col.sql, col.filter)`). Alternative — thread the inner column's kind through
   `CircularJoinPathError` — rejected: keeps a meaningless label in sync by hand.
2. **`DerivedColumnCircularError.model` is a read-only property = `root_model`**;
   the `model` constructor parameter is removed. Keeps the `.model` attribute
   shared with `DerivedColumnFanningError` without a second copy. Alternative —
   engine passes `model=exc.root_model` — rejected as a band-aid (two fields,
   hand-synced). The base `CircularJoinPathError.__init__` never assigns
   `self.model`, so the property does not collide.
3. **`DerivedColumnFanningError.reference` required, full user spelling** built
   from `(*quals, leaf)` (as the circular raise already does), so a leading
   declaring-model qualifier is kept; the `'{hop}.<column>'` fallback is removed.
   Remedy is `{reference}:<aggregation>`.
4. **Messages** (only the opening clause changes, remedy tails unchanged):
   - fanning: `Derived column {column!r} on model {model!r} references {reference!r}, crossing a fanning join hop to {hop!r}: …`
   - circular: `Derived column {column!r} on model {model!r} references {reference!r}, which revisits model {revisited!r} (hop {hop!r} from {via!r}): …`
   - unproven warning: `Derived column {column!r} on model {model!r} has a reference crossing an unproven join hop to {hop!r} …`
5. **Unproven warning dedup per `(column, hop)`** — the warning carries no
   reference, so one per column/hop is the natural unit.

## Risks / Trade-offs

- [Python-API break for anyone constructing these errors with `kind=`/`model=`]
  → internal-only raisers (two sites, both updated); no REST/MCP/CLI consumer
  reads the fields (all map through the generic `ValueError` → 400 path).
- [Remedy text changes for host-prefixed fanning spellings] → the spelled
  `orders.line_items.qty:<aggregation>` resolves identically (a leading host
  qualifier is stripped at resolution).
