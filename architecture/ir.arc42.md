# ir — shared intermediate representation

## 1. Purpose & context

`slayer/ir` is the shared intermediate representation between the planner
(`engine`) and the renderer (`sql`): typed plan shapes (`planned.py`, incl.
producer interning identity), bound expressions (`bound.py`, incl. declared
measures/orders and their discovery walks), the pre-bound query seam
(`prebound.py`), the typed term language (`terms.py`), the elaborated typing
environment (`elaborated.py`), the resolved source bundle (`source_bundle.py`;
the storage-backed builders live in `engine/bundle_builder.py`), and
query-variable merging (`variables.py`).

## 2. Building blocks

Flat submodules, no children: `planned.py`, `bound.py`, `prebound.py`,
`terms.py`, `elaborated.py`, `source_bundle.py`, `variables.py`.

## 3. Principles

1. **Representation only**: data shapes and pure functions over them — no
   storage, no I/O, no planning or rendering logic. [review]
2. **`ir` imports `core` only.** [enforced: arch_check:model-truth]
3. **`__init__.py` stays a docstring**: import from submodules; no re-export
   surface. [review]
4. **Terms add what keys lack**: `ValueKey` stays the identity spine; a term
   carries only the resolved `home: Dataset` and total `grain: Grain`, with
   `Aggregate.recipe` a reference to its key, never a copy. [review]

## 4. Rationale

The renderer must not see how plans are made and the planner must not see how
they are rendered; both need to see what a plan IS. A neutral representation
package is the only home satisfying both, and keeping it pure (P1, P2) is what
lets any layer above `core` share these types without inheriting storage or
pipeline dependencies.
