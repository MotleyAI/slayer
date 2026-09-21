## Why

A custom aggregation's definition default that names a model already on the
aggregation's path (`weight: customers.spend` declared on `regions`, queried as
`customers.regions.pop:wsumr` from `orders`) works today only because DEV-1931's
query-root fallback happens to land on the same customer row when that model is one
hop from the root. One level deeper (`regions → countries`, a `countries`-declared
default `regions.pop` queried as `customers.regions.countries.gdp:…`) the default
fails as "not reachable" while its explicit `weight=` twin executes. The DEV-1900
render heuristic (`_default_frag_owner_path`) and the DEV-1931 legacy owner-only
routes re-decide the same frame question in two more places, the kernels render
expression defaults through a third door at the raw owner anchor, and the
determination route composes a round trip through the query root, refusing a
provably to-one reverse suffix that Axiom 1 already admits.

## What Changes

- **Cancellation is the rule for definition defaults.** Resolving a default's
  qualifier chain owner-first, a token naming a dataset already on the owner's path
  from the query root (root included) cancels the path back to that dataset and reads
  its row there, keeping the path's own spelling; an edge-name token never cancels;
  any other owner-unreachable qualifier still falls back to the query root; a chain
  whose first token resolves but a later one misses fails closed. Only definition
  defaults cancel — a query-typed path or a model-SQL fragment that revisits a dataset
  stays refused.
- **One resolver for every caller.** The DEV-1931 legacy owner-only routes and both
  forward-validity guards are deleted; home, input safety, parameter typing,
  re-aggregation and both render doors consume the same owner-first / cancel /
  root-fallback resolution. The cancelling walk lives beside `walk` in the shared
  join walker.
- **Canonical fragments at both render doors.** A definition default is requalified to
  absolute paths once and entered at the root wherever some reference is not
  owner-forward; a fully owner-forward fragment enters raw at the owner (byte-identical);
  the per-fragment forward/opaque probe is deleted. Picked expression defaults of the
  association, trailing-window and second-order kernels carry canonical SQL in producer
  coordinates instead of raw owner-relative text.
- **The determination route steps back only to the common prefix.** The route from a
  producer root to a host-coordinate path is the reversed target suffix past the two
  paths' longest common prefix plus the host suffix — one primitive behind
  attributability, leaf re-rooting and the broadcast reason — so a to-one reverse
  suffix determines and a fanning one is reported as the fanning hop, never as
  "unreachable".
- Axioms 2.4 / 2.6 in `architecture/semantics.arc42.md` gain the cancellation clause
  (normative harness — edited with per-change approval, granted 2026-09-21).

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `queries/semantics`: *Home dataset of a row-level aggregation source* gains
  reverse-hop cancellation for definition defaults (name-based, anywhere on the path,
  definition defaults only), the canonical rendering of a cancelled or root-frame
  default in every producer kind, and the fail-closed rules around it.
- `models/join-cardinality`: *Determination through to-one chains* states that between
  a model on one join path and a column on another the chain steps back only to their
  common prefix, so a provably to-one reverse suffix determines.
- `queries/cross-model-aggregates`: *Broadcast metadata* pins that a prefix-side
  dimension reachable only over a fanning reverse suffix reports that hop, never
  "unreachable".

## Impact

- `slayer/core/join_walker.py` — the cancelling walk.
- `slayer/sql/column_expansion.py` — the shared default door resolves absolute paths
  with cancellation; forward guard deleted.
- `slayer/engine/reference_closure.py` — legacy routes and forward guard deleted; root
  frame required; canonical expression SQL on the parameter spec.
- `slayer/engine/home.py`, `slayer/engine/compile/stages.py` (re-aggregation caller,
  the three kernels' picked parameters), `slayer/ir/planned.py` (`PickedParam`).
- `slayer/engine/join_safety.py` — the route primitive and its three sites.
- `slayer/sql/generator.py` — fragment door always canonical; picked-parameter door
  enters canonical SQL at the root; `_default_frag_owner_path` deleted.
- `architecture/semantics.arc42.md`, `docs/concepts/models.md`.
- New tests `tests/test_dev1908_*.py`, fixture `tests/_dev1908_fixtures.py`, golden
  `tests/golden/dev1908_sql_baseline.json`; DEV-1892 / 1900 / 1931 goldens stay
  byte-identical; one docstring sentence in `tests/test_dev1900_home_path.py`.
