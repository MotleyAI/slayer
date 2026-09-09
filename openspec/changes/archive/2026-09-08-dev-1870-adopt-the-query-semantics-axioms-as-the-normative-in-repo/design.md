# Design

## Context

DEV-1857 scaffolded the living-architecture layer (LikeC4 + arc42 + import-linter +
`arch_check`). Only `system.arc42.md` and `sql.arc42.md` exist; `docs/architecture/`
still carries 14 contributor docs mixing normative constraints with stale
walkthroughs. The axioms/laws source notes are external scratchpad (untouched here —
the user merges them separately). See proposal.md for motivation.

## Goals / Non-Goals

**Goals:** repo-normative algebra with an explicit truth axis; arc42 files that are
purely normative; monotone dissolution ledger (every deleted file accounted for).

**Non-Goals:** any behaviour change; implementing not-yet-true semantics (mode axis,
positions, population, second-order aggregation); moving content to `docs/concepts/`
(coverage already exists); touching the external notes.

## Decisions

1. **Axioms → OpenSpec, laws → arc42.** Axioms are per-query observable behaviour
   (WHEN/THEN scenarios); laws are ∀-quantified equations between evaluations, whose
   honest enforcement is a generative harness (DEV-1869), not example tests — the
   arc42 `[enforced:]` fitness-function machinery. Alternative (all in OpenSpec)
   rejected: not-yet-true statements break the specs-describe-what-IS premise.
2. **`architecture/semantics.arc42.md` is the complete end-target map** — all axioms
   + all six laws, per-clause status tags. Chosen over scattering into
   `system.arc42.md` (bloat) or Linear (not versioned with code) to prevent
   fragmentation: OpenSpec holds only currently-true behaviour; this file always shows
   the whole algebra and its status.
3. **Three-tag vocabulary.** `[enforced: <id>]` = an automatic check would trip on
   violation (id: contract, `arch_check:<check>`, or `test:<pytest path>`; several
   may accumulate; strength grades structural > static > generative > example matrix
   — tag the strongest available, upgrade over time). `[review]` = true today,
   standing fitness-function candidate. `[target: DEV-XXXX]` = envisioned; that issue
   flips the tag in its own PR. Mixed tags on one principle mark clause boundaries
   (e.g. attributability: broadcast `[review]`, mode axis `[target: DEV-1841]`).
4. **Dice–slice deferred to DEV-1841** (its premise, associate mode, does not exist);
   a pointer comment on DEV-1841 records the flip obligation. Compositionality's
   matrix tag covers only the sibling-measure-independence clause; the full law stays
   `[review]` for the DEV-1869 harness.
5. **`system.arc42.md` principle 8** becomes one line doing both jobs — pointer to the
   algebra plus the cardinality invariant (`[enforced:` matrix `]`) — rather than
   deletion (load-bearing) or duplication of the laws.
6. **Normative-residue rule for dissolution:** a statement survives into arc42 only if
   a future PR could violate it; observable behaviour goes to (or already is in)
   OpenSpec; mechanism description dies. Applied file by file (ledger below).
7. **`arch_check` extensions:** validate all three tag kinds anywhere (malformed
   variants flagged, `[target:]` ids must match `DEV-\d+`); every numbered principle
   item in an arc42 file must carry ≥1 valid status tag; orphan detection — every
   `architecture/*.arc42.md` must be `system.arc42.md`, a node's `arc42:` entry, or
   listed in a new `index.yaml` `cross_cutting_arc42:` list (checked both directions).

## Dissolution ledger

| Source (deleted) | Normative residue → | Everything else |
| --- | --- | --- |
| docs/architecture/index.md | engine.arc42.md P1 (typed pipeline) | history (redesign story, module map, deviations) |
| typed-keys.md | core.arc42.md P1–P3 | key tables, hashing/kwargs detail |
| scopes-and-bundle.md | engine.arc42.md P2, P4 | field tables, walkthrough |
| parsing.md | engine.arc42.md P1, P6–P7 | grammar walkthrough (user docs already in concepts) |
| binding.md | engine.arc42.md P2–P3 | walkthrough |
| planning.md | engine.arc42.md P3 | walkthrough |
| stage-planning.md | engine.arc42.md P4 | walkthrough |
| slack-normalization.md | engine.arc42.md P6–P7 | retired-rule history |
| engine-orchestration.md | engine.arc42.md P2 | two-pipeline history |
| errors-and-warnings.md | core.arc42.md P4–P5 | error-class table (runtime self-describes) |
| cross-model-aggregates.md | — (behaviour already in queries specs) | strategy walkthroughs, deviations |
| ranked-aggregates.md | sql.arc42.md P10 | worked SQL |
| composable-attach.md | sql.arc42.md P10–P12; axioms → queries/semantics | roadmap, mechanics |
| sql-generation.md | sql.arc42.md P10–P12 (P-A…P-I already present as sql P1–P9) | P-J parity history, walkthroughs |
| root specs/ (4 files) | — | pre-OpenSpec planning residue; deliberately reverses the DEV-1857 retention note |

Archive/openspec references to deleted paths are historical and non-resolving by
policy.

## Risks / Trade-offs

- [Axiom overclaim] → Codex plan review already narrowed compositionality (value-level),
  the grain guarantee (`distinct_dimension_values=false` carve-out), association
  restriction (loud-exclusion branch), and warnings (implicit only); scenarios map to
  executed-value tests.
- [Contributor detail loss] → accepted deliberately; tests + specs + git history carry
  it; arc42 stays maintainable.
- [Tag rot] → arch_check validates vocabulary and principle-item coverage; `test:` ids
  remain trust-based (existing behaviour).

## Migration Plan

Pure docs/spec/tooling change, one PR; revert = git revert. DEV-1841 comment posted at
implementation time.
