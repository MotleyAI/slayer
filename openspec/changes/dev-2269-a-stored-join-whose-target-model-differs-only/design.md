## Context

Reproduced on a clean `main` (SQLite):
- `orders` joined to `customers` with the model stored as `Customers` saves in either order (a dangling target is accepted), then every query through the join fails with `target 'customers' not in source bundle`.
- `source_model: TIME_SPINE` raises `ModelNotFoundError`; `TIME_SPINE.timestamp` as a time dimension raises `PopulationInferenceError`.
- A model named `Time_Spine` saves (the reserved check is exact).
- A stored join to `time_spine` saves, then every query through the spine fails with `AmbiguousJoinPathError` — with or without a time axis, because the spine already has a synthesized edge to every dataset.

Structural cause: model names reach exact comparisons in non-canonical spellings — about 236 `target_model` reads in about 37 files, and about a dozen comparisons of query text against `time_spine` before binding (`is_spine_query`, `names_spine`, `reads_spine`, population anchors, `spine_datasource`, `_resolve_source_spec`, `query_engine._infer_population_of`).

Principles that apply: system §3.11 (fail at save; query time keeps a fail-closed backstop), §3.8 (one reader of Mode B), §3.5, §3.6; engine §3.8 (models persist verbatim), §3.7 (one slack pass), §3.3 (binder is pure); storage §3.2 (library growth never invalidates a stored artefact), §3.3 (versioned persistence; §3.1 "a load never writes" is a DEV-1844 target), "access is a view"; core §3.5 (typed errors), §3.6 (structured warnings). Both spec deltas attach to cross-cutting capabilities already mapped in `architecture/index.yaml`; no harness edits.

## Goals / Non-Goals

**Goals:**
- A stored join target is always the exact declared spelling of the model it reaches, or reaches no model in any case, so every existing exact comparison is correct.
- Every model-name position a query writes reaches the built-in spine in any case unless quoted.
- No stored definition that fails every query through it is admitted at save.

**Non-Goals:**
- Stored model names a query writes (`source_model`, an inline source's or extension's join targets, extensions' models) — DEV-2102.
- `inspect` / `get_model_or_builtin` resolving `TIME_SPINE` (not a query).
- The exact stored-`time_spine` clash rule (unchanged).

## Decisions

- **D1 — Refuse at save, both directions; repair stored documents on load.** Options: (A) refuse case-only targets at save, forward and reverse, and respell stored documents on load; (B) respell forward at save, refuse reverse, repair on load — needs an engine §3.8 amendment and makes the outcome depend on save order; (C) respell in memory on every load — breaks the stat-keyed YAML model cache, needs `get_model` turned into a template across every backend and test double, and fails nothing at save. Took A: the only option obeying engine §3.8 and system §3.11 without an arc42 edit, and the `models/stored-upgrade` pattern (stored documents repaired on load, live input validated). (user)
- **D2 — The built-in spine joins the case-insensitive model-name namespace.** Options: (A) new any-case variants refused, a stored variant grandfathered (exact spelling wins, other spellings ambiguous); (B) a stored variant becomes a spine clash — invalidates working stored models, which storage §3.2 forbids without a migration; (C) exact-only saves — keeps admitting new collisions. Took A. (user)
- **D3 — A grandfathered spine variant warns wherever it is used**: on load, on save and on every query response that reads it, with the existing structured name-collision warning (loads already mute it when the caller announces). The user asked to "nag each time the weirdly named pre-existing model is used"; Codex finding 4 aligned the load case with `queries/identifier-case`. (user; codex)
- **D4 — Stored joins into the spine are in scope: refused at save in any spelling, dropped on load with a warning.** Options: (A) in scope; (B) out of scope, dangling as today; (C) a separate issue. Took A: the same "saves, then fails every query" class (system §3.11). Dropping on load never changes a working query, because a declared spine join fails every query through it (probe above). Codex finding 8 made the refusal cover the exact spelling too. (user; codex)
- **D5 — Canonical spellings at the store and query boundaries**, not exact comparisons patched at ~250 sites. (default)
- **D6 — Respelling the spine at the query entry emits no warning**: case folding is first-class grammar (`queries/identifier-case`), not a slack rewrite (engine §3.7). (default)
- **D7 — Spine ambiguity is checked once the query's datasource is known**, against that datasource only. Respelling at the entry applies stage precedence and records which spellings it rewrote; the bundle build checks those against the effective datasource's stored models. An exact stored-name match at the entry consults the pinned datasource if any, else every datasource. (codex)
- **D8 — Query-time backstop**: a stored target that equals several models case-insensitively and none exactly fails with `AmbiguousReferenceError` when the join walk follows it. (default)
- **D9 — An ambiguous join target is refused on every validated save, including when the stored document already holds it.** Unlike a collision inside one document, it is a cross-document defect that fails every query (system §3.11); the remedy is to write one exact spelling. Migration write-back skips validation and is unaffected. (codex)
- **D10 — One new typed error, `JoinTargetCaseError(SlayerError, ValueError)`, for both directions**; the ambiguity, reserved-name and spine-join cases reuse `AmbiguousReferenceError`, `ReservedModelNameError` and `TimeSpineError`. Subclassing `ValueError` lets an access-filtered store map a hidden-only rejection to `HiddenContentConflictError`. (default)
- **D11 — Path heads are respelled by one `core.syntax` rewriter** that changes only the head identifier token and preserves every other byte; quoted heads, string literals and malformed text are left unchanged (system §3.8). (codex)
- **D12 — The upgrade is a version step, not an ordinary converter.** `SlayerModel` goes from 21 to 22 through a stored-only no-op registry entry at source version 21; the async cross-document repair runs in `_migrate_and_refine_on_load` when `pre_version < 22`, before `_canonicalize_join_key_spellings`, `_declare_join_keys` and exact-inverse dedup, which look peers up by exact target. It is idempotent: an interrupted write-back repeats on the next load. It follows the existing write-back mechanism; storage §3.1 stays a DEV-1844 target. (codex)
- **D13 — Access-filtered saves check the visible namespace; the full store is the final guard.** In the view, the case-only target check runs before the unknown-join-target check so the error names the declared spelling; a full-store rejection caused only by hidden models surfaces as `HiddenContentConflictError`, naming no hidden model. (codex)
- **D14 — S1 respells only spellings of the built-in spine in the structured positions as well**; stored names there stay with DEV-2102. Codex finding 2 was rejected: the issue excludes stored model names in those positions, not the built-in. rejected (codex)
- **D15 — Codex finding 1 ("dropping the spine join loses stored meaning") was rejected**: the probe shows a declared spine join fails every query through it, so dropping can only turn a failure into success. rejected (codex)
- **D16 — Tests cover a Unicode fold that differs from `.lower()` (`STRASSE` / `straße`) and the migration ordering with join-key repair and exact-inverse dedup on YAML and SQLite.** (codex)

## Risks / Trade-offs

- **DEV-2102 overlap** (unmerged): it adds `respelled_stages` against stored peers in `bundle_builder`. Whichever PR merges second reconciles them by adding the built-in spine to DEV-2102's model tier, so a query-written name is respelled in one pass with one precedence order.
- **Rename friction**: renaming `customers` to `Customers` takes three steps (delete, respell the joins into it, save the new name).
- **Version bump write-back**: every stored model is rewritten once on its first load at version 22, as with every earlier version step.
- **Exact-name lookup at the entry** without a pinned datasource consults every datasource: `Time_Spine` written against datasource `b` while only datasource `a` stores `Time_Spine` is not respelled. This is rare and accepted.
