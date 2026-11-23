## Context

See proposal.md (Why). On `main` the seven query input objects in `slayer/core/query.py` — `SlayerQuery`, `QueryRefinement`, `QueryDimension`, `QueryMetric`, `TimeDimension`, `OrderItem`, `DatasetExtension` — handle unknown keys three ways: `QueryDimension`/`QueryMetric` raise `UnknownKeyError` from a hard-coded key tuple (`_refuse_unknown_entry_keys`); `SlayerQuery`, `OrderItem`, `QueryRefinement`, `DatasetExtension` rely on `extra="forbid"` (bare `extra_forbidden`); `TimeDimension` has no config, so Pydantic drops the key. None sets `validate_assignment`. Pre-2.0 spellings convert inside each object's wrap validator via `converted_input` + `slayer/core/vocabulary.py` (`Spellings.rename`), and a parent's vocabulary pass also converts its nested entries. Dataset-side objects (`DatasetPart` in `slayer/core/models.py`) already refuse fresh unknown keys and drop stored ones with a warning (DEV-1607).

**Symptom → structural cause.** The `dateRange` bug is one instance of: nothing guarantees every query input object refuses unknown keys through one path. The fix is one refusal path for all seven objects, guarded by a law test that walks every reachable query model, plus a normative principle (`python.core` §3.8), rather than adding config to `TimeDimension` alone. Grounding: `python.core` §3.5 (typed errors, stable format), system §3.6.

**Applicable principles.** `python.core` §3.5, §3.7 (core imports no other node — the change stays inside `core`); new §3.8; system §3.6, §3.11 (stored queries keep the same check on load as fresh input).

## Goals / Non-Goals

**Goals:**
- One input path per query object: camelCase heal → pre-2.0 conversion → refusal of what remains.
- Atomic validated assignment on the seven objects.
- A law test that fails when a reachable query model escapes either rule.

**Non-Goals:**
- Validating `model_copy(update=…)` — internal copies are code, not input; the principle says "input".
- camelCase heal on dataset documents.
- Dataset-side assignment validation — DEV-2398 (gates 2.0).
- A 1.x backport.

## Decisions

1. **camelCase keys heal silently and generally (user).** Options: refuse with a hint naming the snake_case key, or heal. Taken: heal — DEV-2081's product rule (input with exactly one reading heals silently, no error) governs; generalised to every query input object instead of a per-key alias (the issue's original `Field(alias="dateRange")` proposal). The user first chose refusal, then switched once DEV-2081 was surfaced.
2. **One typed refusal path for all seven query objects, enforced by a law test (user).** Options: `TimeDimension` only plus a forbid-only law test; all objects. Taken: all — fixes the class, one message format (§3.5).
3. **`validate_assignment=True` on all seven query objects (user).** Options: `TimeDimension` only; all seven. Taken: all seven; a probe forcing it on all seven passed the full unit suite (52,329 passed, 0 failed).
4. **arc42 principle (user, approved wording).** `architecture/python.core.arc42.md` §3 gains, verbatim:
   "8. **Input refuses what it does not read**: a fresh query or dataset object carrying a key it does not read is refused with a typed error naming the key, never silently dropped. [enforced: test:tests/test_law_query_input_refuses_unknown_keys.py] [review]"
5. **Dataset-side assignment validation is out of scope (user).** A probe forcing it on every `slayer/core/models.py` model gave ~163 unit failures and a hang; filed as DEV-2398, a 2.0 blocker.
6. **DEV-1695 gates 2.0 (user).** It turns accepted input into refused input — breaking, so 2.0 only.
7. **Stored unknown keys are refused; that document fails to load individually (default).** Query precedent (`SlayerQuery`/`QueryDimension` already refuse stored unknown keys); dataset-style drop-and-warn would be the silent-wrong-numbers bug itself.
8. **Heal before pre-2.0 conversion, against the full accepted-spelling map (codex).** The heal map per object is every key it accepts — canonical and pre-2.0 spellings — so `sourceModel` → `source_model` → `population` (with its deprecation warning). Conflicts are detected on the final canonical key: `sourceModel` + `population`, `dateRange` + `date_range` → the existing `SpellingConflictError`. Healing must run wherever a key set is renamed, including a parent's vocabulary pass over nested entries, so it lives with spelling conversion in `vocabulary`, not per model.
9. **Refuse only keys that are not model fields; list only advertised keys (codex).** `OrderItem.raw_expression` and `implied` are real fields that round-trip through storage (`slayer/storage/qualified_references.py:144`) but are hidden from the input schema; refusing on the advertised schema would break stored order items. Accepted set = model fields (today's set, no regression); the message's "takes …" list = the advertised input-schema properties. Derive both from the model — no hard-coded key tuples.
10. **Failed assignment is atomic (user, from codex).** Pydantic's `validate_assignment` raises on a failed model-level after-validator but leaves the new value assigned, and some validators (`_dedupe_time_dimensions`) mutate other attributes. A shared mixin snapshots the instance state (`__dict__`, fields-set) before each assignment and restores it on `ValidationError`; probe-verified. Rejected: stock Pydantic behaviour, which leaves an invalid object behind a caught error.
11. **Error location = Pydantic's `loc` (default, from codex).** Nested refusals surface inside the `ValidationError` with the path to the entry (`time_dimensions.0`); tests assert it for the root and each nested entry kind. Rejected: threading parent paths into the message — Pydantic's location already carries it on every surface.
12. **Law test shape (codex).** It walks every Pydantic model reachable from `SlayerQuery` / `QueryRefinement` field annotations, stopping at dataset-side models (`DatasetPart` subclasses, `SlayerDataset`). Per model: `extra="forbid"`; `validate_assignment`; a minimal valid payload plus a stray key raises `UnknownKeyError` naming it; the camelCase check runs only where the model has a multi-word accepted key, and the full inventory of such keys is pinned so a new multi-word field cannot escape. A minimal-payload table fails when a reachable model has no entry.
13. **Entry-path coverage (codex).** Tests cover dict construction, building from an already-built instance, assignment, the refinement merge, and the storage round-trip; `model_copy` stays a non-goal (decision 1's Non-Goals).
14. **Dedicated hints (default).** `compareDateRange` names `change`, `change_pct`, `time_shift`; the existing `TimeDimension` `name` hint stays.
15. **Spec placement (default).** `compatibility/ossie-vocabulary` (sibling of "Datasets refuse unknown keys") and `queries/time-dimensions`; no new capability.

## Risks / Trade-offs

- [Stored saved queries with junk keys stop loading] → only that document fails, with a typed error naming the key; called out in `docs/upgrading/2.0.md`.
- [Stored queries with `dateRange` change results (they now filter)] → intended: they meant the range; called out in `docs/upgrading/2.0.md`.
- [Silent camelCase heal hides the canonical key from agents] → accepted per DEV-2081; serialization is always canonical.
- [Assignment snapshot cost] → shallow copy per assignment on seven objects that production code rarely mutates (it uses `model_copy`).
- [Existing tests pin `extra_forbidden`] → `tests/test_dev2102_extensions_ir.py:100,109`, `tests/test_dev1922_source_model_union.py:148-158`; aligned with the user's approval in pr-tests.
