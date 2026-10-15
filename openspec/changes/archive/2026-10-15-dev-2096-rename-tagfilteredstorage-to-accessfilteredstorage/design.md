## Context

See proposal.md. The class is defined only in `slayer/storage/tag_filtered.py`; no production module imports it (callers construct it themselves). Applicable principle: `architecture/storage.arc42.md` §3.2 (**Access is a view**), which names the class.

## Goals / Non-Goals

**Goals:** one name everywhere outside OpenSpec change docs.

**Non-Goals:** renaming the module's private helpers (`can_access`, `_View`, …) or the `access_tags` field; any behaviour change; a version bump.

## Decisions

- **D1 — no alias.** The old module and class disappear outright; DEV-2014 has not started, so nothing depends on them.
- **D2 — rename via `dr-refactor`** (`la:deterministic-refactor`), gated by `la-typecheck`, `dr-mock-lint` and the full non-integration suite. Its scope is Python only, so the DEV-2023 archive cannot be touched; verified anyway by an empty `git diff main -- openspec/changes/archive`.
- **D3 — done-when grep excludes `openspec/changes/`.** Change docs, active or archived, are the historical record; this change's own proposal must name what it renames. Check: `git grep -E "TagFilteredStorage|tag_filtered|[Tt]ag-filtered" -- . ':!openspec/changes'` returns nothing.
- **D4 — docs concept name follows the class.** Heading `## Access-filtered storage` (anchor `#access-filtered-storage`) and the `models.md` link move together, so no link dangles.
- **D5 — tracker edits land at the end of `pr-review`**, before merge, each shown to the user first: DEV-2014's body (its canonical plan) names `AccessFilteredStorage` in all three places and says it ships in the release after 1.2.0, which the DEV-2034 pin must reach; DEV-2034 gets a comment saying its pin moves to that release.
- **D6 — no new tests.** No behaviour changes; the five `tests/test_model_access*.py` files switching to the new import path are the failing suite until the rename lands.

## Risks / Trade-offs

- [External code importing `slayer.storage.tag_filtered` from 1.2.0 breaks] → only DEV-2014 planned to; D5 updates it before it starts.
- [Bookmarks to `#tag-filtered-storage` land at the page top] → anchor is days old; accepted.
