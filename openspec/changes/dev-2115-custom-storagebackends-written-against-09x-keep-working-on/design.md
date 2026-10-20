## Context

Diffing every `StorageBackend` method signature between `v0.9.12` and `HEAD` shows exactly two abstract-hook changes (see proposal.md — Why). Callers of `_list_memories_rows` are `base.py` (`_memory_ids`, `load_memories`, `list_memories`) and `access_filtered.py` (`_build_view`, its own delegating override); more will come. Arc42: `system.arc42.md` §3.7 (async-first — the wrapper is async), §3.12 (no dataclasses), §3.13 (dispatch fails closed — an unknown result shape raises, never passes through). Storage node principles are unaffected.

**Structural cause.** A hook contract changed with no adapter, so the wrong shape surfaces at arbitrary call sites. The fix is one definition-time adaptation point that covers every subclass and every present and future caller, never per-call-site patches.

## Goals / Non-Goals

**Goals:** the behaviour in `specs/storage/custom-backends/spec.md`, removable in 2.0 by deleting one module and one hook.

**Non-Goals (user-decided):**
- Old-signature overrides of the public `save_model` / `list_memories` (both now receive `failures=`). They were never hooks (`save_model` was documented "must NOT override"), and the failure is a loud `TypeError` naming the argument. The docs upgrade note says to override only the listed hooks.
- A 0.9.x subclass of a bundled backend that calls `super()._list_memories_rows()` and consumes the result as a list: a return-shape shim cannot change how a caller consumes a value.
- An old `save_datasource` body that itself calls `super().save_datasource()` on a class with no genuine `_save_datasource_impl` (in 0.9.12 that reached an abstract no-op, so no real backend does it).

## Decisions

**D1 — One module, one hook.** `slayer/storage/legacy_backends.py` (claimed by the `slayer.storage` package) holds both adapters. `StorageBackend.__init_subclass__(**kwargs)` calls `super().__init_subclass__(**kwargs)` then the two adapters. It runs inside `type.__new__`, before `ABCMeta` computes `__abstractmethods__`, so a rebound `_save_datasource_impl` satisfies the abstract hook.

**D2 — `save_datasource`, decided by the nearest `_save_datasource_impl`.** For a class defining `save_datasource` in its own `__dict__`, look at the nearest `_save_datasource_impl` among the MRO entries before `StorageBackend`:
- *none* (root 0.9.x class): bind the class's own `save_datasource` as `_save_datasource_impl`, marked adapter-made, and delete the class's `save_datasource` so the base template applies. Warn.
- *adapter-made* (0.9.x child of a 0.9.x class): wrap the class's own `save_datasource` so `check_granularities()` runs first. Warn. Rebinding here instead would recurse when the child calls `super().save_datasource()` (template → the child's body → `super()` → template). The double check when the child does call `super()` is cheap and pure.
- *genuine*: untouched, no warning (e.g. a `YAMLStorage` subclass overriding `save_datasource`).

Alternative rejected: treating any class with an inherited `_save_datasource_impl` as new-style (Codex finding) — it lets a 0.9.x child that skips `super()` bypass validation silently.

**D3 — `_list_memories_rows` wrapped at definition, for every override.** Every non-abstract `_list_memories_rows` in a class's own `__dict__` — the bundled YAML, SQLite and access-filtered overrides included, so there is one code path and no exemption — is replaced by an async `functools.wraps` wrapper. A subclass that does not override inherits the wrapper, so nothing is wrapped twice. A tuple of exactly two lists passes through. A list warns and becomes `(list(result), [])`. Anything else raises `TypeError` naming the defining class (`module.qualname`) and the `(memories, failed_rows)` shape. Per-element checks are skipped: they cost a pass over every row, and the type contract covers the elements. The shape cannot be decided at definition time, because return annotations are optional.

**D4 — Warnings.** Plain `FutureWarning` (Python hides `DeprecationWarning` outside `__main__`, and the warning is raised from the backend author's module). The definition-time warning uses a `stacklevel` that points at the author's `class` statement. The `_list_memories_rows` warning is guarded by an explicit warned-once flag per defining class, so filters such as `always` cannot repeat it. Messages name the class, the hook, the new shape and "until SLayer 2.0".

**D5 — Docs and release note.** The Custom Backends example lists every abstract hook with its current signature, plus the imports it needs (`Loaded`, `Memory`, `Embedding`). The `check_datasource_id_collision` sentence points at `_save_datasource_impl`. A single sentence of upgrade note follows. `RELEASE_NOTES_*.md` files are untracked, so the release note goes on a line in the PR description.

## Risks / Trade-offs

- [The wrapper adds an `isinstance` check to every bundled `_list_memories_rows` call] → negligible next to the I/O, and it buys one code path plus fail-closed shape checking for the bundled backends too.
- [A compat-mode backend loses per-document skip-and-warn: one broken memory fails the call] → same as 0.9.x, and documented in the warning's remedy.
- [The test fixture could inherit `_save_datasource_impl` and never exercise the adaptation] → the 0.9.x test variant has no `_save_datasource_impl` anywhere before `StorageBackend`, and the tests assert that `__abstractmethods__` is empty.
