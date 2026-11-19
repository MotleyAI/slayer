## Why

DEV-2318 renamed SLayer's identifiers to the Ossie vocabulary (dataset, field, metric, relationship, expression), and DEV-1607 brought shapes and docs to their final form. Prose still uses the old words `model`, `column`, `measure`, `join` and `formula`: messages, warnings, help text, tool descriptions, docstrings, comments, `docs/`, `examples/`, notebooks and test prose. `compatibility/ossie-vocabulary` already requires that help topics, tool descriptions and messages use the new words, so this change makes the code meet it. Nothing stops new code from bringing old-word identifiers back.

## What Changes

- **Prose respell** in `slayer/` (messages, warnings, errors, help topics, MCP tool and field descriptions, server instructions, CLI help, docstrings, comments), `docs/` (including the 2.0-behaviour sections of `docs/upgrading/2.0.md` Part 1), `examples/`, the notebooks under `docs/`, and test prose (docstrings, comments, assertion messages, old `--type` and tool-name strings in shared helpers). Each hit is judged in context against the carve-outs. Edited notebooks are re-executed in place.
- **Doc page and example path renames**: `concepts/models.md` → `concepts/datasets.md`, `concepts/formulas.md` → `concepts/expressions.md`, `examples/05_joins/` → `examples/05_relationships/`, `examples/05_joined_measures/` → `examples/05_joined_metrics/`, `examples/16_list_columns/` → `examples/16_list_fields/`, `examples/09_lightning_talk/slayer_models/` → `slayer_datasets/`, and `examples/comparisons/slayer/extra_models/` → `extra_datasets/`. Links are rewritten by resolving each one to its target, and `zensical.toml` gains a redirect for each moved page.
- **Identifier guard**: a test fails on any old-word identifier defined in `slayer/` that its allowlist does not list. The allowlist has `carve_outs` (each with a reason) and `stragglers` (DEV-2364's work list). A ceiling in `architecture/index.yaml` caps the `stragglers` count and may only be lowered.
- Existing tests that pin respelled message text are aligned to the new text.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. The prose requirement is already in `compatibility/ossie-vocabulary`, and no spec scenario quotes a message this change respells (`skip_specs: true`).

## Impact

- **Code:** prose across `slayer/`; no identifier, signature or behaviour changes.
- **Docs:** seven paths move. The published site (docs.motley.ai, built by MotleyAI/motley-docs) copies only SLayer's `nav`, so the redirects reach it through DEV-2368, which must ship before the release that contains this change.
- **Tests:** a new guard test with its allowlist fixture; message-pinning tests aligned; test prose respelled.
- **Architecture:** `architecture/index.yaml` gains `x-vocabulary-stragglers: {baseline: N}`, read by the guard. No arc42 or LikeC4 edit.
- **Follow-ups:** DEV-2364 renames the stragglers and test identifiers. DEV-2368 carries the redirects to docs.motley.ai.
