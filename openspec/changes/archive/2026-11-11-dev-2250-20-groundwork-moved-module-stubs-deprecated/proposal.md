## Why

The 2.0 line breaks users with no pointers: three modules vanished without the moved-module stub every other move got, `strict` is refused outright although it has an unambiguous replacement, there is no shared way to deprecate an input form, and there is no migration guide. Every later 2.0 slice needs the deprecation mechanism and the guide, so they land first.

## What Changes

- Moved-module stubs for `slayer.engine.syntax` and `slayer.core.formula` (→ `slayer.core.syntax`) and `slayer.osi.expression` (→ `slayer.importers.aggregates`), removed in 3.0.
- One mechanism for deprecated input forms: each conversion emits one payload-carrying `FutureWarning` and records a structured `deprecated_input` payload that every run consuming the converted input reports on `SlayerResponse.warnings`, and every save reports on its warnings channel.
- `strict` on fresh query input (Python, REST `/query`, MCP `query`, `refine`, model and memory saves) is converted to `to_many_handling` with a deprecation instead of refused; removed in 3.0. Stored pre-v4 queries keep their silent migration.
- REST model saves (`POST /models`, `PUT /models/{name}`) return a `warnings` list of structured payloads (deprecations and narrowed-access warnings); memory saves add deprecations to their `warnings` strings.
- A migration guide, `docs/upgrading/2.0.md`: every breaking change already on the 2.0 line, plus what 2.0 deprecates for 3.0.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `compatibility/removal-tripwire`: the marker covers `strict` and the three new moved paths; new requirement for deprecated input forms.
- `queries/attribution-modes`: `strict` becomes a deprecated alias instead of a refusal.
- `mcp/query-tool`: stale `strict` mentions become `to_many_handling`.
- `models/model-access`: REST model saves return the narrowed-access warning.

## Impact

- Code: `slayer/core/deprecation.py`, `slayer/core/warnings.py`, `slayer/core/query.py`, `slayer/core/errors.py`, `slayer/storage/migrations.py`, `slayer/engine/query_engine.py`, `slayer/api/server.py`, `slayer/memories/service.py`, `slayer/client/slayer_client.py`; new stubs `slayer/engine/syntax.py`, `slayer/core/formula.py`, `slayer/osi/expression.py`.
- REST: model-save responses gain `warnings` (additive).
- Docs: new `docs/upgrading/2.0.md` and its `zensical.toml` nav entry; `docs/concepts/queries.md`, `docs/reference/mcp.md`, `docs/reference/rest-api.md`.
- Tests: tripwire `PINNED`/`HELPERS`, `MOVED_MODULES`; three existing `strict`-rejection tests become conversion tests.
