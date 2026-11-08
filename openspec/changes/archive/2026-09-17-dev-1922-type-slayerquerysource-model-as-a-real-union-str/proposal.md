## Why

`SlayerQuery.source_model` is declared `object | None`, so the LLM-facing JSON schema for
the field is empty, `source_model=42` validates, every consumer carries a four-way str /
`ModelExtension` / dict / fallthrough ladder, and the `filters` key advertised on an inline
extension is silently dropped. The correct union already exists in the wrong layer
(`slayer/ir/source_bundle.py`), with a `Dict[str, Any]` arm that exists only because the
field is untyped.

## What Changes

- `SlayerQuery.source_model` becomes a real, discriminated union
  `str | ModelExtension | SlayerModel | None`, defined once as `SourceSpec` in
  `slayer/core/query.py`: an object carrying `source_name` always validates as an
  extension, any other object as an inline model, and validation happens at query
  construction with single-branch errors.
- Every dict arm over a source spec is deleted (`ir/source_bundle.py`,
  `engine/bundle_builder.py`, `engine/plan.py`, `engine/stage_ordering.py`,
  `engine/schema_drift.py`, `engine/query_engine.py`, `memories/resolver.py`);
  `SourceSpec` is imported from core, never re-declared.
- `ModelExtension` types its lists (`columns: list[Column]`, `joins: list[ModelJoin]`) and
  sets `extra="forbid"`. **BREAKING** for payloads carrying unknown keys — notably the
  never-implemented `filters` — which now fail at construction instead of being silently
  ignored (the feature itself is DEV-1923).
- REST `QueryRequest.source_model` reuses `SourceSpec`; a malformed spec is rejected at body
  validation (HTTP 422), like every other malformed body field.
- `TimeDimension` accepts `column` as an alias of `dimension` (validation only; dumps and
  schema keep `dimension`).
- The `sql_vs_dsl` example and its notebook, which used the dropped `filters` key, are
  rewritten to a working pattern and re-executed.

## Capabilities

### New Capabilities
- `queries/time-dimensions`: the `TimeDimension` input contract — the `column` alias of
  `dimension`, its serialization and advertised schema.

### Modified Capabilities
- `queries/population`: new requirement "Population specs are typed at construction" —
  extension and inline-model objects validate into their typed forms at construction; an
  object with `source_name` is always an extension; unknown extension keys and non-spec
  values are rejected; the schema lists the three forms; the REST body rejects a malformed
  spec at validation.

## Impact

- Code: `slayer/core/query.py`, `slayer/ir/source_bundle.py`,
  `slayer/engine/{bundle_builder,plan,stage_ordering,schema_drift,query_engine}.py`,
  `slayer/memories/resolver.py`, `slayer/facade/translator.py`, `slayer/api/server.py`.
- Schemas: the MCP `query` tool and the REST OpenAPI embed `ModelExtension` and
  `SlayerModel` for `source_model`.
- Tests: three `tests/test_migrations.py` tests pinning the raw-dict shape are rewritten to
  the typed shape (consented); new `tests/test_dev1922_source_model_union.py`.
- Docs: `docs/examples/02_sql_vs_dsl/*` rewritten and re-executed; one sentence each in
  `docs/concepts/queries.md` §ModelExtension and §TimeDimension.
- basedpyright baseline shrinks; no arc42/.c4 change; no schema-version bump (canonical
  dumps are valid input for older readers, raw objects for the new union).
