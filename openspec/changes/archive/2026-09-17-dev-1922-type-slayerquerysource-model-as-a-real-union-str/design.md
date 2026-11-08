## Context

See proposal.md › Why. `SlayerQuery.source_model: object | None` (`slayer/core/query.py`)
has been untyped since the field was widened from `str`; the intended type lives as
`SourceSpec = Union[str, SlayerModel, ModelExtension, Dict[str, Any]]` in
`slayer/ir/source_bundle.py`, one layer up. Migrations run in a `mode="before"` validator on
`SlayerQuery` (query v1→v2 rewrites an inline extension's `dimensions` → `columns`) and on
`SlayerModel` (v4→v5 walks raw `source_queries[].source_model` objects), so every legacy
shape is rewritten before field validation. `SlayerModel.source_queries` is a bare `list`
whose before-validator already coerces every item to a `SlayerQuery`. `slayer/core/__init__.py`
is empty. Storage backends and the REST handler dump with `exclude_none=True`. Pydantic
2.13.1.

## Goals / Non-Goals

**Goals:** one `SourceSpec` in `core`, consumed by `ir`, `engine`, `memories`, `facade`,
`api`; validation at the boundary with single-branch errors; every dict arm gone;
behaviour-preserving (goldens byte-identical, suite green, enforcement bundle green).

**Non-Goals:** typing `SlayerModel.source_queries` as `list[SlayerQuery]` (follow-up issue —
a mutual recursion across two modules with an empty package init needs its own
import-order design); adding `filters` to `ModelExtension` (DEV-1923); `extra="forbid"` on
`TimeDimension` or `SlayerModel`; typing `QueryListRequest.queries`; a
`RequestValidationError` → 400 handler; the `isinstance(…, str)` dispatch in
`engine/compile/stages.py` (str-vs-resolved-model, not dict arms).

## Decisions

1. **Discriminated union, not a smart union.** `SourceSpec` is
   `Annotated[Union[Annotated[str, Tag("name")], Annotated[ModelExtension, Tag("extension")],
   Annotated[SlayerModel, Tag("model")]], Discriminator(_source_spec_tag)]` with the tag
   function: str → name; `ModelExtension` instance or dict with `source_name` → extension;
   `SlayerModel` instance or any other dict → model; anything else → name (so a
   non-spec value fails as "not a string"). Rationale: a plain smart union decides
   membership by whichever class happens to accept the dict, and `SlayerModel` keeps
   Pydantic's default `extra="ignore"` (persisted-file forward compatibility depends on it),
   so `{"source_name": …, "name": …, "sql_table": …, "data_source": …, "filters": […]}`
   would validate as an inline model, silently dropping `source_name` and `filters` — a
   bypass of the very `extra="forbid"` this change adds, and a regression against every
   consumer's existing "has `source_name` ⇒ extension" rule. The discriminator states that
   rule once; Pydantic applies it at every boundary using the alias (`SlayerQuery`,
   `QueryRequest`, nested `source_queries`); errors name only the classified branch's keys.
   Alternatives: `extra="forbid"` on `SlayerModel` (changes the model-file loading contract);
   a before-validator on `SlayerQuery` rejecting both keys (a band-aid at one boundary).
   Consequence: Pydantic renders a discriminated union as `oneOf`, so the schema is
   `anyOf: [{oneOf: [string, ModelExtension, SlayerModel]}, {type: null}]`; the three forms
   are advertised, as the issue requires, in a nested shape.
2. **`SourceSpec` lives in `core.query`.** `ir/source_bundle.py` and `engine/*` import it;
   `ir` still imports only `core` (ir P2), no new arrow. `__all__` of `source_bundle` drops
   it.
3. **`ModelExtension`: typed lists + `extra="forbid"`.** `columns: list[Column] | None`,
   `joins: list[ModelJoin] | None`. Closes the silent-drop bug by rejection; `filters` as a
   feature is DEV-1923. The overlay's `model_validate`-if-dict arms become dead and are
   deleted.
4. **`SlayerQuery.source_model_name` property replaces `_get_source_model_name`.** A
   `match` over the three forms (str → itself, extension → `source_name`, model → `name`,
   None → None); the two callers (`strip_source_model_prefix`, query-engine reporting) use
   it.
5. **Typed dispatch keeps fail-closed tails.** `_resolve_source_spec` keeps its `raise`
   tail (core P3); `_walk_spec`, `_collect_query_backed_base_names`,
   `_resolve_stage_source_to_base`, `_stage_join_targets` / `_stage_extension_hops` and the
   memories resolver replace `getattr` duck typing with `isinstance` over the union, same
   semantics (joins are read from both extensions and inline models, as the duck typing
   did).
6. **REST: 422.** `QueryRequest.source_model: SourceSpec | None`; a malformed spec fails
   FastAPI body validation like every other body field (was 400 via the downstream
   `SlayerQuery`). The existing `model_dump(exclude_none=True)` → `SlayerQuery.model_validate`
   path round-trips typed objects.
7. **`TimeDimension`: `AliasChoices("dimension", "column")`.** `dimension` first, so the
   schema and dumps keep `dimension`; `column` is a compatibility alias for DEV-1893, which
   reverses the order in its own subclass. No `extra` config: with both keys present the
   first alias wins, as for any Pydantic alias.
8. **No schema-version bump.** Canonical dumps (typed specs with explicit defaults) are
   valid input for older readers, which validated late from raw objects; raw objects are
   valid input for the union. Persisted and wire shapes stay minimal because storage and
   the REST handler already exclude none-valued fields.
9. **basedpyright baseline shrinks only.** Regenerate and confirm the diff is deletions.

## Risks / Trade-offs

- [Earlier validation changes where an error surfaces] → every dict-form `source_model`
  site in `tests/` was probed: all are full extensions or full inline models (the one
  without `data_source` is a valid query-backed inline model); no caller fills a partial
  model in later. The three migration tests that pinned the raw-dict boundary are rewritten
  (consented).
- [Schema size] → the MCP `query` tool schema now embeds `SlayerModel`; accepted by the
  issue (the three forms must be advertised).
- [`oneOf` vs the issue's literal `anyOf`] → same three forms; the pin test targets the
  measured nested shape.
- [A dict carrying both `source_name` and model keys] → rejected on the extension branch
  with every foreign key named (measured), never silently re-classified.
- [Notebook filter vacuity] → the rewritten example's threshold is chosen while executing
  so the filtered rows visibly differ from the unfiltered cell.

## Migration Plan

None required: no persistence version changes. A stored query-backed model whose stage
carried an unknown extension key would now fail to load; none exist in the corpus or the
examples (the only user of `filters` was the rewritten `sql_vs_dsl` example).
