# Slack normalization

**Module:** `slayer/engine/normalization.py` (warning types in
`slayer/core/warnings.py`)

The pipeline begins (principle **P0**) with a single pass that rewrites
*slack-but-unambiguous* agent input into canonical form, so every downstream
stage sees only the canonical shape. Each rewrite is returned as a typed
`NormalizationWarning` and surfaced two ways at once.

The layer has shrunk over time as slack forms were promoted to first-class
grammar: `DOT_PATH_IN_SQL` retired when dots became the canonical Mode-A join
separator (DEV-1743), and `FUNC_STYLE_AGG` retired when the parser made the
functional aggregation spelling a first-class equivalent of colon syntax
(DEV-1826). What remains are *shape* rules — rules about where a value
belongs, not how it is spelled. Formula **text** is never rewritten.

## The active rules

| Rule | Kind | Detects | Effect |
| --- | --- | --- | --- |
| `MISPLACED_MEASURE` | query shape | a bare (no colon, no call) entry in `query.measures` that names a column | moved to `query.dimensions` |
| `MALFORMED_DATE_RANGE` | report-only | a `time_dimensions[i].date_range` that is not two elements | warning only — the planner's silent drop becomes visible |

### `MISPLACED_MEASURE`

Mirrors the legacy `_auto_move_fields_to_dimensions` heuristic but emits a
structured warning. A bare token in `measures` that names a known `ModelMeasure`
stays a measure; one that names a column moves to `dimensions`; an unknown token
is left for the downstream resolver to error on. It is a no-op when the stage has
no resolved model (a sibling-sourced stage), because column classification needs
the model's column names.

### `FUNC_STYLE_AGG` (retired — DEV-1826)

Historically this rule rewrote functional aggregations (`sum(revenue)`,
`count(*)`, `percentile(amount, p=0.5)`) into colon form before parsing, with
a warning nudging agents toward the colon spelling. DEV-1826 inverted the
premise: the functional spelling is now **first-class grammar** — `parse_expr`
dispatches it natively to the same `AggCall` node as colon syntax (see
[Parsing](parsing.md)), unknown names defer to the binder exactly like
`x:whatever`, and a same-model expression source (`sum(amount - cost)`) is
legal. There is nothing to rewrite and nothing to warn about; saved models
keep the author's spelling. The rule, its helpers, and the quiet
`func_style_agg_to_colon` variant (formerly used by schema-drift attribution
and memory entity tagging, which now parse natively) were deleted, along with
the reachable-custom-aggregation BFS that existed only to feed the rewrite.

The legacy regex rewriter in `slayer/core/formula.py` still serves the
importer pipelines (cube/dbt/OSI); consolidating those onto the native parser
is DEV-1831.

### `DOT_PATH_IN_SQL` (retired — DEV-1743)

Historically this rule rewrote a dotted Mode-A join path
(`customers.regions.name`) into the `__` split-alias form
(`customers__regions.name`) at the `engine.execute` / `engine.save_model`
boundary. DEV-1743 makes dots the **canonical** Mode-A separator, so there is
nothing to rewrite: a dotted path stays dotted, and resolution to the internal
`__` JOIN alias happens structurally in the shared resolver
(`column_expansion.expand_derived_refs_sync`), not by a textual normalization.

The inverse is now a **hard error** rather than a rewrite target. A legacy `__`
split-alias qualifier that does not name a real (exact) join target but whose
naive split would walk the graph raises `LegacyDunderAliasError` at two doors:
generation time (the resolver runs during SQL generation) and save time
(`SlayerQueryEngine._validate_mode_a_join_paths` in `save_model`). A `__` token
that IS an exact directly-joined model name resolves normally — `__` is a legal
identifier character now, matched exactly, never split. Stored pre-v9 models
carrying the old split-alias form are migrated to dotted on load by the v9
storage migration.

## Warning shape and dual surfacing

```python
class NormalizationWarning(BaseModel):       # slayer/core/warnings.py
    rule_id: str                 # "MISPLACED_MEASURE"
    original: str                # "status"
    normalized: str              # "dimensions += 'status'"
    location: str                # "measures[2].formula"
    rule_doc_url: Optional[str]  # "docs/agent_input_slack.md#misplaced-measure"

class SlayerNormalizationWarning(UserWarning):
    """Carrier UserWarning around a NormalizationWarning payload."""
```

Every rewrite is surfaced **both** as a Python warning
(`warnings.warn(SlayerNormalizationWarning(payload))`, so
`warnings.catch_warnings()` callers see it) **and** appended to
`SlayerResponse.warnings: List[NormalizationWarning]` (so REST/MCP/CLI consumers
get the structured payload alongside the result). One source of truth, two
surfaces. The payload Pydantic type lives in `slayer.core.warnings` rather than
in the engine module so storage/REST schemas can reference it without importing
engine code.

## Entry points and boundaries

- `normalize_query(query, *, model)` — runs `MISPLACED_MEASURE` over the query
  shape and `MALFORMED_DATE_RANGE` over time dimensions, returning a
  `NormalizationResult(query, warnings)`.

It is invoked at one engine boundary: `engine.execute` (per stage, via
`_normalize_stage`). `engine.save_model` persists the model **verbatim** —
since DEV-1826 no normalization pass touches model text at all. CLI / REST /
MCP go through those entry points automatically. See
[Engine orchestration](engine-orchestration.md) for the call sites.

## Design rationale

- **Why did FUNC_STYLE_AGG move into the parser instead of staying a rewrite?**
  The rewrite had to be replicated at every text surface (query measures,
  filters, model measures at save, order coercion, schema drift, memory
  tagging) and each missed surface was a bug — `ModelExtension` measures and
  hand-authored YAML never passed through it. One parser branch covers every
  position by construction, and both spellings collapse to one node so
  identity, naming, and matching are spelling-insensitive for free.
- **Why typed warnings rather than logging?** Agents (and the REST/MCP consumers
  driving them) need to *see* that their input was reshaped, structurally, so
  they can learn the canonical form. A log line is invisible to them; the
  `rule_doc_url` points at the canonical-form documentation.
