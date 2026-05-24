# Errors and warnings

**Modules:** `slayer/core/errors.py`, `slayer/core/warnings.py`

The redesign replaced anonymous `ValueError`s scattered through enrichment with a
typed error vocabulary. Each error carries the offending input, a scope summary,
and (where feasible) a did-you-mean suggestion, and renders with a **stable
`str()` format** so tests can snapshot it.

## The stable message format

`_format_error_message` builds every stage-5 error's message in one shape:

```text
<ErrorName>: <one-line summary>
  at <location>
  scope: <short scope summary>
  suggestion: <did-you-mean>
```

The first line always begins with the class name, so log greps and snapshot
tests bind to a stable prefix; the indented lines are optional.

## The error classes

| Class | Raised when |
| --- | --- |
| `UnknownReferenceError` | a bare or dotted ref doesn't resolve in scope |
| `AmbiguousReferenceError` | a ref matches multiple candidates in scope |
| `IllegalScopeReferenceError` | a dotted ref against a `StageSchema`, or `__` in a `ModelScope` without an exact match |
| `IllegalWindowInFilterError` | raw `OVER(...)` in a DSL filter, or a filter referencing a windowed `Column.sql` |
| `AggregationNotAllowedError` | type-bucket / PK / `allowed_aggregations` violation |
| `UnknownFunctionError` | a Mode-B call not in `SCALAR_FUNCTIONS` / transforms / aggregations |
| `MeasureRecursionLimitError` | named-measure expansion exceeded depth (32, env-configurable) |
| `MeasureCycleError` | a cycle in named-measure expansion |
| `DuplicateMeasureNameError` | two measures declare the same `name` |
| `MeasureNameCollidesWithColumnError` | a declared `name` matches a source column |
| `CanonicalAliasShadowsColumnError` | a formula's canonical alias shadows a source column |

### `ValueError` multi-inheritance for back-compat

`UnknownReferenceError`, `AmbiguousReferenceError`, `IllegalScopeReferenceError`,
and `IllegalWindowInFilterError` multi-inherit `ValueError` (alongside
`SlayerError`). This is deliberate: the cutover replaced legacy `ValueError`
resolution paths with these typed errors, and many pre-existing call sites and
tests catch `ValueError` (or use `pytest.raises(ValueError)`). Multi-inheriting
keeps them working unchanged. `ColumnCycleError` (DEV-1410) does the same.

`SlayerError` is the base for SLayer's intentional failure modes, so callers can
distinguish them from unexpected `Exception` paths (driver errors, IO errors).

## Warnings

Two warning types are **not** exceptions:

- `SlayerNormalizationWarning` (`core/warnings.py`) — a `UserWarning` carrying a
  `NormalizationWarning` payload, emitted by the
  [slack layer](slack-normalization.md) on every rewrite. Surfaced both via
  `warnings.warn(...)` and on `SlayerResponse.warnings`.
- `UnreachableFilterDroppedWarning` (`core/errors.py`) — a `UserWarning` emitted
  by the [cross-model planner](cross-model-aggregates.md) when a host filter
  references slots unreachable from a CTE's root, so the filter is dropped from
  the CTE (the host still applies it to its own rows). A visibility/debug
  warning, not an error.

## Design rationale

- **Why typed errors over `ValueError`?** The legacy enrichment raised bare
  `ValueError`s that callers couldn't distinguish, so error handling was
  string-matching on messages. Typed classes let surfaces (REST/MCP/CLI) and
  tests react to the *kind* of failure, and the `.name` / `.scope_summary` /
  `.suggestion` attributes make programmatic remediation possible.
- **Why a stable `str()` format?** So snapshot tests can pin the message
  (including suggestion text and scope summary) without brittle substring
  matches, and so agents reading the error get a consistent, parseable shape.
- **Why keep `ValueError` in the MRO?** A clean break would have churned every
  `except ValueError` call site and test in the same PR as the cutover. Multi-
  inheriting defers that churn without weakening the new typed surface — callers
  that want the specific class can catch it; callers that catch `ValueError`
  still work.
