# Formulas

Formulas are the mini-language used inside `fields` (what to compute) and
`filters` (conditions). Parsed with Python's `ast` module — so operator
precedence matches Python.

## Colon syntax

`measure_name:aggregation` is how every aggregated value is expressed.

| Form | Meaning |
|------|---------|
| `revenue:sum` | `SUM(revenue_measure_sql)` |
| `*:count` | `COUNT(*)` — always available, no measure definition |
| `col:count` | `COUNT(col)` — counts non-nulls |
| `col:count_distinct` | `COUNT(DISTINCT col)` |
| `price:weighted_avg(weight=quantity)` | custom-arg aggregation |
| `customers.score:avg` | cross-model — measure from a joined model |
| `customers.regions.population:sum` | multi-hop cross-model |

`*` can **only** combine with `count`. `*:sum`, `*:avg`, etc. are errors.

## Arithmetic

Python-style arithmetic over aggregated measures and literals:

| Operator | Example |
|----------|---------|
| `+` `-` `*` `/` `**` | `"revenue:sum / *:count"` |
| parentheses | `"(revenue:sum - cost:sum) / *:count"` |

Inside a field, use a dict to name the result:

```json
{
  "source_model": "orders",
  "fields": [
    "*:count",
    {"formula": "revenue:sum / *:count", "name": "aov", "label": "AOV"}
  ]
}
```

## Nesting

Transforms (see `help(topic='transforms')`) can wrap measures, arithmetic, or
each other. Arbitrary nesting is allowed:

```json
{
  "source_model": "orders",
  "fields": [
    {"formula": "change(cumsum(revenue:sum))", "name": "cumsum_delta"},
    {"formula": "cumsum(revenue:sum / *:count)", "name": "running_aov"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Each level of nesting becomes an additional CTE in the generated SQL. Turn on
`show_sql=true` if you need to see the shape.

## Filter formulas

The same parser powers `filters`. Left and right of an operator can be a
dimension, a measure with `:agg`, or a transform expression. See
`help(topic='filters')` for operators and routing.

## Gotchas

- Bare measure renames (`{"formula": "*:count", "name": "n"}`) cannot be
  referenced by `n` in `filters` — reference the original `*:count` instead.
- Formulas validate measure names against the source model at query time.
  If you get "measure not found", call `inspect_model` and check the actual
  measure list.

## See also

- `help(topic='aggregations')` — the full list of `:agg` options.
- `help(topic='transforms')` — `cumsum`, `change`, `time_shift`, etc.
- `help(topic='joins')` — dotted paths like `customers.score`.
