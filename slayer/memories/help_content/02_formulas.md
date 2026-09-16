# Formulas

Formulas are the mini-language used inside `measures` (what to compute) and
`filters` (conditions). Parsed with Python's `ast` module — so operator
precedence matches Python.

## Colon syntax

`measure_name:aggregation` is how every aggregated value is expressed.

| Form | Meaning |
|------|---------|
| `sum(revenue)` | `SUM(revenue_measure_sql)` |
| `count(*)` | `COUNT(*)` — always available, no measure definition |
| `count(col)` | `COUNT(col)` — counts non-nulls |
| `count_distinct(col)` | `COUNT(DISTINCT col)` |
| `weighted_avg(price, weight=quantity)` | custom-arg aggregation |
| `avg(customers.score)` | cross-model — measure from a joined model |
| `sum(customers.regions.population)` | multi-hop cross-model |

`*` can **only** combine with `count`. `sum(*)`, `avg(*)`, etc. are errors.

## Arithmetic

Python-style arithmetic over aggregated measures and literals:

| Operator | Example |
|----------|---------|
| `+` `-` `*` `/` `**` | `"sum(revenue) / count(*)"` |
| parentheses | `"(sum(revenue) - sum(cost)) / count(*)"` |

Inside a field, use a dict to name the result:

```json
{
  "source_model": "orders",
  "measures": [
    "count(*)",
    {"formula": "sum(revenue) / count(*)", "name": "aov", "label": "AOV"}
  ]
}
```

## Nesting

Transforms (see `memory:help.transforms`) can wrap measures, arithmetic, or
each other, but they follow compatibility rules. In particular `change`,
`change_pct`, and `time_shift` must wrap an aggregated measure directly; window
transforms such as `cumsum` can wrap those (the reverse is rejected):

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "cumsum(change(sum(revenue)))", "name": "cumulative_change"},
    {"formula": "cumsum(sum(revenue) / count(*))", "name": "running_aov"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

Each level of nesting becomes an additional CTE in the generated SQL. Turn on
`show_sql=true` if you need to see the shape.

## Saved formulas (named measures)

A model's `measures` list is a library of named formulas. Queries reference
them by **bare name** in any formula position — root, inside transforms,
inside arithmetic:

```yaml
# model
measures:
  - {name: aov, formula: "sum(revenue) / count(*)"}
  - {name: aov_pct, formula: "change_pct(aov)"}
```

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "aov"},
    {"formula": "cumsum(aov)"},
    {"formula": "aov * 1.1", "name": "aov_with_markup"}
  ]
}
```

Bare references are inline-expanded at parse time. Saved formulas can
reference other saved formulas; cycles raise. Names matching built-in
transforms (`cumsum`, `change`, `time_shift`, …) are rejected at model save.

## Filter formulas

The same parser powers `filters`. Left and right of an operator can be a
dimension, a measure with `:agg`, or a transform expression. See
`memory:help.filters` for operators and routing.

## Gotchas

- Bare measure renames (`{"formula": "count(*)", "name": "n"}`) can be
  referenced by either `n` or `count(*)` in `filters`.
- Formulas validate measure names against the source model at query time.
  If you get "measure not found", call `inspect(reference="<model>", entity_type="model")`
  and check the actual measure list.

## See also

- `memory:help.aggregations` — the full list of `:agg` options.
- `memory:help.transforms` — `cumsum`, `change`, `time_shift`, etc.
- `memory:help.joins` — dotted paths like `customers.score`.
