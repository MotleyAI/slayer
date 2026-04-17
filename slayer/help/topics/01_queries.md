# Queries

How the pieces of a `SlayerQuery` fit together. For individual field schemas,
see the `query` tool's own arg documentation.

## Parts of a query

| Part | What it drives |
|------|----------------|
| `source_model` | The model name, an inline model, or a `ModelExtension`. Not the table name. |
| `fields` | The SELECTed columns — measures with `:agg`, arithmetic, transforms. |
| `dimensions` | Plain GROUP BY columns. Dotted paths cross joins. |
| `time_dimensions` | GROUP BY on a truncated time column, plus an optional `date_range`. |
| `filters` | List of condition strings, AND-ed. Route themselves to WHERE / HAVING / post-filter. |
| `order` | List of `{column, direction}`. `column` matches a field name or measure. |
| `limit` / `offset` | Row slicing on the final result. |
| `main_time_dimension` | Which time dim drives transforms when 2+ are present. |
| `whole_periods_only` | Snap `date_range` to bucket edges; drop incomplete current bucket. |

## Evaluation order (the SQL the generator builds)

1. WHERE — filters on dimensions / raw columns, including model-level `filters`.
2. GROUP BY — dimensions plus truncated time dimensions.
3. Aggregate measures → SELECT computed fields.
4. HAVING — filters on aggregated measures.
5. Post-filters — filters on transform / computed fields, applied on an outer wrapper.
6. ORDER BY → LIMIT / OFFSET.

Knowing which stage your filter lands in is why the auto-routing works. See
`help(topic='filters')`.

## Dimensions vs time_dimensions on the same column

A time column can appear in either list:

- In `time_dimensions`: grouped by the **truncated** value (e.g. `month`). One row
  per bucket.
- In `dimensions`: grouped by the **raw** value. One row per distinct timestamp.

Both forms of the same column in one query is allowed if you need per-row
detail plus bucketed rollups.

## Disambiguating the "time" for transforms

Transforms like `cumsum` and `change` need to know which column orders time.
Resolution order:

1. `main_time_dimension` on the query (if set).
2. The single time dimension in `time_dimensions` (if exactly one).
3. The model's `default_time_dimension` — if it is itself present in the
   query's `time_dimensions`.

If none apply, the transform errors. Always set `main_time_dimension` when you
have two or more time dimensions.

## Example combining the pieces

```json
{
  "source_model": "orders",
  "fields": [
    "*:count",
    "revenue:sum",
    {"formula": "change_pct(revenue:sum)", "name": "mom_growth"}
  ],
  "dimensions": ["customers.regions.name"],
  "time_dimensions": [{
    "dimension": "created_at",
    "granularity": "month",
    "date_range": ["2025-01-01", "2025-12-31"]
  }],
  "filters": ["status <> 'cancelled'", "mom_growth > 0"],
  "order": [{"column": "revenue_sum", "direction": "desc"}],
  "limit": 20
}
```

The `status` filter is WHERE; `mom_growth > 0` is a post-filter on the outer
query; `customers.regions.name` walks `orders → customers → regions`;
`change_pct` uses the single `time_dimensions` entry automatically.

## See also

- `help(topic='formulas')` — the colon syntax and arithmetic that power `fields`.
- `help(topic='filters')` — operators, WHERE vs HAVING, post-filters.
- `help(topic='time')` — granularities, whole_periods_only, `last()` distinctions.
- `help(topic='extending')` — `source_model` as a `ModelExtension` or a query name.
