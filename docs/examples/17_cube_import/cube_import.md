# From Cube to SLayer

SLayer imports [Cube](https://cube.dev) data models — cubes and views, in **YAML
or JavaScript** — and turns them into queryable SLayer models. The conversion is
**fully offline**: column types come from Cube's own dimension / measure
declarations, so no database connection is needed to import. This example runs
the whole path on a small jaffle-shop-flavored project.

## The one-liner

```bash
slayer import-cube cube_project --datasource shop_cube --storage .cache/slayer_models
```

`--datasource` is just the SLayer datasource name to file the models under — it
doesn't have to exist or be reachable at import time. To *query* the imported
models you register a datasource of that name (the notebook does this against a
tiny DuckDB) and run SLayer queries as usual. See
[Importing Cube definitions](../../cube/cube_import.md) for the full conversion
reference — what maps, what fails cleanly, and the JSON report.

## What this example shows

The `cube_project/` has four files, exercising the main conversions:

| Cube source | Becomes | Feature shown |
|-------------|---------|---------------|
| `cubes/orders.yml` | a table-anchored model + a join | cube → model; `join` → SLayer join |
| `cubes/customers.yml` | a table-anchored model | the join target (`region` dimension) |
| `views/orders_overview.yml` | a facade model | a Cube **view** → thin model over the join path |
| `cubes/order_facts.js` | an sql-mode model | JS cube; `FILTER_PARAMS` → `{var}` / `{? ?}` |

The JS cube's `FILTER_PARAMS` are the interesting part. Cube's
`FILTER_PARAMS.<cube>.<member>.filter('col')` renders a caller-supplied filter,
and SLayer represents it with [`{variable}` substitution](../14_variable_substitution/variable_substitution_nb.ipynb):

- **`category`** carries `meta.required`, so its pushdown is **required** — a bare
  `p.category IN ({category})`. Omit it and the query raises, naming the variable.
- **`region`** has no `meta.required`, so its pushdown is **optional** — wrapped in
  a block `{? c.region IN ({region}) ?}` that collapses to `(1=1)` when omitted.

Both are set-membership (`IN`) filters, so the importer marks them `list_valued`
in `meta.cube_variables`: pass a list, or a bare scalar the engine wraps into a
one-element list (`region="North"` ≡ `region=["North"]`). The generated model's
`Variables:` inspect line reads `category (required), region`.

## Try it — the notebook

[`cube_import_nb.ipynb`](cube_import_nb.ipynb) is self-contained and fully
offline: it builds a deterministic DuckDB, runs `slayer import-cube`, inspects the
generated models (the `Variables:` line and `meta.cube_variables`), and queries
them — the view, the inferred join, and every `FILTER_PARAMS` behavior above —
checking each answer against gold SQL.

## Further reading

- [Importing Cube definitions](../../cube/cube_import.md) — the conversion reference.
- [Variable Substitution](../14_variable_substitution/variable_substitution_nb.ipynb) —
  the `{var}` / `{? ?}` mechanics imported models rely on, across every raw-SQL surface.
