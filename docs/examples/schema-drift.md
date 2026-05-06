# Worked example: detecting and fixing schema drift

This walk-through assumes you have the bundled Jaffle Shop demo set up.
If not:

```bash
slayer datasources create demo --ingest --yes
```

## 1. Cause some drift externally

Connect to the underlying DuckDB and drop a column. We'll use the
`sku` column on `products`:

```bash
duckdb ~/.local/share/slayer/demo/jaffle_shop.duckdb \
  -c "ALTER TABLE products DROP COLUMN sku"
```

## 2. Inspect the drift

```bash
slayer validate-models --datasource demo
```

Output:

```text
EDIT MODEL: products (datasource: demo)
  drop columns: sku
    - column:sku: Live column 'sku' not found
```

The output is a punch list of replayable operations. Each entry maps
directly onto an `edit_model` or `delete_model` call.

## 3. Try a query that touches the drifted column

```bash
slayer query '{"source_model": "products", "dimensions": ["sku"]}'
```

The engine catches the DBAPI error, runs `validate_models` against the
touched models, and surfaces a `SchemaDriftError` with the structured
delete payload. Compared to a raw "column not found" trace, you get a
direct pointer to the diff and the suggested next step:

```text
slayer validate-models --datasource demo
slayer validate-models --datasource demo --force-clean
```

## 4. Apply the deletes

Use `--force-clean --yes` to apply non-interactively (the `--yes` flag
auto-approves the apply prompt):

```bash
slayer validate-models --datasource demo --force-clean --yes
```

The CLI applies each entry through the engine's storage helpers, then
re-runs `validate_models` to confirm there's no residual drift:

```text
EDIT MODEL: products (datasource: demo)
  drop columns: sku
    - column:sku: Live column 'sku' not found

Applied 1 entry/entries.

✓ no remaining drift
```

(Without `--yes`, the CLI prints `Apply 1 delete(s) to storage? [y/N]`
and waits for input.)

## 5. Re-ingest to recover

If the live schema gains new columns again, the idempotent ingest pass
adds them without overwriting your customised descriptions / labels /
formats:

```bash
slayer ingest --datasource demo
```

Output:

```text
Updated: products (+columns: ...)
```

If type drift is detected on existing columns, it shows up under
"Pending drift" in the same output — re-run `slayer validate-models
--force-clean` (or apply manually via `edit_model`) and then re-ingest
to pick up the new live type.
