## 1. Tests first (pr-tests stage)

- [ ] 1.1 `tests/test_dev2172_value_codec.py`: type × path matrix — list, dict, UUID, bytes, timedelta (incl. negative), Decimal (exact, exponent, non-finite), float NaN / ±Inf, date / naive and tz-aware timestamp, nested inside lists and dicts — through MCP json / csv / markdown, REST `/query`, the CLI json and table output, pg-wire text + binary, Flight; every JSON-form output parsed strictly (`parse_constant` raising); verify each case fails before the codec exists
- [ ] 1.2 Real-client round-trips: `psycopg` against an in-process pg facade and a pyarrow Flight client against an in-process Flight server, over a DuckDB fixture with each value type; `collect` over a UUID column on DuckDB (unit) and Postgres (integration, `pytest-postgresql`, TCP-only options per the sandbox); verify they fail today
- [ ] 1.3 `tests/test_dev2172_honest_catalogs.py` (unit, in-process DuckDB): fixture model with list, JSON, UUID, DECIMAL, BLOB, INTERVAL columns plus a to-one and a to-many join; select alone every column in pg `information_schema.columns` / `pg_attribute`, Flight `GetTables(include_schema)` and `INFORMATION_SCHEMA.DIMENSIONS`; fanning dimension returns repeated rows; `SELECT *` omits it; one Metabase-style LEFT JOIN-subquery case; verify the list column fails today
- [ ] 1.4 Facade projection semantics (`facade/projection` scenarios): plain projection keeps duplicates; `DISTINCT` / `GROUP BY` / `HAVING` / aggregate / metric / measure in `WHERE` or `ORDER BY` stay grouped; raw-rows time grains (aliased, unaliased, duplicates, `ORDER BY`, `LIMIT` / `OFFSET`, `DISTINCT`, `GROUP BY`); `GROUP BY skills` names `skills.value`; catalog has no `skills_my_agg`
- [ ] 1.5 Engine-level list rules (`models/list-columns` scenarios): raw-rows plain and computed list dimensions project per record; grouping ones keep the typed error and its precedence; ordering by a list is refused
- [ ] 1.6 `tests/test_dev2172_one_mode_b_reader.py`: AST scan — no `tokenize` import under `slayer/` except the two stored-version migrations (allowlist with reason); usage probes (`sum(x)::int`, `CAST(…)`, `FILTER (WHERE …)`, `count(DISTINCT x)`, `array_length(tags)` → `custom`); memory filter `"region" = 'x'` tags `region`; verify the scan and probes fail today
- [ ] 1.7 Usage-reporting tests: formula → expected counts table covering every `usage-reporting/collection` scenario and each library family; a query with an expression in every position (measures, computed / plain dimensions, time dimensions, filters, order, inline model measures); vocabulary test pinning `FunctionToken` to `core.keys.SCALAR_FUNCTIONS`; privacy fuzz extended to `functions` and the new flags; spool tolerance (`usage-reporting/delivery` scenarios)
- [ ] 1.8 Memory resolver tests: placeholders, quoted literals, quoted identifiers, `FILTER`, JSON operators, `''` escapes, malformed filters (one token)

## 2. Value codec (principle 19)

- [ ] 2.1 Implement the codec's JSON form and text form in `slayer/core/format.py` (own JSON writer for exact Decimals; edge rules per design decision 25); `list_json_text` and `_json_scalar` folded into it; verify 1.1's codec-level cases pass
- [ ] 2.2 Route MCP `_format_json` / `_format_csv` and `SlayerResponse._format_value` through the codec; verify the MCP rows of 1.1 pass
- [ ] 2.3 Route REST `/query` through a codec-rendered body, keeping `QueryResponse` as the response model; verify the REST rows of 1.1 pass and OpenAPI is unchanged
- [ ] 2.4 Route the CLI query json and table output through the codec; verify the CLI rows of 1.1 pass
- [ ] 2.5 Route pg-wire `value_to_text` / `value_to_binary` and Flight `_rewrite_row` through the codec (DOUBLE → float, INT → int from Decimal); verify the wire rows of 1.1 and 1.2 pass

## 3. Selectable facade columns (principle 17)

- [ ] 3.1 `check_list_dimension` takes the query's `will_group_by` at both call sites in `slayer/engine/bind_inputs.py`, computed-dimension checks first; refuse a list `order` key; verify 1.5 passes
- [ ] 3.2 Facade translator: raw-rows classification (no aggregate / metric in the select list, no `GROUP BY` / `HAVING` / `DISTINCT`, no measure reference in resolved filters or order) sets `distinct_dimension_values: false`; `SELECT DISTINCT` selects grouped; verify 1.4 passes
- [ ] 3.3 Facade catalog: no column × custom-aggregation metrics over list columns; verify 1.3 and 1.4's catalog case pass

## 4. One reader of Mode B (principle 18)

- [ ] 4.1 `slayer/core/syntax.py`: make the whole-tree walker public; verify the existing syntax tests pass
- [ ] 4.2 Rewrite `slayer/usage_reporting/features` over the parse tree (every position; function lookup against the payload vocabulary, misses dropped); `payload.py` `Features.functions`, the four flags, `merge`; recorder accumulation; verify 1.6 and 1.7 pass
- [ ] 4.3 Spool reader drops unknown fields / tokens before validating (`slayer/usage_reporting/spool.py`); verify the spool-tolerance tests pass
- [ ] 4.4 `slayer/memories/resolver.py`: filters through `parse_expr` + `walk_parsed_refs`; delete the filter regexes and blacklist; verify 1.8 passes

## 5. Architecture and docs

- [ ] 5.1 `architecture/system.arc42.md`: principle 17 → `[enforced: test:tests/test_dev2172_honest_catalogs.py]`, 18 → `[enforced: test:tests/test_dev2172_one_mode_b_reader.py]` (19 already applied); verify `poetry run la-arch-check` passes
- [ ] 5.2 `docs/interfaces/pg-facade.md` + `docs/interfaces/flight-sql.md`: list / JSON values as JSON text, NULL list NULL, per-record vs grouped projection, `GROUP BY` list → `.value`, encodings; remove the issue numbers at `pg-facade.md:286`, `:302-308`
- [ ] 5.3 `docs/reference/mcp.md:109` + `docs/reference/rest-api.md:47`: output encodings; REST `distinct_dimension_values: false` projects lists
- [ ] 5.4 `docs/reference/usage-reporting.md`: the `functions` field and the four flags (privacy notice lists every field)

## 6. Verification

- [ ] 6.1 Full unit suite `poetry run pytest -m "not integration"`, ruff, basedpyright (ratchet), `la-arch-check` all pass
- [ ] 6.2 CI integration suite (incl. `pg-facade-e2e`, `metabase-e2e`) passes; any Metabase expectation re-blessed only where the new per-record count is the SQL-correct one, each named in the PR description
