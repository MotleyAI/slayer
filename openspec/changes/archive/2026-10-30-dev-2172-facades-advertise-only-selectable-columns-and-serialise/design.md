## Context

See proposal.md — Why. Three structural causes, each fixed at the class level:

1. **The facade groups when SQL does not.** `_translate_slayer_select` (`slayer/facade/translator.py:2807`) turns
   every projection into a grouped `SlayerQuery` (top-level `DISTINCT` is dropped as a no-op), so any column that
   cannot be a group key is advertised but unselectable. Core already has a raw-rows mode
   (`distinct_dimension_values: false`, no top-level `GROUP BY`), and opaque columns are already admitted there:
   `check_opaque_grouping_dim(will_group_by=…)` (`slayer/engine/bind_inputs.py:1077`). The list guard
   `check_list_dimension` (`slayer/engine/elaborate_env.py:1671`, called at `bind_inputs.py:1017` / `:1075`) ignores
   the mode.
2. **Six output paths, six encoders.** MCP `_format_json` (`json.dumps(default=str)`) / `_format_csv`,
   `SlayerResponse._format_value` (markdown), REST (`QueryResponse` serialised by Pydantic), the CLI
   (`slayer/cli.py:1745` json, `:1758` table), pg-wire `value_to_text` (`slayer/pg_facade/types.py:68`) and Flight
   `_rewrite_row` (`slayer/flight/handlers.py:403`) each encode values; `list_json_text` (`slayer/core/format.py:189`)
   is shared only for lists and raises on UUID / bytes / timedelta.
3. **Second readers of Mode B.** `slayer/usage_reporting/features/__init__.py` scans with Python `tokenize`;
   `slayer/memories/resolver.py:469-499` scans filters with regexes.

arc42 principles that apply: `system.arc42.md` 6, 10, 13, 17, 18, 19; `python.usage_reporting.arc42.md` 1–5
(fail-silent, vocabulary only, no user-derived values, only `features` imports `core` / `sql`);
`python.core.arc42.md` 3. No `.c4` change: every import used (`protocols → core`, `surfaces → core`,
`usage_reporting.features → core`) is already licensed. Spec attachment: `facade` and `usage-reporting` are node
specs; `models` is cross-cutting; `results` is a new cross-cutting capability mapped in `architecture/index.yaml`.

## Goals / Non-Goals

**Goals:**
- Principles 17, 18 and 19 tagged `[enforced: test:…]` by tests that would fail on a regression.
- One codec module; no output path keeps a value encoder of its own.

**Non-Goals:**
- `INFORMATION_SCHEMA.METRICS` entries are aggregations, not columns; principle 17's test does not select them
  (its cross-model entries stay deliberately refused by the translator-side guard).
- Plumbing a column's `db_type` to output paths, so a driver-decoded JSON scalar is written as that scalar, not
  re-quoted (decision 17).
- Counting `.value` list children in usage reporting (decision 8).
- Importer regexes (`slayer/dbt/converter.py:1046`, `slayer/osi/expression.py:406`) and the stored-version
  migrations (`slayer/storage/sql_string_migration.py`, `slayer/storage/rank_direction_migration.py`): not Mode B
  readers (decision 10).
- Binaries released before this change still drop a newer spool file whole; only readers from this version on keep
  what they can name (decision 18).

## Decisions

1. **List columns are made selectable by SQL row semantics in the facade plus the opaque grouping rule for lists.**
   A facade `SELECT` is raw rows (`distinct_dimension_values: false`) when its select list has no aggregate and no
   catalog metric, it has no `GROUP BY` / `HAVING` / top-level `DISTINCT`, and no resolved filter or order key
   references a measure; otherwise grouped. `check_list_dimension` takes the same `will_group_by` as
   `check_opaque_grouping_dim`, for plain and computed dimensions (computed-dimension checks keep their order).
   Alternatives: hide list columns from catalogs (lists invisible to BI, `.value` unreachable); group on the sole
   PK and read the list as a row-level value (a new functional-dependency mechanism, fails on keyless models — and
   on a keyed model it equals raw rows anyway). (user)
2. **Decimal in JSON is a number with its exact digits**, not an exact string: exact and type-honest, numeric lists
   stay consistent. Today's scalar Decimal cells change from string to number. (user)
3. **New arc42 principle 19**, applied: "**One writer of result values**: every output path encodes result values
   with one codec in `core.format`; none encodes a value itself. [enforced: test:tests/test_dev2172_value_codec.py]".
   No `.c4` change (imports already licensed; the import law cannot tell "calls the codec" apart). (user, approved
   wording)
4. **Bytes are `\x` + lowercase hex** (Postgres `bytea_output = hex`), not base64: Postgres-native and
   unambiguous. (user)
5. **UUID → canonical string; timedelta → ISO-8601 duration; NaN / ±Inf → JSON `null`** (issue-given). Scalar
   float cells keep native `NaN` / `Infinity` on pg-wire (valid `float8` text) and Flight (native float64).
   (default)
6. **Usage function counters are per library name** (`features.functions: dict[FunctionToken, Count]`), not per
   family: DEV-2035 D7's "built-in names" contract, no second classification table, families derivable later.
   (user)
7. **Four new usage flags**: `cast`, `try_cast`, `aggregate_filter`, `list_literal`. No `case` flag — the parser
   folds `CASE` into `iif`, counted as a function. (user)
8. **`.value` list children are not counted**: undecidable from text (a column `value` on a joined model reads the
   same). Alternatives: a last-segment heuristic (knowingly miscounts `orders.value`); counting after binding
   (breaks per-attempt counting and imports engine internals into usage reporting, against its arc42 §2). (user)
9. **`array_length(tags)` stays `custom`**: not a library function; it parses as a custom-aggregation candidate,
   per DEV-2035's "anything else → `custom`" — intentionally unchanged from the issue's probe list. (user)
10. **Principle 18 enforcement**: an AST scan bans `tokenize` imports under `slayer/` except the two stored-version
    migrations (allowlisted: they rewrite superseded stored spellings the current grammar rejects, principle 11);
    behavioural probes guard the two converted readers; `memories/resolver.py` filter regexes are replaced in this
    change. Regex re-reading is not banned mechanically (false positives everywhere `re` reads non-Mode-B text).
    (user)
11. **New cross-cutting capability `results`** (spec `results/value-encoding`); `architecture/index.yaml`
    `cross_cutting_specs` gains `results: {touches: [python.core, python.surfaces, python.protocols]}`, applied.
    Alternative: per-surface specs (rule written two or three times; REST has no spec folder). (user, approved
    wording)
12. **`SELECT DISTINCT` selects the grouped path** (today silently dropped). (default)
13. **No column × custom-aggregation metrics on list columns** in the facade catalog (core always rejects them).
    (default)
14. **Usage-reporting changes are additive under `SCHEMA_VERSION` 1.** (default)
15. **Inline source-model / model-extension measure formulas are scanned** as Mode B fields. (default)
16. **The principle-17 fixture adds `BLOB` and `INTERVAL` columns** to the issue's list / JSON / UUID / DECIMAL.
    (user)
17. **A driver-decoded JSON scalar is written as that scalar**; only dict / list become JSON text. (user)
18. **From this version on, the spool reader drops unknown fields and tokens before validating** and keeps every
    counter it can name; the send model stays strict (principles 2–3 unaffected — dropping only removes data).
    Alternatives: bump `SCHEMA_VERSION` (old binaries still reject; re-shows the notice); keep as is (a newer
    batch is lost whole, again with every vocabulary addition). (user)
19. **The raw/grouped classification also checks measure references in `WHERE` / `ORDER BY`**, so
    `SELECT region FROM t WHERE revenue_sum > 10` stays grouped (raw rows reject measure references). (codex)
20. **The list-guard relaxation covers computed dimensions too**, keeping the existing computed-dimension checks
    first; tested through the engine directly. (codex)
21. **The CLI query output (json and table) is the sixth codec path** and joins the type × path matrix. Rejected
    part: a module-wide ban on `json.dumps` / `str(cell)` (false positives on every non-result use). (codex)
22. **Function names are counted only through a lookup against the payload's own `FunctionToken` set**, misses
    dropped; privacy fuzz tests extend to `functions` and the new flags (arbitrary / Unicode identifiers, parser
    aliases, malformed text, hand-crafted spool payloads). (codex)
23. **The principle-17 test covers joins**: the fixture adds a to-one and a to-many join; every
    `INFORMATION_SCHEMA.DIMENSIONS` row (dotted joined dimensions included) is selected alone, a fanning one
    returning SQL-style repeated rows; `SELECT *` still omits fanning dimensions; one Metabase-style LEFT
    JOIN-subquery case. `INFORMATION_SCHEMA.METRICS` stays out (see Non-Goals). (codex)
24. **Raw-rows time grains are explicit**: `SELECT month(ordered_at) FROM orders` returns one truncated value per
    record; `DISTINCT` / `GROUP BY` give buckets; tests cover aliased / unaliased forms, duplicate timestamps,
    `ORDER BY`, `LIMIT` / `OFFSET`, `DISTINCT` and `GROUP BY`. (codex)
25. **Codec edge rules**: dict keys are strings (a non-string key takes its text form); non-finite Decimals are
    `null` like floats; a Decimal exponent form (`1E+3`) is kept (valid JSON); a timezone-aware datetime keeps its
    offset in the JSON form (pg-wire keeps its UTC conversion); a negative timedelta is `-P…`; tests nest values
    and check every JSON-form output with a strict parser. (codex)
26. **An unparseable memory filter yields one token**, like the other fields; resolver tests cover placeholders,
    quoted literals, `FILTER`, JSON operators, `''` escapes and malformed filters. (codex)

## Risks / Trade-offs

- [Per-record rows change every plain facade projection; Metabase sync / previews or client dashboards relying on
  the implicit dedup see more rows] → documented in `pg-facade.md` / `flight-sql.md`; Metabase e2e expectations
  re-blessed only where the new count is the SQL-correct one; `DISTINCT` / `GROUP BY` keep the grouped result.
- [REST / MCP json Decimal cells change from string to number; clients string-matching break] → release-notes
  item; the JSON number keeps every digit.
- [The codec writes its own JSON; a bug yields invalid JSON on every path] → every JSON-form output in the matrix
  is parsed with a strict parser (`json.loads` with `parse_constant` raising).
- [REST bypasses Pydantic response serialisation for `/query`] → `QueryResponse` stays the declared response
  model (OpenAPI unchanged); a test compares the rendered body's shape to the model.
- [Older binaries delete newer spool files] → bounded loss, now limited to binaries released before this change.

## Migration Plan

No stored-data migration. Behaviour changes ship together; rollback is a plain revert.
