## Context

The Linear issue DEV-2173 is the itemised work list (sections A–D, file:line refs at `649ecc2b`, unchanged at
the base `81184c79`). See proposal.md for motivation.

Constraints:
- Pages owned by the sibling follow-ups are not touched:
  - DEV-2169: `docs/database-support.md`, `references.md:217-219` / `:242`;
  - DEV-2170: `schema-drift.md`, the drift lines `ingestion.md:359` / `:364`, `models.md` §Schema versioning;
  - DEV-2171: `docs/cube|dbt|osi/*_import.md`;
  - DEV-2172: `interfaces/pg-facade.md`, `interfaces/flight-sql.md`, `reference/mcp.md:109`,
    `reference/rest-api.md:47`.
- Repo and user rules:
  - each doc change is as short as possible, usually one sentence;
  - every new page goes into the `zensical.toml` nav;
  - no issue numbers in docs or code, except forward pointers in code;
  - no tests that assert wording;
  - an edited notebook is re-executed with its real outputs saved.
- Architecture:
  - system §3.9: dotted-canonical spelling in every example;
  - system §3.10: Mode A (free SQL: `Column.sql`, `Column.filter`, model `filters`, `sql_table`) is kept apart
    from Mode B (formulas, query fields), and docs say which surface a rule is for;
  - core P4: the reworded brace error stays the same typed error.
- No arc42 or `.c4` edit. The `mcp` capability is already mapped on the `surfaces` bucket.

## Goals / Non-Goals

**Goals:**
- Every A item is corrected, and every B item is documented at the place the issue names.
- Every docs page is in the nav, except the allowlisted ones.
- No issue numbers remain in `docs/`, beyond the DEV-2172 baseline.
- New guard tests keep those properties.
- The `query` texts' example idioms are all covered by execution tests.

**Non-Goals:**
- Validator leniency for `Column.type` / `element_type` spellings (DEV-2174).
- `docs/getting-started/flight-sql.md` (DEV-2103 adds it to the nav and fixes its token caveat).
- Behaviour that sibling PRs settle (out-of-domain NULL, reserved-name shadowing, list projection in facades).
  Their sentences describe today's code and are reconciled in the merge commits.

## Decisions

### Docs and agent text
- **A14 (probed).** The `==` sentence in `help_content/03_aggregations.md` stays, made precise:
  - after an aggregation's first argument, `name = value` is a keyword argument, so `first(x, a = b)` binds
    the keyword `a`, and a comparison there needs `==`;
  - a keyword's value may itself be a single-`=` comparison (`weight=a = 1`).
  (default)
- **A6 (probed).** An empty list variable binds only as a list-function argument
  (`contains_all(tags, {wanted})`). As the whole right-hand side of a Mode-B `IN`, an empty list raises, as it
  does on Mode-A raw SQL. A literal `x IN []` is rejected. A numeric string (`'10'`) also binds in a bare slot.
  (codex, corrected by probe)
- **Column types.** Every text gives the canonical uppercase names `TEXT, INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP,
  UNKNOWN`, and `ARRAY` with a required `element_type` from that list. Legacy lowercase aliases are documented
  nowhere as input. (user)
- **Issue numbers.** All of them go from every file type under `docs/` outside the sibling-owned pages,
  including those the issue doesn't list (`ingestion.md:104`, `storage.md:62`,
  `docs/examples/09_lightning_talk/setup_talk.py:1`). The inventory runs before the guard is turned on.
  (codex)
- **DEV-1768 drift items.** The case-sensitivity claim, the `order[].column` conflict and `*:count` → `_count`
  are each verified against the current docs and fixed where they remain. (default)

### Orphan pages
- **Delete** `docs/interfaces/{cli,mcp,python-client,rest-api}.md`, after folding their still-accurate unique
  content into the reference pages:
  - `reference/cli.md`: `SLAYER_NOW`, how the `slayer query` positional argument is read,
    `--variables KEY=VALUE` / `--variables-json`, the conceptual-help section;
  - `reference/mcp.md`: the help-memories section and topic table, the `edit_model` parameter list (replacing
    the dangling "See params below");
  - `reference/rest-api.md`: the run-by-name curl example and allowed-field list, the query-backed
    `POST /models` example with the `columns` → 400 rule.

  Keeping both copies would leave two diverging versions of four reference pages, so it isn't an option.
  (user)
- **Retarget tests.** `tests/test_help_seed.py:472` / `:477` read `reference/cli.md` / `reference/mcp.md`
  with the same assertions; `tests/test_docs_dev_1560.py` drops `interfaces/mcp.md` from `_MCP_DOCS`. (user)
- **Notebook 08** joins the nav under Tutorials as "MCP Introspection", after Aggregations. Its narrative moves
  from the deprecated `inspect_model` to `inspect`, and it is re-executed. (user)
- **Flight SQL** (`getting-started/flight-sql.md`) stays an orphan; DEV-2103 owns it (comment posted there).
  The exception lives only in the nav-coverage test; CLAUDE.md and CONTRIBUTING.md are unchanged, because the
  test is the authority and its forward pointer makes the exception expire with DEV-2103. (user)
- **Reference gaps are filled**, each entry checked against the code:
  - `reference/mcp.md`: `validate_models`, `recommend_root_model`, `set_datasource_priority`,
    `get_datasource_priority`;
  - `reference/rest-api.md`: `/datasources/priority`, `/recommend-root-model`, `/validate-models`;
  - `reference/cli.md`: `recommend-root-model`, `import-cube`, `storage`, `pg-serve`, `flight-serve`.

  (user)

### MCP query schema
- **Budget.** `QUERY_SCHEMA_BUDGET` in `tests/test_mcp_surface_budget.py` is set to the measured size after the
  additions plus ~10%, with its comment updated. Rejected alternative: keeping 28,250 and moving the rules to
  help pointers, which hides new grammar from agents that skip `inspect`. (user)
  - rejected (codex): "keep the 28,250 budget". The archived design note asks to stop and ask before raising
    it; we did, and the user chose to raise it.
- **Field additions** are one clause each (see the spec delta):
  - `measures`: `FILTER (WHERE …)`;
  - `filters`: the null-aware operators, `CAST` and the list functions;
  - a query object's `variables`: the binding rules;
  - `source_model`: the per-query expression form, replacing the inline-extension-column advice.
- **Markers.** The new guidance markers join `MARKER_OWNERS` (one owner among the query texts), not
  `SOLE_OWNER_MARKERS`, so the help topics may repeat them. (default)
- **OpenSpec.** A `mcp/tool-surface` delta, not `skip_specs`. Otherwise the "every example executes" SHALL would
  go silently false. (user)
- **Example audit.** Every example in the `query` texts was audited against the idiom requirement. The missing
  ones are added, each with a scenario:
  - year-over-year `time_shift`;
  - `ntile` order;
  - granularity calls;
  - the fixed-grain computed dimension;
  - time points;
  - regex braces;
  - list filters.

  The new examples are added too: `FILTER`, the null-aware tests, the variable binding forms. (default,
  consistency over scope)
- **Execution tests.** Every new idiom bullet gets its own executable test with concrete rows and expected
  output. (codex)

### Structural guards (new unit tests, default suite)
1. **Nav coverage.** Every `.md` / `.ipynb` under `docs/` is in the `zensical.toml` nav. Allowlist:
   `docs/CLAUDE.md`, `docs/api_gaps.md`, and `docs/getting-started/flight-sql.md` with a forward pointer to
   DEV-2103. (user)
2. **No issue numbers.** No `DEV-<n>` in any file under `docs/`, except `docs/CLAUDE.md`. A temporary exact
   baseline lists the (path, token) pairs that `docs/interfaces/pg-facade.md` has today, with a forward pointer
   to DEV-2172. Any other occurrence fails. The baseline is deleted in the merge that brings DEV-2172 in.
   (user + codex)
3. **Doc examples parse.** A schema-aware extractor:
   - parses JSON / YAML query and model objects from the fenced blocks of `docs/**/*.md` and
     `help_content/*.md`, and from `docs/examples/**/*.yaml`;
   - selects named Mode-B fields recursively: query `measures`, `dimensions`, `time_dimensions`, `filters`,
     order `column`, stage lists, `refine` clauses, model measure `formula`;
   - skips Mode-A keys (`sql`, `sql_table`, model `filters`, `Column.filter`);
   - asserts `parse_expr` accepts each value, reporting file and line on failure.

   Prose backticks aren't extracted. Fixture tests cover the extractor: mixed SQL/query fences, multiline
   lists, stage lists, refinements, malformed snippets and ignored prose. (user + codex)

### Dead code
- **The `escape="python"` regime goes.** Removed with it:
  - `substitute_variables`' `escape` parameter, since SQL is the only regime left (an internal signature
    change; the function is re-exported only from `slayer.ir.variables`);
  - `_C0_*`;
  - the python branches.

  The one production caller (`slayer/ir/variables.py:186`) drops the argument. (default)
- **Tests:**
  - python-specific tests are deleted: the trailing tuple comma, backslash / C0 encoding, the python-regime
    optional-block rejection, the invalid-regime check;
  - regime-agnostic `TestSubstituteVariables` tests keep their assertions with `escape=` removed and run on the
    SQL regime with `backslash_escapes=False`;
  - every `escape="sql"` argument in tests goes;
  - `pr-tests` confirms that the SQL-only contract is covered (mandatory `backslash_escapes`, optional blocks,
    empty-list error, per-dialect quote/backslash escaping), adding any missing test.

  (user + codex)
- **The brace error** (`slayer/core/syntax.py:259`) reads "a brace expression is not a variable placeholder;
  write a variable as {name}.". The `match=` strings in `tests/test_mode_b_placeholder_parse.py:71` and
  `tests/test_mode_b_variables_construction.py:64` follow. (user)
- **Stale comments go:** `query.py:206-207`, `slayer/sql/window_detect.py:1-8`,
  `tests/test_sql_boolean_literal_filters.py:1-8`, together with their non-forward issue / PR references.
  `_column_summary` (`slayer/mcp/server.py:470`) is deleted. (default)

## Risks / Trade-offs

- [The doc-example extractor misses a new snippet format] → It fails loudly on a malformed recognised block,
  and has fixture tests; prose snippets are a stated non-goal.
- [Sibling PRs change behaviour this PR documents] → This PR merges last; each sibling merge commit reconciles
  the affected sentences and the DEV-2172 baseline.
- [The raised budget invites future growth] → The new ceiling stays a hard test limit; the next raise needs the
  same explicit approval.
- [Folding orphan content drops something] → The comparison inventory (in this file) lists each folded item;
  the review checks each landed.
