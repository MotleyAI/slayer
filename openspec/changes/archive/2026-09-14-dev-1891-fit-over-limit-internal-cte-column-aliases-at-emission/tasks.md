# Tasks — dev-1891 fit over-limit internal identifiers at emission

## 1. Failing test suite (spec-tests stage)

- [x] 1.1 Create `tests/test_dev1891_internal_identifier_fit.py` (patterns from
      `tests/test_dev1756_identifier_length.py`, fixtures from `tests/_dev1824_fixtures.py`);
      verify the file collects and every test pinning NEW behaviour fails against current
      branch code (premise / no-churn / exemption regression pins may already pass).
- [x] 1.2 Repro tests: nested_attach-shaped query on postgres emits no over-limit
      SLayer-minted identifier; fitted name byte-identical at definition and every
      reference; SQL still parses (`sqlglot.parse_one`).
- [x] 1.3 No-churn tests: under-limit query emits byte-identical SQL; unbounded dialects
      (sqlite, clickhouse) keep canonical over-limit aliases unchanged.
- [x] 1.4 Prefix-collision regression: two internal aliases identical up to the limit emit
      distinct fitted identifiers AND executed results (forced limit ≥ MIN_LIMIT=16,
      monkeypatched `max_identifier_bytes` on an executing dialect) equal the
      unlimited-run results.
- [x] 1.5 Exemption tests: over-limit physical identifier in user `Column.sql` survives
      byte-identical; same via `Column.filter`, model `filters`, `sql_table`,
      `ModelExtension`-added column, and a non-root named stage's concrete user model.
- [x] 1.6 Guard tests: forced `_digest` collision → `IdentifierCollisionError`; fitted
      form equal to an existing token → `IdentifierCollisionError`; surviving non-exempt
      over-limit token → typed backstop error; identical short names in independent
      scopes pass untouched.
- [x] 1.7 Masking tests: alias text inside string literals (incl. `''` doubling), `--`
      and `/* */` comments survives byte-identical while identifier occurrences are
      fitted; candidates with embedded quote chars are skipped; non-ASCII byte budgets.
- [x] 1.8 BQ/T-SQL composition tests: fitted-then-mangled identifier within post-mangle
      byte budget; result keys decode to canonical dotted names.

## 2. Fitting machinery

- [x] 2.1 Add masking + token helpers to `slayer/sql/_identifier_fit.py`
      (`overlimit_tokens(text, *, limit)`, `find_overlimit_quoted(sql, *, limit, quote_open,
      quote_close)`, shared literal/comment mask); verify 1.7 unit tests pass.
- [x] 2.2 Make `substitute_quoted` literal/comment-safe via the shared mask; verify
      existing DEV-1756 suite plus 1.7 pass.
- [x] 2.3 Extend `SqlDialect.rewrite_emitted_sql(sql, aliases, exempt=frozenset())` in
      `slayer/sql/dialects/base.py`: scan-derived mapping unioned with the projection
      aliases, per-dialect quote anchors, collision guards, fitted via
      `fit_alias`; verify 1.2/1.3/1.6 pass.
- [x] 2.4 Add the always-on post-rewrite backstop helper (typed error naming token and
      limit); verify 1.6 backstop test passes.

## 3. Generator wiring

- [x] 3.1 Build the exemption set once per `generate_planned_stages` call from the
      enumerated bundle surfaces (design decision 2); thread `exempt` to both
      `rewrite_emitted_sql` call sites and call the backstop beside
      `maybe_validate_scopes`; verify 1.5 passes.
- [x] 3.2 Update the stale "not a length regex" docstring on `rewrite_emitted_sql`;
      verify `poetry run ruff check slayer/ tests/` is clean.

## 4. Goldens and full verification

- [x] 4.1 Re-bless exactly the 4 over-limit golden entries (`lift/nested_attach` ×
      postgres/tsql/bigquery/duckdb) via `SLAYER_UPDATE_GOLDEN=1`; verify the diff
      touches only those entries and each fitted alias is within its dialect limit.
- [x] 4.2 Full unit suite green: `poetry run pytest -m "not integration" -n auto`.
- [x] 4.3 Tier-1 integration green: `poetry run pytest tests/integration/ -m integration`.
- [x] 4.4 Docs: one sentence on identifier-length handling in
      `docs/database-support.md` if not already stated; verify page is in `zensical.toml` nav.
