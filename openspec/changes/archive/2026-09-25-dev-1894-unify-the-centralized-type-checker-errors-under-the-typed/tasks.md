## 1. Failing tests (spec-tests stage)

- [x] 1.1 Base ctor unit tests: keyword-only; renders `Class: summary` + optional `  at` / `  scope:` / `  suggestion:` lines; attributes set (`None` when absent); `QueryTypeError(...)` itself raises `TypeError` — verify red before 2.1
- [x] 1.2 Per-family table test: one real query per family (spec scenarios) asserting the exact class, `isinstance` of `QueryTypeError` / `SlayerError` / `ValueError`, first line `== f"{Class}: {exc.summary}"`, `at` / `suggestion` lines and attributes where expected — verify red
- [x] 1.3 Time-axis: `TimeAxisError`, `not isinstance(e, NotImplementedError)`; REST `/query` answers 400 with detail starting `TimeAxisError:` — verify red
- [x] 1.4 Flight SQL: a checker type error surfaces as invalid-argument (not unimplemented) carrying the error text — verify (may already pass via pyarrow; the pin is the deliverable)
- [x] 1.5 `WindowDurationError` from `parse_window_duration` directly for non-string, `''`, `'d90'`, `'0d'`, and through a query `sum(revenue, window='90x')` — verify red
- [x] 1.6 `SlayerQuery(distinct_dimension_values=False, measures=[...])` fails with `DistinctDimensionValuesError: ` text whose underlying error is a `QueryTypeError` — verify red
- [x] 1.7 MCP `_format_resolution_error` prints the class name once for a stably formatted error — verify red
- [x] 1.8 Parity-ratchet self-checks: a scratch checker row whose exc is `ValueError`, or the `QueryTypeError` base, is red — verify red against the extended assertion
- [x] 1.9 Ledger + parity test (design D5): `_collapsed` reads summary/location/scope/suggestion kwargs; slayer-root-relative scanned modules incl. `core/window_duration.py`; checker rows' `exc` / `message` rewritten to the target classes and collapsed D3 messages; new `_WD` rows — verify the parity test is red against today's code

## 2. Error family (`slayer/core/errors.py`)

- [x] 2.1 Add `QueryTypeError` base + the 13 family classes; re-parent `TimeDimensionColumnError` (under `TimeAxisError`), `MeasureNameCollidesWithColumnError` / `CanonicalAliasShadowsColumnError` / `DuplicateMeasureNameError` (under `NameCollisionError`, D3 schema, attributes kept), `PositionTypingError`, `DistinctDimensionValuesError` — verify 1.1 green

## 3. Convert raise sites

- [x] 3.1 `elaborate_env.py`: every checker raise → its D2 class with D3 `summary` / `location` / `suggestion` keywords; inline `flatten_collision_message`; `check_window_duration` just calls the parser — verify 1.2 / 1.3 / 1.9 green
- [x] 3.2 `core/window_duration.py`: every arm → `WindowDurationError`, plus a non-string guard — verify 1.5 green
- [x] 3.3 `core/query.py::_validate_distinct_dimension_values` → keyword ctor, D3 schema — verify 1.6 green
- [x] 3.4 MCP `_format_resolution_error` no double prefix — verify 1.7 green
- [x] 3.5 Flight: add explicit type-error → invalid-argument mapping ONLY if 1.4 is red — verify 1.4 green

## 4. Ratchets and test lockstep

- [x] 4.1 Guard ratchet: drop the `^A time-ordered transform …` `ALLOWED_EXPRESSIVENESS` entry — verify `tests/test_law_guard_ratchet.py` green
- [x] 4.2 Mechanical, assertion-preserving retargets: `test_dev1835_guards.py:104`, `test_dev1903_transform_inputs.py:64/65/81/82`, `NotImplementedError` → `TimeAxisError` in `test_dev1832_transform_source.py:270/280/286` and `test_dev1946_transform_parameter.py:223/232`, any `match=` spanning subject + defect → `.location` / `.summary`. Anything not mechanical: STOP and ask — verify each file green
- [x] 4.3 Goldens: list every moved key in its module's `ALLOWED_DELTAS` with the reason (dev1739, 1740_regroup, 1750, 1824, 1839, 1868, 1900, 1908, 1909, 1958), re-bless with `SLAYER_UPDATE_GOLDEN=1 poetry run pytest <module>`, empty the manifests; dev1900's 7 `query_engine.py:535` keys must not move — verify all golden modules green with empty manifests

## 5. Architecture and docs

- [x] 5.1 Apply the approved arc42 text (design D7) to `architecture/core.arc42.md` P4 and `architecture/engine.arc42.md` P9 — verify `poetry run python tools/arch_check.py` green
- [x] 5.2 One sentence in `docs/concepts/queries.md` describing the error layout (`ClassName:` first line, optional `at` / `suggestion:` lines; every type error is a `QueryTypeError`) — verify the page renders in `zensical.toml` nav (already linked)

## 6. Gates

- [x] 6.1 `poetry run pytest -m "not integration" -n auto`, the integration suite with the CI invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `npx -y likec4@1.47.0 validate architecture` — all green
