## 1. Tests (pr-tests)

- [ ] 1.1 Add `tests/test_dev2429_moved_frame_shift.py`: the moved-frame seed (orders → customers, North 2023-12 20, 2024-01-10 30, 2024-02 40, 2024-04 30, 2024-05 5, 2024-06 7) and one executed test per scenario of "time_shift evaluates its operand in the moved frame" on SQLite and DuckDB; verify each fails on the current code for the documented reason (or passes where today's value is already right, e.g. plain `time_shift(sum, -1)`)
- [ ] 1.2 Cover moved bounds staying frame bounds: a moved frame combined with `window=`, nested `cumsum` and a nested shift; open-ended ranges (lower only, upper only) and a relational frame bound (`ordered_at >= …`) beside `date_range`; verify by executed values
- [ ] 1.3 Cover routing: shifts used only in a filter, only in the order, hidden inside a composite measure, and nested under another transform all read the moved frame; verify by executed values
- [ ] 1.4 Golden SQL on postgres, sqlite, duckdb, tsql and bigquery for a shifted predicate, a shifted nested transform and a shifted cross-model composite; verify strict dialects render the predicate answer as a value
- [ ] 1.5 Ask the user's approval to align `tests/test_dev1868_series_shift_exec.py` (date-range edge expectations become the moved-frame values; series-SQL-shape pins such as one `OVER (` and one `= 'a'` move to the producer form) and any other existing test pinning the series regime, listing each file and change; then align

## 2. Planner: one shifted evaluation

- [ ] 2.1 Remove `_series_mode` / `_series_flags` (`slayer/engine/compile/shift.py`), the `series=` emission flags and the `routes()` non-series exemption (`slayer/engine/compile/discovery.py`) and `_shift_candidates`' `series is False` filter (`slayer/engine/compile/stages.py`), so every `time_shift` root reaches `_plan_shifted_attaches`; verify 1.3
- [ ] 2.2 Replace `_frame_free_filters`' frame stripping and `shifted_spine_bounds` with one moved-frame function: move every frame bound on the axis's column (date_range included) to whole source buckets per design decision 4, keep every other filter unchanged; spine and non-spine call it; verify the unaligned and open-ended scenarios
- [ ] 2.3 Pass the moved bounds to the synthesized producer as frame bounds on its time dimension (not plain row filters); verify the windowed scenario and 1.2
- [ ] 2.4 Make the producer's interning identity include its moved frame; verify "Two offsets read their own moved frames" and "Two offsets share one shifted evaluation"
- [ ] 2.5 Remove the spine-only `series=False` override in `_emit_planned`; verify "Spine and non-spine agree" and `tests/test_dev2218_spine_rows.py`

## 3. IR and SQL generator

- [ ] 3.1 Remove `ValueSlot.series` and its readers (`slayer/ir/planned.py`); verify with basedpyright and the affected tests
- [ ] 3.2 Remove `_series_shift_cte` and the `slot.series` branch in `_emit_time_shift_ctes_for_planned` (`slayer/sql/generator.py`); verify 1.1–1.4

## 4. Docs and harness

- [ ] 4.1 `docs/concepts/expressions.md`: rewrite the `time_shift` paragraphs (~358, ~362) to the one rule — the shifted operand is evaluated with the frame moved by the shift, an axis-free leaf keeps its in-frame value — removing the series-regime and cross-dataset-leaf sentences; verify no "series regime" / "materialised series" wording remains in `docs/`
- [ ] 4.2 `slayer/memories/help_content/05_time.md`: one sentence that a shifted running total reads the moved range's running total (`time_shift(cumsum(x), -1, 'year')` = prior-year YTD); verify the help-content tests
- [ ] 4.3 Flip `[target: DEV-2429]` in `architecture/semantics.arc42.md` Axiom 11.3 to `[enforced: test:tests/test_dev2429_moved_frame_shift.py]`; verify `poetry run la-arch-check`

## 5. Gates

- [ ] 5.1 Run the local test scope (`la-affected-tests --base main` plus goldens, `tests/test_dev1868_*`, `tests/test_dev1958_*`, `tests/test_dev2218_*`, `tests/test_dev1732_frame_bound_filters.py`), ruff, basedpyright, `la-arch-check`, `openspec validate dev-2429-time-shift-changes-value-when-its-operand-is --strict`; all green
