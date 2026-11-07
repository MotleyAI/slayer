## 1. Tests (pr-tests stage, all in `tests/test_retype_finding.py`)

- [x] 1.1 Add `"remedy_kind"` to `RETYPE_FIELDS` (the only existing-test change: it pins the new serialized field in `TestOneShape` and `_assert_live`); verify it fails before implementation
- [x] 1.2 Add a helper asserting `remedy` agrees with `remedy_kind` (`reingest` ↔ re-ingest text naming the datasource; `set_type` ↔ text naming `live_type`) and call it on every finding the finding-producing tests see; verify it fails before implementation
- [x] 1.3 Table-backed live list (`listy.skills`) → `remedy_kind == "reingest"`; verify failing
- [x] 1.4 SQLite storage-probe widened INT (`TestSqliteProbe.test_probes_only_live_integer_columns`: `amt`, `amount` over a live `INTEGER` column) → `remedy_kind == "reingest"`, while `code` (live declared `TEXT`, a bucket conflict, not a probe verdict) → `"set_type"`; verify failing
- [x] 1.5 Table-backed type-bucket conflict (`retyped.code`, `legacy.flags`) → `"set_type"`; sql-mode scalar conflicts (`legacy_sql.span|flags|tz`) → `"set_type"`; verify failing
- [x] 1.6 New DuckDB sql-mode model `listy_sql` (`SELECT id, skills FROM kinds`, `skills: TEXT`) → its finding has `remedy_kind == "set_type"`; verify failing and that no other test's expectations change
- [x] 1.7 Legacy load: a finding dump with `remedy_kind` removed loads via `RetypeFinding.model_validate` with the right kind for both canonical texts, and `remedy`/`summary` unchanged; a full dump → load round-trip is equal; verify failing
- [x] 1.8 Legacy load with an unrecognized `remedy` text and no `remedy_kind` raises `pydantic.ValidationError`; verify failing

## 2. Implementation

- [x] 2.1 In `slayer/engine/schema_drift.py`: add `RemedyKind`, stored `remedy_kind`, `remedy`/`summary` as `@computed_field` properties with today's exact texts; verify `test_retype_finding.py` field/shape tests pass
- [x] 2.2 Replace `of(..., reingest: bool)` with `of(..., remedy_kind=...)` and update the three call sites; verify the remedy-kind tests (1.3–1.6) pass
- [x] 2.3 Add the `model_validator(mode="before")` legacy inference over the two canonical texts, refusing anything else; verify 1.7–1.8 pass
- [x] 2.4 Update `docs/concepts/schema-drift.md` §Re-type findings: add `remedy_kind` to the field list plus one sentence (`reingest` or `set_type`); verify `poetry run pytest tests/test_doc_examples.py` passes

## 3. Gates

- [x] 3.1 `poetry run pytest -m "not integration"` passes in full
- [x] 3.2 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check` pass
- [x] 3.3 `openspec validate dev-2224-a-re-type-finding-carries-its-remedy-as-a --strict` passes
