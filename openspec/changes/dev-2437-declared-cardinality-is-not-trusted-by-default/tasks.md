## 1. Tests first (pr-tests)

- [x] 1.1 `tests/test_dev2437_trusted_cardinality.py`: every `queries/semantics` scenario of this change on SQLite and DuckDB over the duplicated Appendix B seed and the oracle retail fixture (C1 duplicated, PK dropped from the DDL); fails on main, verified by running it
- [x] 1.2 Same file: the `models/join-cardinality` Trusted hops scenarios (flag, key-proven, reverse of one-to-many, vacuous, enforced key, stage grain, list position alone)
- [x] 1.3 B/A boundary cases asserting values and join shape: same-branch filter + dimension, home/target predicates under `OR`/`NOT`, two target branches, model `filters` on the target, RLS on the target
- [x] 1.4 Every aggregation family on dirty data (`count(*)`, `avg`, `count_distinct`, `first`/`last`, `collect`, windowed, ranked, aggregate filter, weighted with kwargs) equals the de-duplicated seed; aggregate inputs across the hop under default and `"error"`
- [x] 1.5 `tests/test_law_lowering_soundness.py` instances: B equals the association result on dirty data; trusted equals untrusted on clean data
- [x] 1.6 Ingestion tests: DuckDB PK enforced, Snowflake not (emission/dialect level), partial unique index not, re-ingest fills; dialect capability table test with one row per dialect
- [x] 1.7 Drift tests: vanished enforced key reported and cleared; author `trusted` untouched; non-enforcing dialect clears all enforced keys
- [x] 1.8 Save-validation tests: undeclared enforced key refused, query-backed refused, repointing drops
- [x] 1.9 Document round-trip tests: imported retail never trusted (350), `trusted` round-trips (650), unset cardinality round-trips, enforced keys round-trip
- [x] 1.10 Migration tests from serialized previous-version payloads: `SlayerModel` v28, `SlayerQuery` v18 (query-extension relationship), `Memory` v13 with an embedded query, a query-backed model's backing query; all relationships trusted and EAST 650 preserved
- [ ] 1.11 Existing tests pinning inline SQL or values for an unflagged declared hop: list them and get the owner's approval to align each before changing it

## 2. Model surface

- [ ] 2.1 `DatasetRelationship.trusted: bool = False`, `SlayerDataset.enforced_keys: list[list[str]]`; verify model round-trip tests pass
- [ ] 2.2 Dialect capability `enforces_unique_keys` on every dialect; verify the capability table test
- [ ] 2.3 Save validation of `enforced_keys` and drop-on-repoint; verify 1.8

## 3. Trusted predicate and lowering

- [ ] 3.1 Trusted-hop predicate beside `provably_to_one` (flag, enforced key, constructed keys incl. stage grain, spine, row number, list child's parent hop; inherited edges carry origin trust); verify 1.2
- [ ] 3.2 Switch the emission points to it — `discovery.py` host-inline routing, `stages.py` producer dimension split / `_conjunct_disposition` + `fanning_paths` / inline host-locus, `generator.py` `_filtered_dataset`, `relationship_safety.py` routing shortcuts — leaving every typing consumer on `provably_to_one`; verify typing tests unchanged
- [ ] 3.3 Distinct-projection lowering (B) through the dataset relation door; verify 1.1, 1.3
- [ ] 3.4 Fallback to the association producer (A) without warnings for ineligible cases and aggregate inputs across the hop; `"error"` refusal for inputs; verify 1.3, 1.4
- [ ] 3.5 Verify 1.5 and the full local test scope; re-bless goldens only where SQL for an unflagged declared hop changes, with owner approval

## 4. Ingestion, drift, documents, migration

- [ ] 4.1 Ingestion and re-ingestion record enforced keys; verify 1.6
- [ ] 4.2 Drift rule for vanished enforced keys; verify 1.7
- [ ] 4.3 SLAYER entry carries `trusted`, `enforced_keys`, unset cardinality; writer `default` predicate fixed; verify 1.9
- [ ] 4.4 Migrations `SlayerModel` 28→29, `SlayerQuery` 18→19, `Memory` 13→14 (stored-only); verify 1.10

## 5. Normative docs and user docs

- [ ] 5.1 Apply the approved Axiom 4 and Law 6 wording to `architecture/semantics.arc42.md` verbatim (design.md decision 24); verify `poetry run la-arch-check`
- [ ] 5.2 Update `docs/concepts/datasets.md`, `semantic-models.md`, `ingestion.md`, `schema-drift.md`, `docs/upgrading/2.0.md`, `docs/osi/osi_export.md`; verify no doc still says declarations are trusted at query time
- [ ] 5.3 Enforcement bundle green: `la-arch-check`, likec4 validate, `la-typecheck`, ruff
