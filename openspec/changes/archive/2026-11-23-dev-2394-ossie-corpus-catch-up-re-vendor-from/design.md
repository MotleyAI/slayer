## Context

See proposal.md (Why). Facts that shape the approach:

- Vendored corpus: apache/ossie `94cf86bd` (2026-11-20). Upstream since: `3719487` (portable-construct rejections), `1b1ebb6`
  (`AGG(source.measure)`), `0f579ad` (validator sqlglot guard), `448612e` (`.ost` query scripts, scott gains `pet`/`bonus`), `83d39d6`
  (loader merge precedence on PyYAML 6.0.4), `d4dca4e` (aggregates without `OVER` refused in field expressions). On a fresh checkout only `harvested/0c27bf637ef2.yaml` fails (6 tests); the new
  `harvested/9e8e4d46686b.yaml` and the new scott already pass.
- Ossie rules (core-spec/spec.md, Metric Scoping): rule 7 allows `AGG(source.measure)` when the source is a measure relation; rule 8
  makes it a derived measure; rule 9: validation checks only the form, and a consumer generating SQL against the source MUST reject
  `AGG` on a non-measure column (including when the source has no measures) and MUST NOT replace `AGG` with an ordinary aggregate.
- No SLayer source exposes measure columns. Snowflake semantic views and Databricks metric views do, but forbid query-time joins
  (measure-relation condition 4); BigQuery graph measures, Spark 4.2 metric views, DuckDB `yardstick` and Oracle analytic views were
  researched (2026-11-22); evaluation is DEV-2399.
- `slayer/core/ossie_validity.py` matches only `exp.Anonymous` named `AGG`; sqlglot ≥ 30.19 parses `AGG` as `exp.Agg`, and under
  30.33 the native check reports no `AGG` error at all (probed). SLayer allows `sqlglot >= 30.17`.
- Document text is parsed by `yaml.safe_load` in `storage/semantic_models.py` (`parse_document_text`), `storage/base.py:299`,
  `osi/importer.py:247,283` and `osi/exporter.py:1069`; duplicate keys silently keep the last value (probed), and malformed tagged
  scalars raise `AttributeError` / `ValueError` (probed). Plain `safe_load` already gives YAML 1.1 merge precedence on PyYAML 6.0.3
  and 6.0.4 (probed), so upstream `83d39d6` needs no SLayer change beyond keeping that precedence in the new loader.
- SLayer's expression parser accepts `INTERVAL` (documented in `docs/concepts/references.md`) and lowers it to `date_add`, as it does
  `DATEADD`; it rejects the other three constructs `3719487` refuses (probed). A string `expression` is Ossie's portable `sql`.

Principles that apply: system P4 (SQL as AST), P11 (fail at save time); core P5 (typed errors), P6 (structured warnings); engine P2
(one node per spelling), P8 (amended, decision 8); storage P3.1 (a load never writes); importers P1 (preserve meaning).

## Goals / Non-Goals

**Goals:**
- The corpus tracks upstream `main` mechanically, and the native validity check agrees with upstream's validator on every supported
  sqlglot.
- Ossie's `AGG(source.measure)` is accepted, stored, round-tripped and refused at evaluation exactly as rule 9 requires.
- Every portable SQL text SLayer writes is valid `OSSIE_SQL_2026`.

**Non-Goals:**
- Evaluating `AGG(source.measure)` on measure-relation sources (DEV-2399, post-2.0).
- Vendoring or running upstream's Layer 2 `.ost` query scripts (DEV-2400, post-2.0).
- A values test for a dataset-scoped `AGG(…)` metric (dropped from the brief: rule 9 forbids evaluation on SLayer's sources).
- An upgrade path for documents holding portable `INTERVAL`: native documents first shipped on the unreleased 2.0 line.

## Decisions

1. **`AGG(source.measure)` follows Ossie rules 7–9**: valid in form; a dataset metric using it loads unevaluable and every query
   reading it fails with a typed error. Brief item 3 (reuse the `AGG(dataset.metric)` evaluation) and its values test are dropped.
   Rejected: implementing evaluation for Snowflake/Databricks now — those engines forbid query-time joins, so the no-duplication
   guarantee could hold only in join-free queries, and Databricks has no live coverage. (user)
2. **Evaluation on measure-relation sources is DEV-2399**, post-2.0, recorded in DEV-2372. (user)
3. **Duplicate YAML keys are rejected in this change**, through one document-text parser. Rejected: a separate issue (the native
   check would keep disagreeing with upstream). (user)
4. **One shared `AGG` recognition (an iterator yielding both sqlglot shapes, plus the predicate)** replaces every enumeration and
   predicate use. Rejected: raising the sqlglot floor (forces users onto 30.19+, still needs the `exp.Agg` match). (user; the
   iterator from codex finding 1)
5. **Upstream's `.ost` Layer 2 scripts are not vendored**; they belong to DEV-2400 (post-2.0 Layer 2 compliance), linked from
   DEV-2372 and DEV-1768. Rejected: vendoring them unused (drift alerts on content nothing reads). (user)
6. **`ossie_schema_drift.py --vendor <checkout>`** makes re-vendoring one command. Rejected: hand-copying files and blob shas. (user)
7. **Portable `INTERVAL` is respelled as `DATEADD`** wherever SLayer writes `OSSIE_SQL_2026`. Rejected: storing the author's
   `INTERVAL` as the datasource dialect's entry (loses portability, and SLayer's `INTERVAL` forms are not valid in every dialect, so
   the text is not verbatim either). (user)
8. **Engine principle 8 amended** (`architecture/python.engine.arc42.md`), approved wording: "`save_dataset` stores the author's
   spelling unchanged, except a deprecated spelling or one Ossie rejects where it is stored, each stored as its canonical twin;
   normalization applies to queries at execute time only." (user, approved wording)
9. **Documents load an unevaluable metric with a warning; a SLayer-authored save adding one is refused**, the treatment windows
   already get. (default; system P11 and the DEV-2243 precedent)
10. **Inside a dataset metric, `AGG(source.x)` is always the source-measure reading**, even if a relationship path or dataset is named
    `source`. (default; Ossie name resolution, consistent with `source.column`)
11. **The native check mirrors upstream's `3719487` construct rejections, its `d4dca4e` field-aggregate rejection and their
    message texts.** (default; Ossie decides)
12. **The vendored set gains the core-spec text and `compliance/expressions/*.txt`**, with parity tests. (default; brief item 1, and
    a test for `3719487`)
13. **No upgrade path for stored documents with portable `INTERVAL`** — none exist outside the unreleased 2.0 line. (default)
14. **Every reader of document text uses one parser** (`storage/base.py`, `osi/importer.py` incl. the legacy re-parse,
    `osi/exporter.py`, `storage/semantic_models.py`), rejecting duplicate keys and keeping merge precedence. (codex)
15. **Respelling scope**: only text SLayer writes (dataset-tool and `edit_semantic_model` saves, model-scoped metric saves, the
    import upgrade, export) through one canonical portable-SQL writer; unchanged document text is never rewritten, so every valid
    document still round-trips byte for byte; a standalone dataset stored with `INTERVAL` is respelled once on its next save and is
    stable afterwards. (codex)
16. **Every portable-SQL write path is inventoried and routed through the canonical writer**, with tests per path (field, dataset
    metric, model metric, imported legacy document, export). (codex)
17. **Parity tests run natively without `uv`** (every `valid.txt` line passes the expression syntax check, every `invalid.txt`
    line fails it, except
    upstream-deferred `SELECT a FROM t`, pinned as accepted), plus a rule 7–9 context matrix (field rejection, dataset acceptance,
    model acceptance, field named `source`, `FILTER`, superseded portable entry, vendor-dialect exemption); the upstream `uv` comparison
    stays as an extra check. (codex)
18. **Unevaluability propagates transitively and cycle-safely through model-scoped metrics**; the error names the selected metric,
    the dependency chain and the reason. (codex)
19. **`--vendor` deletes only paths the previous provenance listed**, stages the new snapshot and swaps it in only after copy and
    harvest succeed, writes provenance last, and refuses paths escaping the checkout or corpus roots. (codex)
20. **Constructor errors (`ValueError`, `AttributeError`, `LookupError`) from YAML tags are invalid-document reasons**, as upstream's
    loader treats them. (codex)

## Risks / Trade-offs

- [Upstream keeps moving between this plan and the re-vendor run] → `--vendor` pins whatever commit it runs on; any new failure it
  surfaces is triaged with the owner before tests are written against it.
- [The weekly job runs a fresh upstream, which may already be ahead when this lands] → acceptance is a manual dispatch on this
  branch; later drift is the job's purpose.
- [Respelling changes what `inspect` shows for an `INTERVAL` field] → meaning is identical; `docs/concepts/references.md` says so.
- [`exp.Agg` carries its argument differently from `exp.Anonymous` (`this` vs `expressions`)] → the shared iterator also yields the
  argument list, and every caller reads arguments through it.
