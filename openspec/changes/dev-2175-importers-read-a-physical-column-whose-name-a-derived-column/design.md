## Context

See proposal.md for why. State on `main` (`e6978e9a`):

- `Column.sql` is native SQL. One walker (`slayer/sql/column_expansion.py::_process_reference_site`) resolves a bare name to a sibling column that needs expansion, else to the physical column; save-time dependencies (`slayer/engine/column_dependency.py`) use the same sibling-first rule. Join keys, drift and type refinement read `physical_name` (a base column's bare `sql`), so `sql: doubled` naming another column is read logically by queries and physically by joins and drift.
- `Column.filter` and model `filters` go through the same walker. A trivial base column's own filter may name itself (`physical_read_exempt`).
- Mode B binds names only to model columns (`slayer/engine/binding.py`), never to physical columns.
- DEV-2171's `slayer/importers/physical.py` (`PhysicalIndex`, `unreadable_columns`) drops every imported construct reading a physical name another column holds, and every self-named member; 33 strict xfails wait on this change.
- The Ossie importer selects one dialect entry and renders it for the target (`slayer/osi/dialects.py::select_expression`); the exporter writes `{sql}` when it round-trips exactly, else a dialect entry.
- Stored versions (with DEV-1607 merged in): SlayerModel 26, SlayerQuery 16, Memory 12. Migrations are pure dict steps that also run inside `model_validate`; model-aware repairs run in `_migrate_and_refine_on_load` (`slayer/storage/base.py`), and migrated documents are written back.
- DEV-2318 (on `main`) renamed every name to Ossie's vocabulary (`Column` → `DatasetField`, `columns` → `fields`, `type` → `datatype`, metric `formula` → a string `expression`, `measure` → `metric`). DEV-1607 (merged into this branch, not yet on `main`) added the Ossie shapes: query entries, `from_columns` / `to_columns`, filter strings, unknown-key refusal, Ossie datatype names, `SLAYER` payload version 2. Both keep field `sql` and a string metric `expression`. The `SLAYER` payload carries field `sql` on the wire (`FieldContent.sql`, embedded `DatasetField`s).

**Principles that apply:**
- `system` P4 (AST-built SQL), P7 (fail closed), P8 (one reader of Mode B), P10 (one typing), P11 (fail at save time);
- `semantics` Axiom 1 and 2.1 (derived definitions cross provably to-one hops), §4 P1–P3 (edited by this change);
- `python.core` P5 (typed errors), P6 (structured warnings);
- `python.engine` P1 (no string rewrite of a previous stage's output), P2, P8 (models persist verbatim; a deprecated spelling stored as its canonical twin), P10 (dependencies are closures);
- `python.storage` P3 (versioned persistence; P3.1 is a target clause);
- `python.sql` P4 (one Mode-A door);
- `python.importers` P1 (importers preserve meaning).

**Structural cause.** The symptom (importers cannot read a masked or self-named physical column) is one instance of a context-dependent name rule: the meaning of a bare name in a definition depends on which other fields exist. That also makes `physical_name`, drift and join keys disagree with query reads. The fix removes the class: one context-free rule (bare = source column, dotted = field), one dependency graph, one physical-reader predicate.

## Goals / Non-Goals

**Goals:**
- A valid Ossie field or dataset-scoped metric means in SLayer what it means in Ossie.
- One name rule in every definition, with no dependence on sibling fields.
- Every stored dataset keeps its results through the conversion.

**Non-Goals:**
- Delimited (quoted) Ossie names: skipped as today; DEV-2244 item 7.
- Running a native dialect entry on a metric; running an entry for a dialect other than the datasource's; storing a field with no runnable form (DEV-2244 item 6 may relax the refusal).
- Field references in Ossie's own syntax (Ossie has none); the exporter inlines them.
- Re-anchoring dotted paths at their first segment as a dataset name (DEV-2277).
- Derived relationship keys (Ossie allows any field as a key; SLayer keeps unfiltered base keys).
- A load that never writes (storage P3.1, DEV-1844).

## Decisions

### Shape and name rule

1. **One definition key, `expression`**, Ossie's `{sql, dialects}` object; no field `sql` beside it. Supersedes the 2026-10-30 split into two keys and the 2026-11-12 "keyed on the spelling" rule. (user, ruling of 2026-11-12)
2. **A dotted name in a field expression reads a field** (`orders.status`, `customers.region`); a bare name reads the source column. Alternative: a reserved `datasets.<dataset>.<field>` prefix; rejected as a second qualified grammar with a reserved word. Ossie gives dotted names in field expressions no meaning, and `dataset.field` is its own qualified form in model-scoped metrics. (user)
3. **Which form runs**: the entry for the datasource's own dialect wins; portable `sql` is the fallback. Alternative: `sql` always wins; rejected because it ignores the author's native variant on the very dialect it was written for. Ossie's "authoritative" is relative to an `OSSIE_SQL_2026` entry. (user)
4. **`source.x`** reads the dataset's source column in every definition unless the dataset declares a field named `source`. Alternative: a relationship or dataset named `source` keeps precedence; rejected because a valid Ossie model would read differently. (user)
5. **A string `expression` is a permanent shorthand** for `{sql: …}`, stored as written. Alternative: deprecate it; rejected as churn for SLayer's most-used authoring form. (user)
6. **No native metric variants in 2.0**: a metric with an entry for the datasource's dialect is refused at save; entries for other dialects are kept and unused. Alternative: run them as opaque aggregates; rejected as new machinery for an additive gain. (user)
7. **`filter` and dataset `filters` follow the row-definition rule.** Keeping the sibling-first rule there would leave the Cube measure-filter, segment and CASE-predicate xfails failing. (default)
8. **A field with no runnable form is refused at save** (system P11). (default)
9. **Dotted paths use today's path grammar from the declaring dataset** (hop by relationship name, else target dataset); the self-qualifier reads the field. DEV-2277 re-anchors every reference later. (default)
10. **Native text keeps its lexical scoping**: CTE and subquery aliases first, then the self-qualifier, then a relationship or target-dataset hop; any other dotted name (`schema.table.col`) is opaque and kept verbatim. (codex)
11. **One dependency graph**: references extracted from portable text (`core.syntax`) and native text (the sqlglot walker), including field and dataset filters, feed one graph used at save time and at query time. (codex)
12. **Physical reader** = a field whose running form is a bare source column and which has no `filter`. Helpers, drift, ingestion, typing and join keys share this one predicate. (codex)
13. **Typing**: in portable `sql`, a source column takes the field's declared `datatype`, else the live type, else `Opaque`; a derived field's declared `datatype` is cast exactly as today. (default)
14. **Metric `datatype`** comes from DEV-1607's rename of `type`; no work here. (default)

### Old spelling and stored documents

15. **Stored and old-spelling field `sql` becomes the datasource dialect's entry**, verbatim apart from qualified references. Alternative: portable `sql` where provably equal; rejected because "provably equal" needs a per-construct judgement and a wrong one silently changes results. (user)
16. **The entry gets the datasource's concrete dialect token; a bare identifier becomes the portable shorthand; while the dialect is unknown the field keeps its deprecated `sql`** (B2), converted wherever the datasource becomes known; only converted documents are written back. Alternatives: a SLayer-only `NATIVE` token, rejected as opaque to every other Ossie consumer; making such documents unloadable, rejected because it strands stores and breaks the Python twin. (user)
17. **Two phases** (codex):
    - a pure registry step stamps the version and turns a bare or quoted identifier into the portable shorthand; it also runs inside `model_validate`. Non-trivial text stays as deprecated `sql`, B2's state;
    - a load-pipeline refinement with datasource and peer context qualifies sibling references (today's resolution), tags the concrete dialect and migrates `source.` paths. It runs after the existing join-key repairs and inside `stored_document_boundary`.
18. **Dialect-dependent checks** (runnable form, duplicate entry, native metric entry) run wherever the dialect is known: engine save, storage save with the datasource config, query time. Otherwise they are deferred to query time, and an unresolved deprecated `sql` is saved as written. Portable `sql` is checked everywhere. (codex)
19. **`source.` migration is version-gated**: only documents from before this step are rewritten, since `source.x` could then only be a path. It is rewritten to the target dataset's spelling when that resolves uniquely, otherwise the document becomes unloadable through the existing boundary, untouched on disk. (user; gate from codex)
20. **Field `sql` beside `expression` is refused** with a typed error naming both (DEV-1607's pattern). (default)

### Importers and exporter

21. **Cube and dbt write native datasource-dialect entries**, or the portable shorthand for a bare column. `{other}.x` and physical join keys read a physical reader of `other`, else a hidden helper. (default)
22. **One helper per physical identifier per dataset** (exact quoting and case), reused by every importer surface, named `x_raw`, `x_raw_2`, … independent of traversal order; re-import is idempotent. (codex)
23. **Only the masking outcomes go**: "masked by another column" and "reads its own name". The absent, ambiguous and undecidable-case verdicts stay with their DEV-2242 tests. (codex)
24. **Ossie fields are stored verbatim**; when no form can run, a readable entry is translated into a datasource-dialect entry, as today, and reported. A metric's datasource-dialect entry is dropped with a report when `sql` exists, translated otherwise. (default)
25. **Delimited names** stay skipped; the work moves to DEV-2244 item 7, with a comment there. (user)
26. **Exporter**: stored forms exported as they are with the real token; field references inlined on each layer's AST with each `filter` applied once; native text never labelled portable `sql` (a portable definition inlining a native field exports as that dialect's entry); unrepresentable joined or fanning definitions reported. (codex)

### Architecture, tests, process

27. **arc42 wording**, `architecture/semantics.arc42.md` §4 (user, approved wording):
    - P1: replace "drops out (`r.x ≡ x` from root `r`); keys, join aliases" with "drops out (`r.x ≡ x` from root `r`), except inside a row definition (P3); keys, join aliases".
    - P2 first sentence: "2. **Two expression layers**: Mode A, the datasource's own SQL (a definition's entry for that dialect, `filter`, dataset `filters`), vs Mode B, a SQL-compatible grammar (a definition's portable `sql`, query fields) — one canonical function library, extended never forked. [review]"
    - P3: "3. **Logical names, physical once**: inside a **row definition** — a field's `expression` or `filter`, a dataset's `filters` — a bare name is a source column and a path from the dataset (`d.f`) a field; inside a metric's `expression` a bare name is a field; in both, `source.x` is a source column unless a field is named `source`. Every other reference — query fields, relationship keys, unique-key sets — names a field. A source column's spelling is applied once, where a reference becomes SQL or meets the live schema, and is never the key two references are matched on. [enforced: test:tests/test_dev1902_join_key_spelling.py] [enforced: test:tests/test_dev2175_definition_names.py]"
28. **Spec attachment**: the new capability `models/field-expressions` sits in the cross-cutting `models` group; `model.c4` and `index.yaml` are unchanged. (default)
29. **Suite respelling now**: a value pass scripted on the production conversion function (Python sources and YAML fixtures, each touched test re-run), then the `sql=` keyword and dict-key respelling with `la:deterministic-refactor`, after DEV-2295 and DEV-2292 are released and the la pin is bumped. Any expected value that would change goes to the owner for consent. Alternative: keep old tests on the deprecated spelling; rejected, as for DEV-1607's decision 8. (user)
30. **Test alignments** approved by the owner:
    - `tests/test_dev2171_collision_drops.py`: delete its 8 drop cases, move `test_nested_scope_reference_does_not_collide` and `test_view_over_a_filtered_measure` to `tests/test_dev2171_cube_import.py`, delete the file;
    - `tests/test_dev2171_importers_preserve_meaning.py`: collision forms back to value checks;
    - delete `tests/test_importer_physical.py`;
    - delete `tests/test_osi_source_columns.py::test_source_column_behind_a_renamed_field_is_reported`;
    - `tests/test_dbt_converter.py::TestJoinKeyColumns::test_target_key_named_by_a_derived_column_drops_the_join` → `…_keys_the_join_on_the_physical_column`, asserting the join keyed on hidden `id_raw` carrying the primary key, the derived `id` kept and not the key, no warning;
    - `tests/test_dev2171_cube_import.py::test_quoting_is_kept_on_the_hidden_base_column` → `test_quoting_is_kept_on_the_physical_read`, asserting `status_upper`'s dialect entry keeps `UPPER("status")` and its values. (user)
31. **Stacked on DEV-1607**: DEV-1607's branch (`egor/dev-1607-ossie-shapes-on-top-of-the-vocabulary-rename`) is merged in (2026-11-19); the PR opens after DEV-1607 lands on `main`. (default; the issue says so)
32. **Rejected (codex)**: "top-level `sql` must win over a dialect entry" — the owner ruled the reverse (decision 3) and Ossie's text supports it; a test pins the precedence with deliberately different forms.
33. **Rejected (codex)**: "write-back on load violates storage P3.1" — P3.1 is a `target:` clause; migrated documents are written back today (as DEV-1607's decision 11).
34. **The `SLAYER` payload moves to version 3**, carrying a field's definition under `expression`; versions 1 and 2 still import, their field `sql` converting as a stored field `sql` does with the import's datasource dialect. DEV-2318's ruling: a renamed wire key bumps its format's version. (default; added after merging DEV-1607, user-approved edit)
35. **`models/join-keys` › Importers emit logical key names follows the helper rule**: joins key on a physical reader, else the shared hidden helper; a join is dropped only when an ON operand is a member reference that is not a physical reader. Its old text (Cube drops a join over a non-base member; dbt synthesises a key only when no dimension covers the entity) contradicted the Cube and dbt deltas. (default; correction found while rebasing on DEV-1607, user-approved edit)
36. **Datatype names follow DEV-1607**: the untyped fallback is `Opaque`, and the tripwire delta is rebased on `population`. (default; user-approved edit)

## Risks / Trade-offs

- [A stored sibling reference qualified wrongly changes results] → the refinement reuses today's resolver to decide which bare names were siblings; stored-upgrade fixtures (0.10.2, 1.8.2 and new ones) must return their previous results.
- [Datasets that never get a datasource config keep the deprecated `sql` on disk] → accepted (B2); 3.0 removes the spelling and the migration guide says so.
- [A dataset moved to another database type finds no entry for its dialect] → refused at save, or at query time when stored; the error names the dialects present. Authors use portable `sql` for multi-dialect datasets.
- [Ossie documents whose portable `sql` Mode B cannot read yet (DEV-2282 gaps)] → the importer translates a readable entry into a datasource-dialect entry and reports it.
- [The suite respelling is large] → value pass first on the tested production conversion; the keyword pass under `dr-refactor` with the full gate; changed expected values need consent.
- [Concurrent version bumps (DEV-2277, DEV-2243)] → this step follows DEV-1607's (SlayerModel 26 → 27) and is renumbered at merge; migration tests pin source versions symbolically.
- [Native text with dotted names that are neither paths nor lexical aliases] → kept verbatim as opaque, as today.

## Migration Plan

- One `SlayerModel` version step: the pure phase in the registry, the context phase in the load pipeline. Converted documents are written back as today; unresolved ones stay on disk until their datasource is known.
- A document converted by this release does not open on the previous release (unknown `expression` key on a field); forward-versioned documents are best-effort, as for every bump.
