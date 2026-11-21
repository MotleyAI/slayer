## Context

See proposal.md for motivation. Current state that shapes the approach:

- Storage holds one `SlayerDataset` document per `(data_source, name)` (`models/<ds>/<name>.yaml`, SQLite `models` table); there is no container entity. `SlayerDataset.version` is an integer migration counter (`storage/migrations.py`).
- `slayer import-osi` converts through `slayer/osi/converter.py` after live introspection; model-scoped metrics are anchored on a heuristically chosen dataset. The exporter (`slayer/osi/exporter.py`) and the versioned `SLAYER` payload (`slayer/osi/slayer_extension.py`) already classify SLayer content into Ossie core vs extension.
- DEV-2277 (merged into this branch, not yet on `main`) makes a bare query name a metric (alias) or an `UnknownMetricError`, and fixes the order alias → model-scoped metric → legacy conversion → error (its design decision 14).
- `ruamel-yaml` is a dev dependency; PyYAML is the runtime YAML library.
- Native datasets already expose only declared fields; `source.x` is already read in dataset metrics; `queries/identifier-case` already matches unquoted names case-insensitively and refuses case-only collisions at save.
- Declared `many_to_one` already proves a hop to-one (`models/join-cardinality`).
- `agg` is reserved (`core/enums.py` `RESERVED_NAMES`); nothing evaluates it.

Element principles that apply: system 4, 5, 6, 8, 10, 11; storage 1, 2, 3, 3.1, 3.2; importers 1; semantics Principles 1, 3, 4 and Axioms 2.4, 4; core 5, 6.

## Goals / Non-Goals

**Goals:**
- Every valid Ossie `0.3.0.dev0` document stores, loads and round-trips byte-identically, with Ossie's meaning wherever SLayer can evaluate it.
- One code path decides ownership of a dataset; one parser (`core.syntax`) reads `AGG` and model-scoped metric expressions.
- No regression for standalone datasets, custom storage backends, or anything the 1.x converter imported.

**Non-Goals:**
- Evaluating `OVER` (DEV-2282), composite unique-key proofs and the rest of the dataset surface (DEV-2244), binding a metric from its own home (DEV-2388), Ossie error codes and the strict binding (DEV-2217).
- Registering SLayer in apache/ossie-converters (moved to DEV-2220).
- A document-name qualifier in references.
- Probing and persisting types at save.
- Migrating 1.x-converted datasets on load (re-import is the upgrade path; a load never writes).

## Decisions

Each entry keeps its source tag: `(user)` the owner decided, `(default)` taken because it had an obvious default, `(codex)` a Codex plan-review finding folded in.

1. **One namespace per datasource; colliding saves refused.** A SLayer datasource is the equivalent of an Ossie semantic model. Rejected: a document-name qualifier — it adds a grammar level and collides with "the first segment is a dataset". (user)
2. **Storage converges on Ossie documents after 2.0.** Where a SLayer refusal blocks an Ossie-valid shape, it is lifted for all datasets, not only document ones. (user)
3. **The dataset tools edit through to the owning document.** Rejected: document datasets read-only through the dataset tools — two editing vocabularies, and SLayer-only settings could only be hand-written. (user)
4. **Unknown datatypes are probed lazily at query time,** cached per engine and storage fingerprint, never persisted on load; `validate-datasets` reports untyped fields and `--force-clean` writes them in (`Array` as `Opaque` plus a `SLAYER` entry). Rejected: probing and persisting at save — two type paths, stale types, unrequested writes. (user)
5. **Textual fidelity via ruamel.yaml round-trip mode,** patching only changed nodes; SQLite stores text verbatim; `ruamel-yaml` becomes a runtime dependency. Rejected: semantic-only fidelity — every edit would wipe comments. (user)
6. **`AGG(…)` is accepted in stored dataset metrics** with the same meaning as in queries and model-scoped metrics. Rejected: refusing it there — a position-dependent refusal of a well-typed term breaks closure (semantics Axioms 9, 13). (user)
7. **The query oracle is upstream `compliance/wide_table`, every case,** run on in-process DuckDB; cases whose metric binds through a host with two routes to its home strict-xfail against DEV-2388, `e*` cases against DEV-2217. Rejected: a self-written reference lowering — its misreadings can mirror ours. (user)
8. **The corpus is every file-based upstream semantic-model document plus the accepted inline validator documents, harvested.** This extends the issue's m0–m3 to m0–m8 and adds the compliance models. PRs run it against the pinned snapshot; a dedicated job, on the weekly schedule and manual dispatch only, refreshes it from upstream `main` (with the harvest) and runs the same suite. Rejected: file-based documents only. (user)
9. **System Principle 11 is unchanged.** `import-osi` stores the user's file verbatim after an Ossie-validity check, like a hand-written file; save-time checks judge only what a save adds or changes (plus today's broken-dependents analysis); SLayer authoring stays strict. Rejected: a Principle 11.1 exempting Ossie-valid definitions at save. (user)
10. **The upstream grader registration moves to DEV-2220;** this change ships `slayer ossie-roundtrip` and the vendored-grader test. Rejected: drafting the upstream PR now — upstream's runner interface may change before release. (user)
11. **`export-osi` writes one merged document per datasource,** the stored text verbatim for a single-document datasource, document boundaries in a document-level `SLAYER` entry. Rejected: one file per document — relationships from standalone datasets into documents would become cross-document references. (user)
12. **semantics Principle 3** (user, approved wording) — the principle's clause becomes: "inside a **row definition** — a field's `expression` or `filter`, a dataset's `filters` — a bare name is a source column and a path from the dataset (`d.f`) a field; inside a dataset metric's `expression` a bare name is a field; in both, `source.x` is a source column unless a field is named `source`. A model-scoped metric's `expression` reads like a query: `d.f` is a field anchored at dataset `d`, and a bare name a metric." The rest of the principle is unchanged; `[enforced: test:tests/test_dev2243_model_scoped_metrics.py]` is appended.
13. **Relationship shapes Ossie allows are lifted for all datasets:** self-relationships load inert (wide_table §4); an edge may be named like a dataset, with only an actually ambiguous hop refused at the reference; keys may be computed fields reading their own source; parallel pairs are kept. Exact-inverse twins are already legal when their names differ, which Ossie's unique relationship names guarantee. (default; Ossie decides, plus decision 2)
14. **`import-osi` stores natively;** earlier shapes are upgraded per Ossie's migration guidance; a re-import replaces 1.x-converted datasets, moving anchored metrics into the document scope and carrying SLayer-only additions into `SLAYER` entries; the semantic converter is retired, its introspection kept for the legacy-shape upgrade. (default; issue and DEV-2214 carry-over)
15. **A model-scoped metric is expanded in place,** one component per aggregate, its key the bare name. (default; Ossie wide_table §5.2 and DEV-2214's inline-equality test)
16. **A bare model-scoped name resolves inside other model-scoped metrics,** circularity refused; such a metric is SLayer-only (decision 26). (default; DEV-2214 carry-over)
17. **SLayer's version counter lives in a document-level `SLAYER` entry;** a document without one is current. (default; issue)
18. **Custom backends get hook defaults that raise a typed unsupported error,** and list no documents. (default; no regression for `register_storage` backends)
19. **Moving standalone datasets into a document** (`create_semantic_model(datasets=…)`, `edit_semantic_model(add_datasets=…)`) is the native authoring path for model-scoped metrics. (default; decision 1 plus DEV-2214's native-authoring requirement)
20. **Ossie validity is checked natively** with strict Pydantic models (Ossie shape, `extra="forbid"`) plus semantic checks mirroring upstream `validate.py`; no runtime `jsonschema`, no vendored `validate.py` in production. The corpus test runs upstream `validate.py` as the oracle for agreement. (default)
21. **Placement:** the document model and the `SLAYER` payload in `slayer/core` (`slayer/osi/slayer_extension.py` moves there with `dr-refactor`, since storage may import core but not importers); decomposition, ruamel patching, owner routing and hooks in `slayer/storage`; `AGG`, bare-name resolution, expansion and type probing in `slayer/engine`. No `model.c4` / `index.yaml` change intended; if `la-arch-check` requires one, stop and present the exact edit for approval. New capabilities under the existing cross-cutting `models` and `queries` groups. (default)
22. **Names follow `queries/identifier-case` in every new namespace** (documents, model-scoped metrics, cross-document collisions, relationship keys), keeping declared spellings; mixed-case tests added. (codex)
23. **Key mapping:** `primary_key` sets per-field primary-key flags (composite members included); a one-field `unique_keys` entry sets `unique`; composite unique keys stay in the document text, unused for proofs (DEV-2244); relationship `to_columns` are checked against keys exactly as upstream `validate.py` checks them (a superset of a key is accepted). Codex's stricter whole-key match is rejected (codex, partly rejected: upstream accepts a superset).
24. **Per-document write serialisation:** YAML takes a per-document flock across read-patch-write; SQLite reads, patches and writes in one `BEGIN IMMEDIATE` transaction. (codex)
25. **One owner lookup routes every dataset mutation** (save, delete, sampling, `--force-clean`, `--persist-cardinality`, ingest upserts, moves, re-import) to standalone storage or to the owning document. (codex)
26. **Anything SLayer authors that is not valid Ossie core in its position** — a model-scoped metric referring to another, `AGG` in a dataset metric, any SLayer-only setting — is stored in a `SLAYER` entry, never in core; a document whose core is Ossie-invalid is refused by the validity check, so portable content never acquires a SLayer-only meaning. (codex)
27. **Deletes reuse the dataset cascade** (embeddings, memory entities, inbound relationships); model-scoped metrics reading a deleted dataset get today's broken-dependent warning; deleting a document cascades per member plus document and metric embeddings. (codex)
28. **Saves judge what they add or change, plus today's broken-dependents analysis** over what depends on the change; rename and delete behaviour is unchanged. (codex)
29. **Every document content or membership change changes `graph_fingerprint`** (YAML's tree fingerprint already covers `semantic_models/`; SQLite's covers the new table; the hook contract states it for custom backends). (codex)
30. **Collision quarantine is dependency-aware:** a colliding dataset's dependents in the same document are unloadable too, each naming the cause. (codex)
31. **Access filtering prunes model-scoped metrics transitively** over every aggregate component and metric dependency; a pruned metric reads as unknown with no hidden-name suggestion; filtered writes merge onto the full document. (codex)
32. **Re-import keeps dataset names**, so saved queries, stages and memories referencing them keep resolving; deletes and renames follow decisions 27 and 28. (codex)
33. **Corpus and edge-case tests** cover case-only collisions, composite and reordered keys, concurrent same-document writes, owner routing, fingerprint invalidation, transitive access pruning, memory/stage cleanup, member deletion and a dataset named `source`. (codex)
34. **Every construct the 1.x converter mapped keeps its meaning natively** (FILTER aggregates, statistical and approximate aggregates, array functions, aggregate operands, list counts, date literals, `source.column`), through the one Mode B reader; expressions with no runnable form are translated in memory at load, never written back. (default; no regression)
35. **A model-scoped metric Ossie rejects evaluates whenever SLayer evaluates the same expression in a query** (an aggregate reading several datasets, a FILTER on another dataset): SLayer is a superset of Ossie, so only what SLayer cannot evaluate loads unevaluable. Rejected: loading such metrics unevaluable; moving them into dataset scope. (user)

## Risks / Trade-offs

- [DEV-2277 is still in flight on its own branch] → this branch merges DEV-2277's tip; if DEV-2277 changes before it lands, merge `origin/main` (never rebase) and re-check the bare-name order and the specs DEV-2277 modifies (`mcp/tool-surface`).
- [ruamel round-trip patching from a Pydantic-level diff is intricate] → golden-text tests per edit kind (field, metric, relationship, extension entry, document field) assert the saved text differs only in the edited nodes.
- [Retiring the converter can drop a mapping it performed] → decision 34's requirement is tested construct by construct against the 1.x converter's expected values before the converter is deleted.
- [Lazy type probing adds a round trip to the first query on an untyped dataset] → cached per engine and fingerprint; declaring `datatype` or running `--force-clean` removes it.
- [Upstream corpus churn turns the weekly job red] → intended: it is the refresh signal; PR CI runs only the pinned snapshot.
- [The harvest depends on upstream test internals] → it runs only in the weekly job and the drift script, never in PR CI, and fails loudly.
- [Custom backends see new optional hooks] → defaults keep them working for datasets (decision 18).

## Migration Plan

- `docs/upgrading/2.0.md` gains a section: `import-osi` stores documents natively and no longer needs a reachable datasource for current-shape documents; re-importing a 1.x-imported document replaces its converted datasets and moves anchored metrics into the document scope; `export-osi` writes one merged document per datasource; relationship shapes previously refused now load.
- No stored migration: 1.x-converted standalone datasets keep loading unchanged until re-imported.
- Rollback: documents live in their own directory/table; removing them leaves standalone datasets intact.
