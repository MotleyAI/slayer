## Context

See proposal.md › Why. Three load-time readers decide which datasource an unpinned stored query reads, each by resolving its population:

1. `_datasource_of` (`slayer/storage/qualified_references.py`): the qualified-reference upgrade, and through `stage_readings` the literal typing. It falls back to the named datasets only for a spine population.
2. `_memory_data_sources` (`slayer/storage/base.py`): the granularities a pre-v9 memory's custom-bucket rewrite reads (the union of the datasources holding its population).
3. The memory check in `_rename_granularities` (`slayer/storage/base.py`): whether a memory belongs to the datasource whose library-colliding granularity is renamed (its population in that datasource and nowhere else, or pinned by an `entities` entry).

For a root-less memory all three find nothing; for a spine memory, 2 and 3 find nothing. Readers 2 and 3 work on the raw stored dict before migration (the colon rewrite needs the datasource's granularities first); reader 1 works on the validated query.

Only memories reach the unpinned path: dataset `source_queries` (saved queries, query-backed stages) convert in their dataset's datasource, and readers 2 and 3 read memories only. No 1.x release could save a root-less or `time_spine` memory query through its service (`extract_entities_from_query` refuses both); such memories come from the raw storage API, hand edits, and pre-release 2.0 builds, which saved them with `<datasource>.<dataset>…` entities.

Principles: system §3.8 (one reader of Mode B: names are read only through `core.syntax`); storage §3.3 (migrations run on load) and §3.1 (a load never writes; memories convert on each load). No model or arc42 change; every edit stays inside `slayer.storage`, so the import law is unchanged.

## Goals / Non-Goals

**Goals:**
- One reader of a triggering memory's datasource, from raw data only (its dataset-level entity pin, else the store's only datasource), shared by the three readers.
- An explicit refusal, never a guess, when neither decides.

**Non-Goals:**
- Run-time datasource scoping of root-less queries (`queries/population` keeps refusing an ambiguous datasource; 1.x did too).
- Extension-field inlining (`query_scopes`): a query that needs inlining names its datasets, and its multi-scope reader is deliberately lenient.
- Memories whose population names a stored dataset: their three readers are unchanged, entities included (decision 4).
- The single-`=` old-grammar gap: DEV-2444.

## Decisions

1. **Structural fix, not an instance patch.** The symptom is one instance of "a population that names no stored dataset finds no datasource", repeated in three readers. One reader decides the datasource for that case; each of the three uses it. (default)
2. **The datasource.** A triggering memory's datasource is the one datasource its dataset-level entities (`<datasource>.<dataset>[.<leaf>]`, exact ids of stored datasets) name, else the only datasource holding a stored dataset. A memory saved through the service records where it resolved as such entities; a bare `<datasource>` entity survives the deletion of its datasets, so it does not pin; entities naming several datasources do not pin. The issue's "else the store's only datasource" is kept. (user)
3. **Readers 2 and 3 in scope.** Both were reproduced failing for root-less memories after fixing reader 1 alone (`created_at:mid_month` left as `mid_month(…)` → `GranularityCallError`; `left` not renamed). Rejected: a separate issue. (user)
4. **Populated memories unchanged.** Their readers keep the population's holders, and reader 3 its `entities` pin; taking a populated memory's datasource from its entities in readers 1 and 2 would change how populated memories load in multi-datasource stores. (user)
5. **The spine population is in the same fallback.** Reproduced failing in readers 2 and 3 on HEAD; reader 1 already had it. (default)
6. **Exact trigger.** The fallback applies only when the population is absent, or spells the time spine and no stored dataset has that exact spelling (the current holders-first order). An explicit population naming a missing or misspelled dataset gets no fallback in any reader, so no reader reinterprets it against an unrelated datasource before qualification refuses it. (codex)
7. **Neither decides → refuse.** With no entity pin and several datasources holding datasets, a memory whose qualified-reference upgrade is pending fails to load, its warning naming it and saying its datasource cannot be determined (a population or entities fix it); readers 2 and 3 do nothing for it. A memory whose upgrade is not pending needs no datasource and loads as before. Rejected: the first datasource where the memory resolves, which needs a trial qualification per datasource and leaves readers 2 and 3, which cannot load datasets, disagreeing with it; ranking datasources by the datasets the query names. Both serve only memories no released version could save. (user)
8. **Run-time ambiguity unchanged.** A loaded root-less query whose anchors two datasources hold is still refused unpinned, as `queries/population` specifies and as 1.x behaved. (default)
9. **Extension-field inlining keeps its reader.** (default)
10. **Scenario coverage.** The entity pin over the priority, the pinned datasource as the only one tried, the refusal of an entity-less memory on a multi-datasource store, literal typing following the pinned datasource, and the pinned custom-granularity repairs each get a scenario. (codex)
11. **The single-`=` grammar gap is out of scope.** Filed as DEV-2444 under DEV-2372 (blocks DEV-2220); DEV-2372 and the 2.0 Path page were updated. (user)
12. **Only the granularity rename persists.** A memory's conversion runs on each load and is never written back; the library-granularity rename (storage §3.2) persists into memories as into every stored document. (codex)

## Risks / Trade-offs

- [Reader 3 runs inside the granularity settle, before memories load] → it reads the raw dataset identities and the memory's raw entities only, never a loaded dataset or memory, so no settle recursion.
- [Stale or hand-edited entities] → only exact dataset-level ids of stored datasets pin; a pinned datasource where the memory no longer resolves fails the load rather than reading another.
- [Entity-less root-less memories on a multi-datasource store stop loading] → only hand-made ones; the warning names the memory and the fix.
