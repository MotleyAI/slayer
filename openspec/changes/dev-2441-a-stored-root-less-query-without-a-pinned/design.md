## Context

See proposal.md › Why. Three load-time readers decide which datasource an unpinned stored query reads, each by resolving its population:

1. `_datasource_of` (`slayer/storage/qualified_references.py`): the qualified-reference upgrade, and through `stage_readings` the literal typing. It falls back to the named datasets only for a spine population.
2. `_memory_data_sources` (`slayer/storage/base.py`): the granularities a pre-v9 memory's custom-bucket rewrite reads (the union of the datasources holding its population).
3. The memory check in `_rename_granularities` (`slayer/storage/base.py`): whether a memory belongs to the datasource whose library-colliding granularity is renamed (its population in that datasource and nowhere else, or pinned by an `entities` entry).

For a root-less memory all three find nothing; for a spine memory, 2 and 3 find nothing. Readers 2 and 3 work on the raw stored dict before migration (the colon rewrite needs the datasource's granularities first); reader 1 works on the validated query.

Principles: system §3.8 (one reader of Mode B: names are read only through `core.syntax`); storage §3.3 (migrations run on load) and §3.1 (a load never writes; memories convert on each load). No model or arc42 change; every edit stays inside `slayer.storage`, so the import law is unchanged.

## Goals / Non-Goals

**Goals:**
- One fallback, shared by the three readers, for a query that is pinned to no datasource and either names no population or names a spine spelling no stored dataset spells exactly.
- One reader of the names a stored query references, so the raw readers and the qualification reader cannot diverge.

**Non-Goals:**
- Run-time datasource scoping of root-less queries (`queries/population` keeps refusing an ambiguous datasource; 1.x did too).
- Extension-field inlining (`query_scopes`): a query that needs inlining names its datasets, and its multi-scope reader is deliberately lenient.
- Readers 2 and 3 for a population that names a stored dataset: unchanged (union of holders; `entities` pin).
- The single-`=` old-grammar gap: DEV-2444.

## Decisions

1. **Structural fix, not an instance patch.** The symptom is one instance of "a population that names no stored dataset finds no datasource", repeated in three readers. One fallback function decides the datasource for that case; each reader calls it. (default)
2. **The fallback rule.** Candidates are the datasources holding a dataset the query's references or extensions name (dotted heads, extended datasets, join targets; spine spellings excluded), else every datasource. The datasource priority picks, then the datasource name. This is the rule the spine branch and populated queries already use. Rejected: refusing unless the priority decides, which would add a second tie-break rule and leave some 1.x stores unloadable. The issue's "else the store's only datasource" is the single-datasource case of this rule. (user)
3. **Readers 2 and 3 in scope.** Both were reproduced failing for root-less memories after fixing reader 1 alone (`created_at:mid_month` left as `mid_month(…)` → `GranularityCallError`; `left` not renamed). Rejected: a separate issue. (user)
4. **Populated logic of readers 2 and 3 unchanged.** Unifying it would change how populated memories load in multi-datasource stores. (default)
5. **The spine population is in the same fallback.** Reproduced failing in readers 2 and 3 on HEAD; reader 1 already had it. (default)
6. **Exact trigger.** The fallback applies only when the population is absent, or spells the time spine and no stored dataset has that exact spelling (the current holders-first order). An explicit population naming a missing or misspelled dataset gets no fallback in any reader, so no reader reinterprets it against an unrelated datasource before qualification refuses it. (codex)
7. **One name reader for raw and validated queries.** The raw readers get the names from every location `query_reference_parts` and `extension_dataset_names` cover: dimensions, time dimensions (text and structured forms), metrics, filters, order keys and expressions, `main_time_dimension`, extended datasets and join targets. Each text is read at its stored version through `core.syntax` after legacy spellings are canonicalised; unparsable text contributes no name. No regex scan. (codex)
8. **Run-time ambiguity unchanged.** A loaded root-less query whose anchors two datasources hold is still refused unpinned, as `queries/population` specifies and as 1.x behaved. (default)
9. **Extension-field inlining keeps its reader.** (default)
10. **Scenario coverage of the shared datasource.** The extension branch, and literal typing following the priority-selected datasource, each get a scenario. (codex)
11. **The single-`=` grammar gap is out of scope.** Filed as DEV-2444 under DEV-2372 (blocks DEV-2220); DEV-2372 and the 2.0 Path page were updated. (user)

## Risks / Trade-offs

- [Reader 3 runs inside the granularity settle, before memories load] → it reads the priority and the raw dataset identities only, never a loaded dataset or memory, so no settle recursion.
- [Raw texts at old versions hold colon spans or Python spellings] → canonicalise legacy spellings first; a text that still does not parse contributes no name, and the fallback then widens to every datasource rather than failing.
- [With no priority, name order decides] → same as for populated and spine queries today; inference must still find exactly one dataset reaching what the query reads, else the document fails to load.
