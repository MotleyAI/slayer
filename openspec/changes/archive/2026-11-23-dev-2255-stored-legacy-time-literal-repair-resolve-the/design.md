## Context

See proposal.md (Why). Current state on main (2.0 storage):
- `StorageBackend._migrated_dataset` (`slayer/storage/base.py`) runs, for a below-current dataset: the migration chain,
  `_repair_source_query_filters` (legacy literals: sqlglot sites from `storage/legacy_time_literals.py`, typed by
  `StoredDatasets.field_type`), `_inlined_source_queries` (extension fields), `_qualified_source_queries`; a current
  document holding an older nested stage runs only qualification. Memories run the same steps in `_memory_from_stored`.
- The whole-day upgrade of stored queries is the SlayerQuery v14→15 migration (`storage/date_only_migration.upgrade_query`),
  typed by `StoredDatasets.field_type` through the `typing_with` context; its sites come from `core.date_only`.
- `StoredDatasets.field_type` is a raw-dict walker: incident edges either direction and the spine, but no short-form
  routing and no quoting.
- Inlining and qualification read typed datasets from `_converting_datasets` (`get_dataset`, the documents converting
  excluded through `_INLINING`); qualification resolves each pre-2.0 reference with `resolve_path` from the population
  (`qualified_references._Stage`), infers a root-less population (`_inferred_population`), picks the datasource with
  priority (`_datasource_of`), and builds the stage universe (`_stage_universe`).
- `resolve_path` / `resolve_anchored_path` and the matchers below them (`drop_root_hop`, `find_anchor`, `resolve_hop`,
  short-form routing, `walk`) match unquoted.

Principles that apply: system §3.8 one reader of Mode B; §3.2 model truth (storage → engine, storage → core, engine →
core arrows exist; no `.c4` change expected); §3.6 Pydantic, never dataclasses; §3.5 async-first; storage §3.3 versioned
persistence (the rewrites stay part of the load-time upgrade).

**Symptom:** a routed or quoted DATE / TIMESTAMP operand keeps its legacy literal (`TimeLiteralError` at run time) or
loses its whole-day meaning. **Structural cause:** storage types operands with a second reference resolver that must agree
with the binder by hand. **Fix:** type with the binder's resolver over the typed datasets qualification already reads.

## Goals / Non-Goals

**Goals:**
- One reference resolver for stored-query literal typing, shared with qualification, quoting included.
- One typing for every literal of a stored query (legacy repair and whole-day upgrade).
- One Mode-B reader: storage stops parsing Mode-B text with sqlglot.

**Non-Goals:**
- Typing a dataset's own formulas and field filters (they read dotted paths from their own dataset, with no short-form
  routing, and are not qualified): they stay on `StoredDatasets`.
- Time-dimension `date_range` repair: unchanged.
- Live refinement of below-v8 peers: their untyped columns stay untyped.
- How `_converting_datasets` loads peers.

## Decisions

1. **Re-plan against main 2.0 instead of amending the plan written before it.** (user)
2. **The typed rewrites resolve references with the binder's resolver over typed datasets; the raw walker is not
   patched.** Weighed: adding routing and quoting to `StoredDatasets.field_type`. Rejected: it keeps a second resolver
   agreeing with the binder by hand — the bug class itself. (default, structural)
3. **The whole-day upgrade of stored queries moves with the legacy repair into one typed step.** Weighed: moving only the
   legacy repair (the issue's wording). Rejected: two typings in one query — a slashed `'2024/01/01'` would get its
   whole-day meaning on a routed column while `'2024-01-01'` would not. The SlayerQuery v14→15 and Memory v10→11
   migrations become version stamps; `typing_with` / `needs_typing` remain for a dataset's own texts. (user)
4. **The typed step runs after migration and before extension inlining and qualification, typing each compared reference
   with `resolve_path` from the population — the pre-2.0 reading qualification keeps.** A leaf naming a stored extension
   column on the resolved dataset is typed by its declared type through the inliner's own lookup
   (`storage/extension_fields.py`), exposed not duplicated; inlining then carries the rewritten literal into its cast.
   Weighed: typing the qualified stage with `resolve_anchored_path`. Rejected: an inlined extension column is a cast or
   bare expression by then, so the reader would have to type expressions. (user)
5. **The plan's earlier side-effect-free typed views are superseded: the step reads `_converting_datasets`**, the
   universe inlining and qualification already read; the hygiene test (raw reads unmutated, bytes equal across load
   orders) already holds on it. (default; supersedes an earlier user answer made against the pre-2.0 code)
6. **Quoting reaches every matcher.** `resolve_path` / `resolve_anchored_path` take optional per-hop `quoted` flags,
   threaded through `drop_root_hop`, `find_anchor`, `resolve_hop`, short-form route selection and `walk`; none passed
   behaves as today. The typed step and the qualifier pass each reference's parsed flags; a quoted leaf matches its field
   exactly; the qualifier keeps a quoted leaf double-quoted when it respells the path, so its reading after qualification
   is the one the step typed. (default: one reading; propagation from codex)
7. **A dataset's own texts stay on `StoredDatasets`.** (default)
8. **A memory's query is typed in the datasource its qualification reads** (`_datasource_of`: priority applied), replacing
   the whole-day rule "the datasource of the stored model its source names, else the store's only datasource"; the
   requirement and its spine-memory scenario are rewritten, and a two-datasource scenario added. (default: one universe;
   requirement edit from codex)
9. **Untypable → left as written; refusal stays qualification's.** (default)
10. **A population naming an earlier stage types from the stage host the static readers build** (`_sibling_host`: the
    output fields its extension joins name, untyped): a bare reference stays verbatim, a hop over the stage's extension
    join is typed. (codex)
11. **Typing, inlining and qualification run in one guarded conversion scope**: one `_INLINING` token for the document and
    one read of the typed universe, so the step cannot load the document it converts. (codex)
12. **The conversion runs each operation whenever a nested stage needs it, whatever the container's version**, so a
    current document holding an older nested stage is typed and inlined, not only qualified. (codex)
13. **A root-less stage types from the population qualification infers**, through qualification's own inference, shared
    not copied. (codex)
14. **Site reading.** `core.syntax.literal_comparisons` (Pydantic items: value, source span `[start, end)`, the other
    operand's parts and per-segment quoting) reads through the shared parser, recording each string literal's offset
    outside node identity, and yields nothing on unparseable text; it covers `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
    in either operand order, parenthesised, and each element of an all-literal `IN` / `NOT IN`. `core.date_only` sites
    carry per-segment quoting. `storage/legacy_time_literals.py` keeps normalisation, `date_range` repair and the span
    splice, with no sqlglot. (user, from the earlier plan)
15. **Legacy repair keeps `NOT IN`, `==` and `<>`** beyond main's listed operators. Rejected (codex): restricting the
    repair to main's list — a legacy literal in those layouts fails at run time just the same; the requirement text is
    widened. (rejected, codex)
16. **The extension test expects main's inlined form** (`CAST(... AS DATE) >= '2024-01-01'`). (user)

## Risks / Trade-offs

- [The binder's reading differs from the raw walker's spine rules or another whole-day scenario] → every existing
  whole-day, inlining and qualification scenario must pass unchanged; a divergence is raised, not folded.
- [Moving the whole-day upgrade out of the migration chain breaks idempotency or the interrupted-load guarantee] → the
  step stays gated by each nested document's own version and runs before the write-back; the idempotency and
  interrupted-load scenarios guard it.
- [Recording literal offsets in the shared parser changes node identity] → offsets sit outside equality and hashing; the
  full suite guards it.
- [A below-v8 peer lacks column types] → its operands stay untyped (non-goal).
