## Context

See proposal.md for motivation. Current state:

- **Export.** `_Exporter._render` (`slayer/osi/exporter.py`) renders a bound measure with `render_value_key` in the alias environment (`AliasFacilities`), slotting each field leaf to its Ossie column reference through `value_by_slot_id`. The alias environment resolves the five `_ALIAS_SLOTTED_KINDS` (field, field-SQL, bucket, aggregate, transform) only by slot and raises for an unslotted one. That guard is right for the query pipeline, where a later stage must read a bucket an earlier stage projected, and wrong for the exporter, which slots only leaves. The bucket branch of `render_value_key` also resolves a field source through `scope.resolve` directly, a second path around the alias facility. The portable dialect is the bare base `SqlDialect()`, whose `week_sunday` and `date_add` use `INTERVAL` arithmetic, which is not `OSSIE_SQL_2026`. `bind_dataset_expression` (`slayer/engine/binding.py`) builds its `ResolvedSourceBundle` without granularities, so custom-granularity buckets do not bind.
- **Import.** Import stores the document verbatim and evaluates metric `sql` natively; on the merged tree, with typed fields, `DATE_TRUNC('MONTH', CAST(x AS TIMESTAMP))`, its DATE cast, `DATEADD(DAY, n, x)` and the `week_sunday` form evaluate on DuckDB and SQLite, so no importer change is needed, except Ossie's `DATE_PART(MONTH, x)` (decision 9).
- **Probe (DuckDB, SQLite).** With the alias guard lifted for buckets, the neutral builders emit `DATE_TRUNC('MONTH', CAST(x AS TIMESTAMP))` (DATE results cast back), and values match after re-import on DuckDB.

Applicable principles: system §3.4 (SQL as AST); sql P2 (dialect quirks only in `dialects/`, amended here), P5 (one ValueKey renderer), P9 (fail closed), P14 (emitted types are logical types); importers P1 (importers preserve meaning). `importers/osi-export` belongs to the `importers` bucket's `specs ['importers']`; `queries/date-functions` is claimed by no node, and its grammar change lives in `core` (`slayer/core/syntax.py`). The new module lives under the existing `sql.dialects` element, reached through the existing `importers -> sql` arrow, so the model and `index.yaml` are unchanged.

## Goals / Non-Goals

**Goals:**
- One render path for a bucket in every environment; the pipeline's fail-closed alias check unchanged.
- One writer of Ossie SQL, reusable by any node that may import `sql`.

**Non-Goals:**
- `date_diff` export (DEV-2282 settles Ossie `DATEDIFF`'s meaning first).
- Folding the exporter's typing casts back into bare buckets on import.
- Buckets in field expressions (fields export their stored portable text, not through the renderer).
- Mode B accepting other Ossie spellings (`DATEDIFF`, …), which is DEV-2282.

## Decisions

Tags: `(user)` the user's choice, `(default)` an obvious default, `(codex)` a Codex finding folded in.

1. **The alias environment states the key kinds it resolves only by slot.** `AliasFacilities` carries that set, defaulting to the five slotted kinds, so the generator is unchanged; the exporter passes the field leaves only, so a bucket (and any composite row key) renders over the slotted leaves through `render_value_key`. The bucket branch resolves a field source through the alias facility when one is present. This removes the bug class (an environment slotting only leaves cannot render a composite row key), not the instance; there was no patch-the-instance option worth weighing. (default)
2. **Bucket shape: the neutral builders' `DATE_TRUNC('<UNIT>', CAST(x AS TIMESTAMP))`, cast to DATE for a day-or-coarser bucket of a DATE.** Ossie leaves `DATE_TRUNC`'s result type open and engines differ (Postgres returns `timestamptz` for a date), so the casts give the logical type everywhere (sql P14). (default)
3. **`week_sunday` exports as portable Ossie SQL**, `DATEADD(DAY, -1, DATE_TRUNC('WEEK', DATEADD(DAY, 1, x)))` with the base's typing casts. The issue assumed Ossie cannot express it; `DATEADD` is a REQUIRED Ossie function and the export spec mandates `OSSIE_SQL_2026` for metrics. (default)
4. **The Ossie writer is a dialect, `slayer/sql/dialects/ossie.py`**: a `SqlDialect` subclass overriding date addition to emit `DATEADD`, used by the exporter for portable metrics in place of the bare base dialect. A consequence on the same path: `date_add` in a measure exports as `DATEADD`. Placement weighed against `slayer/osi/` (no harness edit, but nodes outside the importers bucket, such as storage for DEV-2243 or strict binding for DEV-2217, could not reuse it). sql P2 is amended to admit it. (user; codex finding 1 raised the placement) The approved P2 wording, applied verbatim (user, approved wording): "**Dialect quirks live only in `dialects/`** — one file per Tier-1 dialect, data-shaped Tier-2 table, and one for the Ossie document dialect; everything not explicitly overridden goes through sqlglot transpilation. No `if dialect == …` outside `dialects/`."
5. **The exporter binds measures with the datasource's granularities**, through a new optional `granularities` keyword on `bind_dataset_expression` that fills `ResolvedSourceBundle.granularities`; the converter's own call site (`converter.py` `_add_metric`) is left as it is. (default; mechanism from codex finding 2)
6. **A datasource-dialect entry is written only when SLayer reads it back and it calls none of SLayer's registered functions; otherwise the measure is SLayer-only with a report entry.** Load reads a dialect-only metric's native text as Mode B, so a rendering Mode B refuses (DuckDB's `//` and `n * INTERVAL`, SQLite's `STRFTIME`) would not round-trip; the check binds the rendered text as load does. Every built-in dialect's custom bucket fails it, so a custom-granularity measure is SLayer-only. Weighed against reading native entries natively on load (a new opaque native-SQL metric form, against decision 8) and a Mode-B-readable dialect variant (month bases only). (user)
7. **One registry of the functions SLayer registers on a SQLite connection** (every `slayer_*` function and the math overrides such as `floor`). Connection setup registers from it, the exporter's check reads it, and a test checks that every function a connection receives is in it; a dialect without such functions has an empty registry. (default; completeness from codex finding 3)
8. **No importer change.** Native load reads the exporter's SQL as written, so re-imported formulas keep the typing casts; the extension path restores the original formula. (default, after the DEV-2243 merge)
9. **Mode B's `date_part` accepts a bare part word**, mirroring the `EXTRACT` rule, so native load reads the exporter's `DATE_PART(MONTH, x)` and third-party Ossie documents alike; a bare word outside the part set is still refused. Weighed against exporting extraction as `EXTRACT` (export-only, leaves Ossie documents unreadable) and a strict xfail against DEV-2282. (user)
10. **`date_diff` deferred to DEV-2282** (comment posted 2026-11-22), since its export needs DEV-2282's `DATEDIFF` meaning; DEV-2282 is listed in the DEV-2372 umbrella. (user)

## Risks / Trade-offs

- [Re-imported formulas read noisier than the originals without extensions] → accepted (decision 8); with extensions the original formula is restored.
- [The Ossie dialect inherits base builders that might emit other non-Ossie forms in future] → the "no `INTERVAL`" clause of the export requirement and the per-unit pins catch regressions for the forms this change writes; `date_diff` is tracked on DEV-2282.
- [A custom bucket's dialect entry is runnable only on that engine] → by design (decision 6); other Ossie consumers ignore a foreign dialect entry.
- [Widening `AliasFacilities` touches the hot query path] → the default keeps the five kinds; a regression test pins that an unslotted bucket still raises under the default.
