## Context

See proposal.md › Why. Today a slot's type is set by the caller that interns it, from one of several partial derivations:

- `engine/key_metadata.py::metric_key_type` types casts, lists, bare fields, conditionals, date scalars and local aggregates only; arithmetic, other scalars, transforms, `IN` and literals get `None`. A cross-dataset aggregate source gets `None` unless it is a count.
- `engine/compile/stages.py` (the regroup producer, around the `grain_dms` / `agg_dms` loops) calls `dimension_key_metadata` and `metric_key_type` without the bundle, so path sources come out untyped.
- Regroup placeholders (`FieldKey` leaves named `__regroup__N__…`) have no type in their consumer's scope.
- `dimension_key_metadata`, `date_scalar_type` and the facade's `aggregated_type` (`facade/catalog.py`) each derive types again.
- `ValueRegistry.intern` / `_intern_hidden` (`engine/compile/projection.py`) take an optional caller-supplied type; hidden slots get none.
- `ir/source_bundle.py::dataset_from_stage_schema` reads an untyped `StageField` as FLOAT.

Planning probe: logging every untyped `StageField` over the unit suite gave about 16.8k emissions (regroup placeholders about 5.3k, transforms about 3k, scalars about 2.8k, arithmetic about 2.8k, aggregates about 2.1k, `IN`, literals). With every field leaf typed, `core/keys.py::value_type` types all of them except custom aggregations, and `aggregation_result_type` types those except a custom aggregation over a boolean. `value_type` also has no rule for value-preserving transforms (`lag(text)` is `None`).

sqlglot probe (`annotate_types`, sqlglot 30.17, placeholders as typed columns): `COUNT(CASE WHEN {value} THEN 1 END)` → BIGINT; `BOOL_AND` → BOOLEAN; `AVG(int)` → DOUBLE; `STRING_AGG` → VARCHAR; `SUM(dec) * 10` → DECIMAL; but `SUM(bool) * 1.0 / COUNT(*)` → BOOLEAN (sqlglot does not promote a boolean in arithmetic), `MY_UDF(x)` and `MEDIAN` on Postgres/DuckDB → UNKNOWN, and the SQLite annotator folds BOOLEAN and DECIMAL into INTEGER/REAL.

Principles that apply: `system.arc42.md` §3.10 One typing (target DEV-2180 — this change moves the slot-typing half toward it; the tag stays), §3.11 Fail at save time (`Aggregation.datatype` validated on save), §3.2 model truth (no new import arrows); `python.engine.arc42.md` P1 typed pipeline, P5 stages compose only through schemas, P9 the checker raises every user-facing type error. No arc42 or `.c4` edit is part of this change.

## Goals / Non-Goals

**Goals:**
- One total derivation of a key's type, read by every slot-typing site.
- An untyped slot or stage column cannot be constructed.
- Custom aggregations typed from their declaration or formula.

**Non-Goals:**
- The check-side parallel derivations in `engine/elaborate_env.py` (`_text_valued`, `_count_type` / `_leaf_count_type`, `time_dimension_type`) and the §3.10 tag flip: DEV-2180.
- Changing the cast policy (`ValueSlot.cast_type`, `_wrap_cast_for_type`).
- Typing the result of a non-template (built-in) aggregation differently.
- Distinguishing a NULL-only stage column from an INT one downstream.

## Decisions

1. **OPAQUE is not the type of an untypable value.** Options: A — OPAQUE for whatever stays untyped, with per-query `type` / saved-metric `datatype` as remedies; B — a declared result type. A was rejected ("Certainly not A"): it breaks later-stage reads that work today. (user)
2. **Custom aggregations: inference from the formula, a declared `Aggregation.datatype` that wins, input type as the fallback** (option Y over X = inference + input-type fallback without a declared type). The author fixes a UDF's or an inference miss's type once, at the definition. (user)
3. **Scope against DEV-2180.** DEV-2373 retires the slot-typing derivations (`metric_key_type`, `aggregated_type`, `date_scalar_type`, the type part of `dimension_key_metadata`, the producer typing in `stages.py`); the check-side ones and the §3.10 tag stay with DEV-2180 — one PR per issue. (default)
4. **Mixed-type branches are refused by the existing `queries/mixed-type-coercion` rule.** Ossie Layer 1 defines no result type for mixed branches (`core-spec/expression_language.md`, Conditional Functions), so SLayer's rule stands; typed stage columns now reach it. (default)
5. **A NULL-only value is INT.** Only a numeric type keeps every later-stage read working as on main (`sum` → NULL, `count` → 0); OPAQUE breaks them, TEXT breaks `sum` (`sum(TEXT)` is a type error by spec), refusing breaks projecting `null` today. INT matches DuckDB's resolution of an untyped NULL and does not reintroduce a FLOAT default. Inside branch folding a NULL literal stays absorbed. (default)
6. **`ValueSlot.type` and `StageField.datatype` become required.** The structural guarantee: an untyped emission cannot be built. (default)
7. **Casts follow the existing cast policy.** Newly typed values get exactly the casts `ValueSlot.cast_type` + `_wrap_cast_for_type` derive: none for an inferred aggregate INT, STRING, OPAQUE, ARRAY, a bare column or a preserved exact decimal; a CAST otherwise. Golden SQL is re-blessed. (default; wording sharpened per Codex #5)
8. **The formula typer lives in `slayer.sql`** (it owns sqlglot and `sql_template.py`); `core/keys.py` gains a hook parameter for it like `field_type`, and the engine supplies it. `core` has no arrow to `sql`; `engine -> sql` and `facade -> engine` exist, so no arrow is added. (default)
9. **SQLite formulas are annotated under sqlglot's dialect-neutral rules**, since SQLite's annotator folds BOOLEAN and DECIMAL; other dialects annotate under their own rules. Formulas are parsed under the datasource's dialect. (default)
10. **A declared ARRAY `datatype` has element type OPAQUE.** (default)
11. **The custom-aggregation chain is total**: declared `datatype` → inferred formula type → the input's type (always known, since the derivation is total) → over `*`, INT (the existing count rule). No save-time requirement to declare. (codex #2)
12. **Formula inference runs over the bound call.** Each placeholder binds to its argument in that `AggregateKey`: a key argument as a column typed by the derivation; a parameter default written as SQL (`SqlFragmentKey`) as its SQL with each ref a typed column; a literal as itself. Booleans in numeric positions are re-read as INT before annotation (the boolean-is-its-integer rule), fixing sqlglot's `SUM(bool) * 1.0` → BOOLEAN. The sqlglot result maps to `DataType` through the existing database-type → `DataType` mapping; UNKNOWN falls through. Results may be cached by (formula, placeholder types, dialect). (codex #3)
13. **The registry owns typing.** `ValueRegistry` holds the typing context (field typing over the scope + bundle, placeholder originals, the formula hook) and types every fresh slot from its key; a caller-supplied explicit type wins and sets `type_is_explicit`; on merge, a later explicit type overrides an inferred one, and two different explicit types keep today's behaviour. Hidden slots are typed the same way. Regroup placeholders resolve to their original key's type. (codex #4)
14. **A NULL-only stage column is not absorbed downstream.** Codex #1 noted that `coalesce(s.nul, order_date)` in a later stage is refused (INT vs DATE) although `coalesce(null, order_date)` in-stage is DATE. Rejected: main already refuses it (FLOAT vs DATE), the shape is pathological (a column that is always NULL), a null-only marker would be a second fact beside the type, and databases draw the same line — Postgres resolves an untyped NULL in a CTE column to `text`. Remedy: `CAST(null AS DATE)` in the producing stage. (user; rejected (codex))
15. **Transform types**: `lag`, `lead`, `time_shift`, `first`, `last` → input's type; `cumsum` → `sum`'s result type over the input; `change` → the type of `input - input`; `change_pct`, `percent_rank` → DOUBLE; `rank`, `dense_rank`, `ntile`, `consecutive_periods` → INT. (default)

16. **The `queries/time-dimensions` scenario "Untyped stage column fails closed" keeps its title** and now describes a NULL-only (INT) stage column, instead of being dropped: OpenSpec's MODIFIED block cannot drop or rename a published scenario. (default)

## Risks / Trade-offs

- [Golden SQL churn from new casts] → re-bless with each diff reviewed for casts only; executed tests per newly typed category on SQLite and DuckDB guard values (Codex #5).
- [Changed result types: a custom `AVG({value})` over INT becomes DOUBLE, a custom aggregation over a boolean takes its formula's type] → intended (proposal › BREAKING); release notes call it out.
- [sqlglot annotation gaps beyond those probed] → UNKNOWN falls back to the input type; `Aggregation.datatype` is the author's fix; a sqlglot upgrade can change inferred types, pinned by the formula-type tests.
- [Making `ValueSlot.type` required ripples into tests that build slots by hand] → mechanical alignment (a type argument), no logic change.
- [Mixed-type conditionals over stage columns now refused] → matches the same conditional over model columns, as `queries/mixed-type-coercion` › Inside a stage already requires.
