# Design — dev-1891 fit over-limit internal identifiers at emission

## Context

DEV-1756 already owns the fitting machinery: pure `fit_identifier`
(`slayer/sql/_identifier_fit.py`), the write-side `substitute_quoted`, and the
per-dialect seam `SqlDialect.rewrite_emitted_sql` called at the end of both
`generate_planned_stages` branches (`slayer/sql/generator.py:6892`, `:6951`) — the
single seam every production render path crosses. It is fed only the plan-derived
`projection_aliases`; internal CTE column aliases (canonical aggregate names embedded
in stage schemas and producer CTEs) never reach it. CTE names, join aliases, and table
aliases are already fitted at mint. BigQuery/T-SQL compose dot-mangling after the base
length pass (`DottedAliasManglingMixin`), sizing budgets via `expand=encode_alias`.
Constraints: sql arc42 principles 1 (no re-parse of emitted SQL), 3 (one naming
authority), 9 (fail closed); dev-1871 pins canonical alias spellings
(`tests/test_dev1871_alias_stability.py`), so canonical names must stay canonical
everywhere internal.

## Goals / Non-Goals

**Goals:**

- Fit every SLayer-minted identifier at the one emission seam; keep canonical names
  internal-only concerns.
- Exempt user-authored identifiers explicitly (enumerated surfaces), never
  positionally or heuristically.
- Fail closed on anything unaccounted.

**Non-Goals:**

- CTE names / join aliases / table aliases (already fitted at mint).
- The BigQuery/T-SQL dot-mangle contract for user-authored dotted identifiers
  (pre-existing DEV-1571 behaviour, documented false positive in
  `slayer/sql/dialects/bigquery.py`): the byte-identical guarantee here covers the
  *length* pass only.
- Changing how canonical aliases are constructed or interned.

## Decisions

1. **Scan-at-emission over mint-time fitting or a mint-time registry.** The base
   `rewrite_emitted_sql` gains a second mapping source: over-limit quoted identifiers
   found in the assembled SQL (literals/comments masked), fitted via the dialect's
   `fit_alias` (so BQ/T-SQL budgets use `expand=encode_alias`), unioned with
   `alias_rewrite_map(aliases)`, substituted once. *Why:* internal aliases are minted
   across engine stage schemas, producer CTE schemas, and generator paths — a registry
   threads state through all of them and still fails open on ad-hoc names; fitting at
   mint breaks the "canonical inside, fitted at emission" doctrine and the dev-1871
   alias-stability pins. Purity of `fit_identifier` makes definition/reference
   consistency automatic.
2. **Explicit exemption set, not positional guessing.** The generator extracts
   over-limit identifier-shaped tokens (bare runs and quoted spans) from every
   user-authored raw-SQL surface in the bundle: model `sql`, `sql_table`, `filters[]`,
   and per-column `name`, `sql`, `filter`, over `bundle.source_model`,
   `bundle.referenced_models`, `bundle.stage_source_models.values()` (concrete user
   models for named stages), any stage `render_source_model`, plus
   `bundle.inline_extensions[*].columns` directly (idempotent — overlays are already
   folded into resolved models by `apply_extension_overlay`). Synthetic stage-schema
   models are built later inside the generator and never enter the extraction.
   `backing_query_sql` is **not** extracted: it is SLayer-generated text, so its
   embedded aliases are legitimate fitting targets.
3. **Exempt wins on a spelling tie.** A name that is both user-authored and
   SLayer-minted passes through unfitted everywhere: user SQL must never break; the
   identical-spelling internal alias degrades to today's uniform-truncation behaviour.
   Per-occurrence provenance tracking rejected as complexity without a realistic case.
4. **Masking shared by scan and substitution.** `substitute_quoted` becomes
   literal/comment-safe (mask `'…'` with `''` doubling, `--` and `/* */` comments,
   Postgres `$$` bodies; substitute; unmask), fixing the pre-existing DEV-1756
   exposure too. Candidates containing the dialect's quote character are skipped —
   SLayer-minted names never contain quotes, so those are user text. A full
   dialect-aware lexer was rejected: the exposure window is quoted spans, over-limit,
   non-exempt only.
5. **Two fail-closed guards.** (a) Collision: two distinct names fitting to one form,
   or a fitted form equal to any token already present (exempt included) →
   `IdentifierCollisionError`; the guard never polices pre-existing short names
   against each other. (b) Backstop, **always on in production**: after the full
   rewrite (post dot-mangle), any surviving non-exempt over-limit identifier-shaped
   token → typed error, called beside `maybe_validate_scopes` at both generator call
   sites. With exemptions explicit, a survivor is by definition unaccounted-for.

## Risks / Trade-offs

- [Same-spelling tie leaves that internal alias truncating as today] → documented
  tie-break (decision 3); the shapes of canonical aliases make a byte-equal user
  column pathological.
- [Stale stored `backing_query_sql` referencing an over-limit physical column whose
  source model is outside the bundle would be fitted and break] → accepted residual
  corner: requires a >limit physical column AND a query-backed model regenerated
  before this fix; regenerating the backing SQL heals it.
- [Backstop rejects a query the target DB would have run via truncation] → only for
  non-exempt tokens, i.e. genuinely unaccounted provenance; rejecting beats silently
  wrong bindings (principle 9), and the exemption inventory is tested per surface.
- [Substitution hits an exempt-shaped token inside a literal] → masking removes
  literal/comment content from both scan and substitution.

## Migration Plan

Pure emission-side change; no storage or API migration. Golden re-bless limited to
`lift/nested_attach` × postgres/tsql/bigquery/duckdb per the divergence protocol.
Rollback = revert; canonical plans are unaffected.
