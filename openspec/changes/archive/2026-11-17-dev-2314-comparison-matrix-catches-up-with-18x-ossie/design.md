## Context

Base and PR target: `1.x` (version 1.8.5). See proposal.md for the motivation.

- `main` already carries Q27 and the Q22 update (commits `e9914e5a`, `c1444f09`), but its
  `examples/comparisons/` diff also flips C9 to yes, rewrites the Q6 caveat and adds explicit broadcast options. All
  three are 2.0-only, so the diff is taken piecewise, not wholesale.
- Verified before planning:
  - `main`'s 7 `Q27-*` SLayer probes pass on 1.8.5.
  - With `main`'s `dataset.sql`, `probes.yaml` and `customers.yaml` overlaid, the full SLayer suite passes except the
    4 C9 probes, which are 2.0-only and not taken.
  - An Ossie round trip of the probe models (export from one store, `import-osi` into a fresh store under the same
    datasource name) gives 8 models equal to the originals once the importer's `osi_*` keys are dropped from `meta`.
    `monthly_rev` and `avg_customer_rev` come back query-backed.
  - Re-importing into the same store under a second datasource makes query-backed models ambiguous
    ("orders exists in multiple datasources"), so the round trip needs a fresh store.
  - The export report lists `aov_chg` (a `change_pct` transform) as `extension_only`; with `--no-extensions` it is
    `dropped` ("not exported").
- Architecture: `examples/` and `tests/test_comparison_matrix.py` are outside the LikeC4 model and no
  `system.arc42.md` principle binds example harnesses, so there are no harness edits and no spec attachment.

## Goals / Non-Goals

**Goals:**

- C23, Q22, Q27 and C4 SLayer cells on `1.x` state what 1.8.5 ships. C23 and Q27 rest on probes that pass against
  1.8.5; Q22 rests on the Q27 probes; C4 rests on `docs/database-support.md`.
- The `1.x` → `main` forward-merge is clean for every line taken from `main`. After it, `main` gains C23 and C4 and
  keeps C9 yes and the Q6 associate caveat.
- Probe links in `matrix.json` resolve on the branch that generated it.

**Non-Goals:**

- No probe for C4 (approved exception to "every SLayer verdict backed by a probe").
- No Q22-row probes and no `STRUCT` column in the dataset.
- No C9 probes or cell, no Q6 caveat change, and none of `main`'s explicit `to_many_handling: broadcast` options on
  F9d, Q6a, Q9d and F6.
- No change to the `54-capabilities` slug; the post's title and count are edited in motley-website.
- No change to SLayer itself.

## Decisions

1. **C4 has no probe.** The 9 / 4 counts come from `docs/database-support.md` and the Tier-1 integration suites.
   Options: no probe, or a runner check counting the docs' Tier-1 rows. Taken: no probe, because a DuckDB-only suite
   cannot show engine coverage and a docs-vs-docs check proves nothing about the engines. (user)
2. **Q22 takes `main`'s cell byte for byte; the Q27 probes are its evidence.** Options: verbatim, or Q22-row probes
   plus a `STRUCT` dataset column. Taken: verbatim, for a clean forward-merge, no duplicated probe and no change to the
   shared dataset. A struct column types as `UNKNOWN` on 1.8.5, which is opaque. (user)
3. **C23 SLayer cell and comment.** (user, approved wording)
   - verdict: `'yes'`
   - how: "`slayer export-osi`"
   - text: "Writes an Ossie document that re-imports unedited to the same models (verified); imports Ossie, dbt and Cube."
   - caveat: "What Ossie can't express (transforms, windowed and multi-model measures) travels in a SLayer extension and is reported (verified)."
   - comment: "SLayer and MetricFlow write Ossie; OSI has been renamed Apache Ossie."
4. **The probe-link ref comes from the package version.** `export_matrix.py` reads `version` from `pyproject.toml`:
   below 2 links to `1.x`, anything else (`2.0.0.dev0`, `2.1.0`) to `main`. `PROBES_URL` uses `blob/<ref>/` and
   `source` uses `tree/<ref>/`. Options: from the version, hard-code `1.x` (the forward-merge would silently carry it
   into `main`), or leave `main` (wrong line anchors while the site syncs from `1.x`). Taken: from the version,
   because it is right on both branches with no hand fix-up at merge time and stays deterministic for `--check`.
   (user)
5. **The C23 probes use a round-trip mode on the SLayer block.** `osi_roundtrip: {extensions: bool}` (default
   `true`) works the same way `steps` does: it takes the place of `query` / `run_by_name` / `steps`, except that it
   may be combined with a `query`. The runner:
   1. builds the usual probe store;
   2. runs `export-osi` to a file in the probe's workdir;
   3. runs `import-osi` into a fresh store that holds only the `probe` datasource (same name and granularities);
   4. evaluates `expect` (Decision 7).

   Options: this mode, or a generic CLI step kind inside MCP `steps`. Taken: the mode, because `steps` share one store
   and the round trip needs a fresh one. (default)
6. **The CLI runs as `[sys.executable, "-m", "slayer", ...]`, and each phase is captured separately.** A non-zero
   return code from either export or import fails the probe. (codex)
7. **Round-trip expectations:**
   - `match`: the probe's `query` runs against the re-imported store and is compared with `truth_sql`.
   - `{same_models: true}`: every model in the original store equals its re-imported twin, comparing
     `model_dump(exclude_defaults=True)` with `meta` keys prefixed `osi_` (import provenance) removed, and an emptied
     `meta` dropped, at every nesting level.
   - `{contains, lacks}`: checked against the **export** report text only.

   (codex: the model-equality assertion substantiates "re-imports unedited to the same models"; the export-only report
   check keeps import output from masking export claims.)
8. **The C23 probes:**
   - `C23-ossie-roundtrip-models`: `same_models`.
   - `C23-ossie-roundtrip-transform`: `aov_chg` by month together with revenue by `customers.regions.name`, after
     the round trip, against `truth_sql`.
   - `C23-ossie-roundtrip-query-backed`: `monthly_rev`, after the round trip, against `truth_sql`.
   - `C23-ossie-report-extension`: the export report contains `aov_chg` as carried only in the SLAYER extension.
   - `C23-ossie-report-plain`: with `--no-extensions`, the export report says `aov_chg` is not exported.

   (default; `roundtrip-models` from codex)
9. **Q27 is taken byte for byte from `main`,** including the SLayer caveat "once per tag under `associate`". On 1.x
   `Q27-customers-per-tag` already sets `to_many_handling: associate` explicitly, so the caveat is accurate. (default)
10. **`main`'s `test_list_columns_row_has_probes_on_every_engine` and its `import yaml` are ported byte for byte**
    into `tests/test_comparison_matrix.py`. (codex)
11. **`main`'s explicit broadcast options on F9d, Q6a, Q9d and F6 are not taken.** Broadcast is the 1.x default, and
    leaving those lines alone keeps `main`'s versions on merge. (default)
12. **The `54-capabilities` slug stays,** following DEV-2104's precedent. No other file in the repo states a
    capability count. (default)
13. **All four engine runners run in full,** because the dataset change reaches every engine:
    - SLayer;
    - Malloy;
    - Cube, both planners;
    - MetricFlow.

    Every probe must be PASS or an existing KNOWN-BUG, with no FAIL and no FIXED. (default)
14. **No OpenSpec capability delta (`skip_specs: true`).** SLayer behaviour does not change. (default)

## Risks / Trade-offs

- [The Malloy, Cube or MetricFlow toolchain fails to install locally] → Report the failure and ask; never mark the
  Q27 cells verified without a run.
- [The forward-merge conflicts in `matrix.json` and in `probes.yaml` near the C23 / C9 probe blocks] → Regenerate
  `matrix.json` after the merge, and keep `main`'s C9 probes and cell.
- [`same_models` gets brittle if the importer adds more provenance keys] → It ignores only the `osi_` prefix the
  importer owns, so any other difference is a real round-trip regression.
- [`customers.yaml`'s `tags` line differs from `main`'s (it adds `db_type`)] → The forward-merge keeps `1.x`'s line;
  `main`'s C23 round trip needs it too.
