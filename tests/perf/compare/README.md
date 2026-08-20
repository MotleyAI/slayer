# Engine A/B audit: branch vs pinned PyPI release

Plain scripts (never collected by pytest, never run in CI) that empirically
compare this checkout's query engine against `motley-slayer==0.9.12` (the
latest PyPI release at pin time): result values, error behavior, generated
SQL (recorded, informational), and performance.

## Run

```bash
poetry run python tests/perf/compare/compare.py                 # full audit
poetry run python tests/perf/compare/compare.py --skip-timing   # correctness only
poetry run python tests/perf/compare/compare.py --backends sqlite --entries bench_simple_count
poetry run python tests/perf/compare/compare.py --db-url postgresql://u:p@host/slayer_bench --db-type postgres
```

`--db-url` reseeds destructively; the URL must contain `bench`.

Everything lands in `out/` (gitignored): `report.md`, `correctness.json`,
`perf-flags.json`, `timings.csv`, `run-manifest.json`, per-run JSON artifacts,
seeded DB files, and the reusable baseline venv (`out/pypi-venv`).

## Pieces

- `corpus.py` — ~100 shared queries (pure data), model dicts, adversarial dataset
- `runner.py` — executes the corpus under either interpreter (baseline venv / poetry venv)
- `classify.py` — slayer-free comparison logic (taxonomy, tolerance, perf flags)
- `oracle.py` — pandas ground truth from explicit per-entry specs, arbitrates who is wrong
- `compare.py` — orchestrator: seeding, ABBA timing, classification, report
- `audit_params.py` — scales, repeats, thresholds, backends

## Unit tests for the tooling

```bash
poetry run pytest tests/perf/compare/test_logic.py -o addopts=""
```
