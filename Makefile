# Common benchmark flags: disable GC during measurement, warmup enabled,
# minimum 10 rounds per test for stable results
BENCH_FLAGS = --benchmark-only --benchmark-disable-gc --benchmark-warmup=on --benchmark-warmup-iterations=3 --benchmark-min-rounds=5 --benchmark-max-time=10.0

.PHONY: test lint bench bench-report bench-csv

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------

test:
	poetry run pytest tests/ --ignore=tests/perf

lint:
	poetry run ruff check slayer/ tests/

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

# Print results to stdout
bench:
	poetry run pytest tests/perf/ $(BENCH_FLAGS) -v

# Run + save JSON + convert to CSV into benchmarks/
bench-report:
	@test -n "$(NAME)" || (echo "Usage: make bench-report NAME=run1" && exit 1)
	@mkdir -p benchmarks
	poetry run pytest tests/perf/ $(BENCH_FLAGS) --benchmark-json=benchmarks/$(NAME).json -v
	poetry run python tests/perf/bench_to_csv.py benchmarks/$(NAME).json -o benchmarks/$(NAME).csv
	@echo "Results: benchmarks/$(NAME).json + benchmarks/$(NAME).csv"

# Convert an existing JSON to CSV
bench-csv:
	@test -n "$(SRC)" || (echo "Usage: make bench-csv SRC=benchmarks/run1.json [DST=benchmarks/run1.csv]" && exit 1)
	poetry run python tests/perf/bench_to_csv.py $(SRC) $(if $(DST),-o $(DST))
