"""Convert pytest-benchmark JSON output to a pivot CSV.

Usage:
    poetry run pytest tests/perf/ --benchmark-only --benchmark-json=bench.json
    python tests/perf/bench_to_csv.py bench.json           # prints to stdout
    python tests/perf/bench_to_csv.py bench.json -o out.csv # writes to file
"""

import csv
import io
import json
import sys


def convert(json_path: str) -> str:
    with open(json_path) as f:
        data = json.load(f)

    # Parse benchmark entries: extract group (scale) and query name
    # Test names look like: tests/perf/test_bench.py::TestBench1K::test_query[monthly_change]
    rows: dict[str, dict[str, float]] = {}  # group → {query_name → mean_ms}
    all_queries: list[str] = []

    for bench in data["benchmarks"]:
        group = bench.get("group", "unknown")
        # Extract query name from params or fullname
        params = bench.get("params", {})
        query_name = params.get("query_name", bench["name"])

        mean_s = bench["stats"]["mean"]
        mean_ms = round(mean_s * 1000, 2)

        if group not in rows:
            rows[group] = {}
        rows[group][query_name] = mean_ms

        if query_name not in all_queries:
            all_queries.append(query_name)

    # Sort groups by numeric part (1k < 10k < 100k)
    def _sort_key(g: str) -> int:
        num = "".join(c for c in g if c.isdigit())
        return int(num) if num else 0

    sorted_groups = sorted(rows.keys(), key=_sort_key)

    # Write CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["scale"] + all_queries)
    for group in sorted_groups:
        row = [group]
        for q in all_queries:
            val = rows[group].get(q)
            row.append(val if val is not None else "")
        writer.writerow(row)

    return output.getvalue()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <bench.json> [-o output.csv]")
        sys.exit(1)

    result = convert(sys.argv[1])

    if "-o" in sys.argv:
        out_path = sys.argv[sys.argv.index("-o") + 1]
        with open(out_path, "w") as f:
            f.write(result)
        print(f"Written to {out_path}")
    else:
        print(result, end="")
