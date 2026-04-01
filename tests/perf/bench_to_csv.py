"""Convert pytest-benchmark JSON output to a pivot CSV.

Usage:
    poetry run pytest tests/perf/ --benchmark-only --benchmark-json=bench.json
    python tests/perf/bench_to_csv.py bench.json           # prints to stdout
    python tests/perf/bench_to_csv.py bench.json -o out.csv # writes to file
"""

import csv
import io
import json
import math
import sys

from tests.perf.conftest import SCALES


def _estimate_complexity(times: list[float], sizes: list[int]) -> str:
    """Estimate Big-O exponent from timing data via power law regression.

    Fits time = c * n^k by computing k = log(t2/t1) / log(n2/n1) for each
    adjacent pair, then takes the median. Returns a human-readable label.
    """
    exponents = []
    for i in range(len(times) - 1):
        t1, t2 = times[i], times[i + 1]
        n1, n2 = sizes[i], sizes[i + 1]
        if t1 > 0 and t2 > 0 and n1 > 0 and n2 > 0 and n1 != n2:
            k = math.log(t2 / t1) / math.log(n2 / n1)
            exponents.append(k)

    if not exponents:
        return "?"

    exponents.sort()
    median_k = exponents[len(exponents) // 2]

    # Map exponent to Big-O label
    if median_k < 0.15:
        return "~O(1)"
    elif median_k < 0.65:
        return "~O(√n)"
    elif median_k < 0.85:
        return "~O(n^0.7)"
    elif median_k < 1.15:
        return "~O(n)"
    elif median_k < 1.65:
        return "~O(n·log(n))"
    elif median_k < 2.15:
        return "~O(n²)"
    else:
        return f"~O(n^{median_k:.1f})"


def convert(json_path: str) -> str:
    with open(json_path) as f:
        data = json.load(f)

    # Parse benchmark entries: extract group (scale) and query name
    rows: dict[str, dict[str, float]] = {}  # group → {query_name → mean_ms}
    all_queries: list[str] = []

    for bench in data["benchmarks"]:
        group = bench.get("group", "unknown")
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
    group_sizes = [SCALES[g] for g in sorted_groups if g in SCALES]

    # Write CSV: queries as rows, scales as columns, plus complexity estimate
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["query"] + sorted_groups + ["complexity"])
    for q in all_queries:
        row = [q]
        times = []
        for group in sorted_groups:
            val = rows[group].get(q)
            row.append(val if val is not None else "")
            times.append(val if val is not None else 0)

        complexity = _estimate_complexity(times, group_sizes)
        row.append(complexity)
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
