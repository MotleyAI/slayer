"""Shared verification utilities for Docker Compose examples.

Usage in verify.py:
    from verify_common import run_common_checks, api, check, results
"""

import json
import os
import statistics
import sys
import urllib.request

# Import seed data to derive expected counts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from seed import ORDERS

TOTAL_ORDERS = len(ORDERS)
STATUS_COUNTS = {}
for o in ORDERS:
    STATUS_COUNTS[o[4]] = STATUS_COUNTS.get(o[4], 0) + 1

# Repeated string literal hoisted to a constant (Sonar python:S1192).
COUNT_MEASURE = "*:count"

BASE_URL = "http://localhost:5143"

_passed = 0
_failed = 0


def api(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method=method,
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def check(name, condition):
    global _passed, _failed
    if condition:
        print(f"  PASS: {name}")
        _passed += 1
    else:
        print(f"  FAIL: {name}")
        _failed += 1


def check_column_types(model_name, expected_types):
    """Assert /models/{name} returns the expected DataType strings.

    expected_types: dict mapping column name to DataType .value string
        (e.g. "number", "string", "time", "date"). Columns absent
        from the dict are ignored — different dialects expose different
        column sets, and this helper is a positive-coverage check, not
        an exhaustive schema comparison.
    """
    model = api("GET", f"/models/{model_name}")
    columns_by_name = {c["name"]: c for c in model.get("columns", [])}
    for col_name, expected_type in expected_types.items():
        col = columns_by_name.get(col_name)
        check(f"{model_name}.{col_name} exists", col is not None)
        if col is None:
            continue
        actual = col.get("type")
        check(
            f"{model_name}.{col_name} type = {expected_type} (got {actual!r})",
            actual == expected_type,
        )


def summary():
    """Print summary and exit with appropriate code."""
    print(f"\n{'=' * 40}")
    print(f"Results: {_passed} passed, {_failed} failed")
    if _failed > 0:
        sys.exit(1)
    print("All checks passed!")


def run_common_checks():
    """Run checks common to all database examples. Returns the models list."""
    # --- Health check ---
    print("API health:")
    try:
        health = api("GET", "/health")
        check("health endpoint", health["status"] == "ok")
    except Exception as e:
        print(f"  FAIL: cannot connect to {BASE_URL} — {e}")
        print("\nMake sure docker compose is running: docker compose up -d")
        sys.exit(1)

    # --- Models ---
    print("\nModels:")
    models = api("GET", "/models")
    model_names = [m["name"] for m in models]
    check("models endpoint returns list", len(models) > 0)
    check("orders model exists", "orders" in model_names)
    check("customers model exists", "customers" in model_names)
    check("products model exists", "products" in model_names)
    check("regions model exists", "regions" in model_names)

    # --- Queries ---
    print("\nQueries:")

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
        },
    )
    check(f"total orders = {TOTAL_ORDERS}", result["data"][0]["orders._count"] == TOTAL_ORDERS)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
            "dimensions": ["status"],
        },
    )
    by_status = {r["orders.status"]: r["orders._count"] for r in result["data"]}
    for status, expected in STATUS_COUNTS.items():
        check(f"{status} = {expected}", by_status.get(status) == expected)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
            "filters": ["status = 'completed'"],
        },
    )
    check(
        f"filter works (completed={STATUS_COUNTS['completed']})",
        result["data"][0]["orders._count"] == STATUS_COUNTS["completed"],
    )

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
            "dimensions": ["customer_id"],
            "order": [{"column": "count", "direction": "desc"}],
            "limit": 3,
        },
    )
    check("order + limit returns 3 rows", result["row_count"] == 3)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "products",
            "measures": [COUNT_MEASURE],
        },
    )
    check("8 products total", result["data"][0]["products._count"] == 8)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "customers",
            "measures": [COUNT_MEASURE],
        },
    )
    check("10 customers total", result["data"][0]["customers._count"] == 10)

    # --- Datasources ---
    print("\nDatasources:")
    datasources = api("GET", "/datasources")
    check("datasource exists", len(datasources) > 0)
    check("demo datasource", any(d["name"] == "demo" for d in datasources))

    return models


def _percentile_cont(values, p):
    """Linear-interpolation percentile, matches Postgres PERCENTILE_CONT.

    Inlined here so verify.py has no third-party dependencies.
    """
    s = sorted(values)
    n = len(s)
    if n == 1:
        return s[0]
    rank = p * (n - 1)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    return s[lo] + (rank - lo) * (s[hi] - s[lo])


def check_median_percentile(measure="quantity"):
    """Run median + percentile against a numeric measure on the orders model.

    Compares against reference values computed in Python from the seed data so
    this works for any database whose dialect SLayer supports for these aggs.
    Do not call from MySQL examples — SLayer raises NotImplementedError for
    median/percentile on MySQL and the API will surface that as an error.
    """
    print("\nMedian / percentile:")

    # Reference values from the seed (orders[3] is quantity in the tuple).
    quantities = [o[3] for o in ORDERS]
    expected_median = statistics.median(quantities)
    expected_p25 = _percentile_cont(values=quantities, p=0.25)
    expected_p75 = _percentile_cont(values=quantities, p=0.75)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [
                f"{measure}:median",
                f"{measure}:percentile(p=0.25)",
                f"{measure}:percentile(p=0.75)",
            ],
        },
    )
    row = result["data"][0]
    got_median = float(row[f"orders.{measure}_median"])
    got_p25 = float(row[f"orders.{measure}_percentile_p_0_25"])
    got_p75 = float(row[f"orders.{measure}_percentile_p_0_75"])

    # Tolerance covers float drift across dialects without masking real bugs.
    check(f"median({measure}) = {expected_median}", abs(got_median - expected_median) < 1e-6)
    check(f"percentile(p=0.25) = {expected_p25}", abs(got_p25 - expected_p25) < 1e-6)
    check(f"percentile(p=0.75) = {expected_p75}", abs(got_p75 - expected_p75) < 1e-6)


def _covariance(xs, ys, *, ddof):
    """Hand-rolled covariance — `statistics` doesn't expose it directly.

    `ddof=1` → sample (Bessel-corrected). `ddof=0` → population.
    """
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - ddof)


def check_stddev_var(measure="quantity"):
    """1-arg stat aggregates: stddev_samp/_pop, var_samp/_pop.

    Safe on every Tier-1 dialect (incl. MySQL — these are native there).
    """
    print("\nStddev / variance:")

    quantities = [o[3] for o in ORDERS]
    expected_stddev_samp = statistics.stdev(quantities)
    expected_stddev_pop = statistics.pstdev(quantities)
    expected_var_samp = statistics.variance(quantities)
    expected_var_pop = statistics.pvariance(quantities)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [
                f"{measure}:stddev_samp",
                f"{measure}:stddev_pop",
                f"{measure}:var_samp",
                f"{measure}:var_pop",
            ],
        },
    )
    row = result["data"][0]
    got_stddev_samp = float(row[f"orders.{measure}_stddev_samp"])
    got_stddev_pop = float(row[f"orders.{measure}_stddev_pop"])
    got_var_samp = float(row[f"orders.{measure}_var_samp"])
    got_var_pop = float(row[f"orders.{measure}_var_pop"])

    check(
        f"stddev_samp({measure}) = {expected_stddev_samp}",
        abs(got_stddev_samp - expected_stddev_samp) < 1e-6,
    )
    check(
        f"stddev_pop({measure}) = {expected_stddev_pop}",
        abs(got_stddev_pop - expected_stddev_pop) < 1e-6,
    )
    check(
        f"var_samp({measure}) = {expected_var_samp}",
        abs(got_var_samp - expected_var_samp) < 1e-6,
    )
    check(
        f"var_pop({measure}) = {expected_var_pop}",
        abs(got_var_pop - expected_var_pop) < 1e-6,
    )


def check_corr_covar(measure="quantity", other="customer_id"):
    """2-arg stat aggregates: corr, covar_samp, covar_pop.

    Do NOT call from MySQL examples — SLayer raises ``NotImplementedError``
    for these on MySQL (no native function, no Python-UDF mechanism).
    Use MariaDB or compute client-side as a workaround.
    """
    print("\nCorrelation / covariance:")

    # ORDERS tuple is (id, customer_id, product_id, quantity, status, created_at)
    # so `quantity` = orders[3], `customer_id` = orders[1].
    field_index = {"id": 0, "customer_id": 1, "product_id": 2, "quantity": 3}
    xs = [o[field_index[measure]] for o in ORDERS]
    ys = [o[field_index[other]] for o in ORDERS]

    expected_corr = statistics.correlation(xs, ys)
    expected_covar_samp = _covariance(xs, ys, ddof=1)
    expected_covar_pop = _covariance(xs, ys, ddof=0)

    result = api(
        "POST",
        "/query",
        {
            "source_model": "orders",
            "measures": [
                f"{measure}:corr(other={other})",
                f"{measure}:covar_samp(other={other})",
                f"{measure}:covar_pop(other={other})",
            ],
        },
    )
    row = result["data"][0]
    got_corr = float(row[f"orders.{measure}_corr_other_{other}"])
    got_covar_samp = float(row[f"orders.{measure}_covar_samp_other_{other}"])
    got_covar_pop = float(row[f"orders.{measure}_covar_pop_other_{other}"])

    check(
        f"corr({measure}, {other}) = {expected_corr}",
        abs(got_corr - expected_corr) < 1e-6,
    )
    check(
        f"covar_samp({measure}, {other}) = {expected_covar_samp}",
        abs(got_covar_samp - expected_covar_samp) < 1e-6,
    )
    check(
        f"covar_pop({measure}, {other}) = {expected_covar_pop}",
        abs(got_covar_pop - expected_covar_pop) < 1e-6,
    )


def check_rollup(expect_rollup=True):
    """Check join-based cross-model queries on the orders model."""
    print("\nJoins:")
    orders_model = api("GET", "/models/orders")
    join_targets = [j["target_model"] for j in orders_model.get("joins", [])]
    has_joins = len(join_targets) > 0

    if expect_rollup:
        check("joins present", has_joins)
        if has_joins:
            result = api(
                "POST",
                "/query",
                {
                    "source_model": "orders",
                    "measures": [COUNT_MEASURE],
                    "dimensions": ["products.category"],
                },
            )
            by_cat = {r["orders.products.category"]: r["orders._count"] for r in result["data"]}
            check("query by product category works", len(by_cat) > 0)
            check(f"all categories sum to {TOTAL_ORDERS}", sum(by_cat.values()) == TOTAL_ORDERS)

            result = api(
                "POST",
                "/query",
                {
                    "source_model": "orders",
                    "measures": [COUNT_MEASURE],
                    "dimensions": ["customers.regions.name"],
                },
            )
            by_region = {r["orders.customers.regions.name"]: r["orders._count"] for r in result["data"]}
            check("transitive join by region works", len(by_region) > 0)
    else:
        check("no joins (expected)", not has_joins)
