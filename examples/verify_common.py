"""Shared verification utilities for Docker Compose examples.

Usage in verify.py:
    from verify_common import run_common_checks, api, check, results
"""

import json
import os
import sys
import urllib.request

# Import seed data to derive expected counts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from seed import ORDERS

TOTAL_ORDERS = len(ORDERS)
STATUS_COUNTS = {}
for o in ORDERS:
    STATUS_COUNTS[o[4]] = STATUS_COUNTS.get(o[4], 0) + 1

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


def summary():
    """Print summary and exit with appropriate code."""
    print(f"\n{'='*40}")
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

    result = api("POST", "/query", {
        "model": "orders",
        "fields": [{"formula": "count"}],
    })
    check(f"total orders = {TOTAL_ORDERS}", result["data"][0]["orders.count"] == TOTAL_ORDERS)

    result = api("POST", "/query", {
        "model": "orders",
        "fields": [{"formula": "count"}],
        "dimensions": [{"name": "status"}],
    })
    by_status = {r["orders.status"]: r["orders.count"] for r in result["data"]}
    for status, expected in STATUS_COUNTS.items():
        check(f"{status} = {expected}", by_status.get(status) == expected)

    result = api("POST", "/query", {
        "model": "orders",
        "fields": [{"formula": "count"}],
        "filters": ["status == 'completed'"],
    })
    check(f"filter works (completed={STATUS_COUNTS['completed']})", result["data"][0]["orders.count"] == STATUS_COUNTS["completed"])

    result = api("POST", "/query", {
        "model": "orders",
        "fields": [{"formula": "count"}],
        "dimensions": [{"name": "customer_id"}],
        "order": [{"column": {"name": "count"}, "direction": "desc"}],
        "limit": 3,
    })
    check("order + limit returns 3 rows", result["row_count"] == 3)

    result = api("POST", "/query", {
        "model": "products",
        "fields": [{"formula": "count"}],
    })
    check("8 products total", result["data"][0]["products.count"] == 8)

    result = api("POST", "/query", {
        "model": "customers",
        "fields": [{"formula": "count"}],
    })
    check("10 customers total", result["data"][0]["customers.count"] == 10)

    # --- Datasources ---
    print("\nDatasources:")
    datasources = api("GET", "/datasources")
    check("datasource exists", len(datasources) > 0)
    check("demo datasource", any(d["name"] == "demo" for d in datasources))

    return models


def check_rollup(expect_rollup=True):
    """Check joined dimensions on the orders model."""
    print("\nRollup:")
    orders_model = api("GET", "/models/orders")
    dim_names = [d["name"] for d in orders_model.get("dimensions", [])]
    # Joined dimensions use dotted names (e.g., "products.category")
    has_joins = any("." in d for d in dim_names)

    if expect_rollup:
        check("rollup dimensions present", has_joins)
        if has_joins:
            result = api("POST", "/query", {
                "model": "orders",
                "fields": [{"formula": "count"}],
                "dimensions": [{"name": "products.category"}],
            })
            by_cat = {r["orders.products.category"]: r["orders.count"] for r in result["data"]}
            check("rollup by product category works", len(by_cat) > 0)
            check(f"all categories sum to {TOTAL_ORDERS}", sum(by_cat.values()) == TOTAL_ORDERS)

            result = api("POST", "/query", {
                "model": "orders",
                "fields": [{"formula": "count"}],
                "dimensions": [{"name": "regions.name"}],
            })
            by_region = {r["orders.regions.name"]: r["orders.count"] for r in result["data"]}
            check("transitive rollup by region works", len(by_region) > 0)
    else:
        check("no rollup dimensions (expected)", not has_joins)
