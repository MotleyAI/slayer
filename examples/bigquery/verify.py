"""Verification script for the BigQuery example.

Run after starting the SLayer API server (see README.md):

    python examples/bigquery/verify.py

Targets the read-only public dataset ``bigquery-public-data.thelook_ecommerce``.
Assertions avoid hardcoded row counts (the public dataset isn't strictly
frozen) — instead they prove semantic correctness:
  - models load, queries succeed, joins work end-to-end
  - sum of grouped counts equals the ungrouped total (cardinality invariant)
  - aggregates return numbers in plausible ranges
  - time dimension truncation works against TIMESTAMP columns
"""

import json
import os
import sys
import urllib.error
import urllib.request


BASE_URL = os.environ.get("SLAYER_URL", "http://localhost:5143")

# Repeated literals (hoisted per SonarCloud python:S1192) — these strings
# would otherwise show up nine and eight times respectively.
QUERY_PATH = "/query"
COUNT_MEASURE = "*:count"

_passed = 0
_failed = 0


def api(*, method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP {e.code} on {method} {path}", file=sys.stderr)
        if body:
            print(f"    request body: {json.dumps(body)}", file=sys.stderr)
        print(f"    response body: {body_text}", file=sys.stderr)
        raise


def check(*, name, condition):
    global _passed, _failed
    if condition:
        print(f"  PASS: {name}")
        _passed += 1
    else:
        print(f"  FAIL: {name}")
        _failed += 1


def summary():
    print(f"\n{'=' * 40}")
    print(f"Results: {_passed} passed, {_failed} failed")
    if _failed:
        sys.exit(1)
    print("All checks passed!")


def main():
    print("API health:")
    try:
        health = api(method="GET", path="/health")
        check(name="health endpoint", condition=health.get("status") == "ok")
    except Exception as e:
        print(f"  FAIL: cannot reach {BASE_URL} — {e}", file=sys.stderr)
        print("\nStart the server first: ./start.sh", file=sys.stderr)
        sys.exit(1)

    print("\nModels:")
    models = api(method="GET", path="/models")
    names = {m["name"] for m in models}
    for expected in ("orders", "order_items", "products", "users"):
        check(name=f"{expected} model present", condition=expected in names)

    print("\nDatasource:")
    datasources = api(method="GET", path="/datasources")
    check(
        name="thelook datasource registered",
        condition=any(d["name"] == "thelook" for d in datasources),
    )

    # --- Baseline counts ---------------------------------------------------
    print("\nBaseline counts:")
    total_orders = api(
        method="POST", path=QUERY_PATH,
        body={"source_model": "orders", "measures": [COUNT_MEASURE]},
    )["data"][0]["orders._count"]
    check(name="orders count > 0", condition=total_orders > 0)
    print(f"    (orders._count = {total_orders})")

    total_users = api(
        method="POST", path=QUERY_PATH,
        body={"source_model": "users", "measures": [COUNT_MEASURE]},
    )["data"][0]["users._count"]
    check(name="users count > 0", condition=total_users > 0)

    total_products = api(
        method="POST", path=QUERY_PATH,
        body={"source_model": "products", "measures": [COUNT_MEASURE]},
    )["data"][0]["products._count"]
    # thelook_ecommerce.products has roughly 29k rows; allow a wide band.
    check(
        name="products count in [1k, 1M]",
        condition=1_000 < total_products < 1_000_000,
    )

    # --- Cardinality invariant (the SLayer "adding a field can't change
    # cardinality" principle, exercised at the wire layer) ----------------
    print("\nCardinality invariant (sum-of-grouped == total):")
    by_status = api(
        method="POST", path=QUERY_PATH,
        body={
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
            "dimensions": ["status"],
        },
    )["data"]
    summed = sum(row["orders._count"] for row in by_status)
    check(
        name=f"sum(orders by status) == total ({summed} == {total_orders})",
        condition=summed == total_orders,
    )
    statuses = {row["orders.status"] for row in by_status}
    # thelook statuses (stable over years): Complete, Processing, Shipped,
    # Cancelled, Returned. Require at least three of them to be present so
    # the check survives a future status rename.
    expected_subset = {"Complete", "Processing", "Shipped", "Cancelled", "Returned"}
    check(
        name=f"order statuses include >= 3 of {sorted(expected_subset)}",
        condition=len(statuses & expected_subset) >= 3,
    )

    # --- Joined query: order_items → products (category rollup) ----------
    print("\nJoin: order_items by product category:")
    by_category = api(
        method="POST", path=QUERY_PATH,
        body={
            "source_model": "order_items",
            "measures": [COUNT_MEASURE],
            "dimensions": ["products.category"],
        },
    )["data"]
    check(name="by-category rows present", condition=len(by_category) > 0)
    total_items = api(
        method="POST", path=QUERY_PATH,
        body={"source_model": "order_items", "measures": [COUNT_MEASURE]},
    )["data"][0]["order_items._count"]
    summed_cat = sum(row["order_items._count"] for row in by_category)
    check(
        name=f"sum(items by category) == total ({summed_cat} == {total_items})",
        condition=summed_cat == total_items,
    )

    # --- Transitive join: order_items → users → country -------------------
    print("\nTransitive join: order_items by user country:")
    by_country = api(
        method="POST", path=QUERY_PATH,
        body={
            "source_model": "order_items",
            "measures": [COUNT_MEASURE],
            "dimensions": ["users.country"],
            "order": [{"column": "count", "direction": "desc"}],
            "limit": 5,
        },
    )["data"]
    check(name="top-5 countries returned", condition=len(by_country) == 5)
    check(
        name="countries are non-empty strings",
        condition=all(row.get("order_items.users.country") for row in by_country),
    )

    # --- Aggregates on a numeric column ----------------------------------
    print("\nAggregates on order_items.sale_price:")
    aggs = api(
        method="POST", path=QUERY_PATH,
        body={
            "source_model": "order_items",
            "measures": [
                "sale_price:sum",
                "sale_price:avg",
                "sale_price:min",
                "sale_price:max",
            ],
        },
    )["data"][0]
    s = aggs["order_items.sale_price_sum"]
    a = aggs["order_items.sale_price_avg"]
    mn = aggs["order_items.sale_price_min"]
    mx = aggs["order_items.sale_price_max"]
    check(name="sum > 0", condition=s > 0)
    check(name="min >= 0", condition=mn >= 0)
    check(name="max > min", condition=mx > mn)
    check(name="avg between min and max", condition=mn <= a <= mx)

    # --- Time dimension (BigQuery DATE_TRUNC on TIMESTAMP) ---------------
    print("\nTime dimension (month bucket on orders.created_at):")
    by_month = api(
        method="POST", path=QUERY_PATH,
        body={
            "source_model": "orders",
            "measures": [COUNT_MEASURE],
            "time_dimensions": [
                {"dimension": "created_at", "granularity": "month"},
            ],
            "order": [{"column": "created_at", "direction": "asc"}],
            "limit": 6,
        },
    )["data"]
    check(name="month-bucket rows returned", condition=len(by_month) >= 1)
    if by_month:
        first_bucket = by_month[0].get("orders.created_at")
        # BQ emits TIMESTAMP truncated to month; SLayer surfaces it as ISO 8601.
        check(
            name="first bucket parseable",
            condition=first_bucket is not None and ("T" in str(first_bucket) or " " in str(first_bucket)),
        )

    summary()


if __name__ == "__main__":
    main()
