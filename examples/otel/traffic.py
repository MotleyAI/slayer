"""Send a mix of REST queries to SLayer so SigNoz has traces and metrics to show.

Usage: python traffic.py [BASE_URL] [ROUNDS]   (defaults: http://localhost:5143, 20)
"""

import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5143"
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 20

QUERIES = [
    {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"]},
    {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["products.category"]},
    {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["regions.name"]},
    {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"], "dry_run": True},
    {"source_model": "no_such_model", "measures": ["count(*)"]},
]


def post(path: str, body: dict) -> int:
    request = urllib.request.Request(
        f"{BASE}{path}", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def wait_until_ready() -> None:
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{BASE}/health", timeout=5):
                return
        except OSError:
            time.sleep(2)
    raise SystemExit(f"SLayer at {BASE} did not become ready")


wait_until_ready()
for round_number in range(1, ROUNDS + 1):
    statuses = [post("/query", query) for query in QUERIES]
    print(f"round {round_number}: {statuses}", flush=True)
    time.sleep(1)
