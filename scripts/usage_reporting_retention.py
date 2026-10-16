"""Delete SLayer usage reports from PostHog once past retention, or one install's on request.

Run weekly by `.github/workflows/usage-reporting-retention.yml`; stdlib only.
"""

import functools
import json
import os
import sys
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from typing import Any

HOST = "https://eu.posthog.com"
PROJECT_ID = "308099"
EVENT = "slayer_usage"
# An install ID sends nothing once this old (it is replaced), so its whole history can go.
ID_LIFETIME_MONTHS = 11
GRACE_DAYS = 7
BATCH = 1000
QUERY_PAGE = 10_000
TIMEOUT = 60.0

Post = Callable[[str, dict[str, Any]], dict[str, Any]]


def _post(path: str, body: dict[str, Any], *, key: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{HOST}/api/projects/{PROJECT_ID}/{path}", data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return json.loads(response.read() or b"{}")


def expired_install_ids(post: Post) -> list[str]:
    """Install IDs whose first report is older than their lifetime plus a grace period."""
    install_ids: list[str] = []
    while True:
        query = (
            f"SELECT distinct_id FROM events WHERE event = '{EVENT}' GROUP BY distinct_id "
            f"HAVING min(timestamp) < now() - INTERVAL {ID_LIFETIME_MONTHS} MONTH - INTERVAL {GRACE_DAYS} DAY "
            f"ORDER BY distinct_id LIMIT {QUERY_PAGE} OFFSET {len(install_ids)}"
        )
        response = post("query/", {"query": {"kind": "HogQLQuery", "query": query}, "name": "usage-reporting retention"})
        page = [row[0] for row in response["results"]]
        install_ids.extend(page)
        if len(page) < QUERY_PAGE:
            return install_ids


def delete_installs(install_ids: list[str], post: Post) -> list[Any]:
    """Delete each install's person and events; return PostHog's deletion errors."""
    errors: list[Any] = []
    for start in range(0, len(install_ids), BATCH):
        response = post("persons/bulk_delete/", {"distinct_ids": install_ids[start:start + BATCH], "delete_events": True})
        if "persons_found" not in response:
            errors.append({"batch_start": start, "unexpected_response": sorted(response)})
        errors.extend(response.get("deletion_errors", []))
    return errors


def main(env: Mapping[str, str] = os.environ, post: Post | None = None) -> int:
    key = env.get("POSTHOG_PERSONAL_API_KEY", "")
    if not key:
        print("POSTHOG_PERSONAL_API_KEY is not set", file=sys.stderr)
        return 2
    post = post or functools.partial(_post, key=key)
    requested = env.get("INSTALL_ID", "").strip()
    if requested:
        try:
            install_ids = [str(uuid.UUID(requested))]
        except ValueError:
            print(f"not an install ID: {requested!r}", file=sys.stderr)
            return 2
    else:
        install_ids = expired_install_ids(post)
    errors = delete_installs(install_ids, post)
    print(f"deletion requested for {len(install_ids)} install(s); {len(errors)} error(s)")
    if errors:
        print(json.dumps(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
