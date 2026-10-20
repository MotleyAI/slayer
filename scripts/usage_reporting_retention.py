"""Delete SLayer usage reports from PostHog once past retention, or one install's on request.

Run weekly by `.github/workflows/usage-reporting-retention.yml`; stdlib only.
"""

import functools
import json
import os
import re
import sys
import urllib.error
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
_INSTALL_ID = re.compile(r"[0-9a-f-]{36}")

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
        # Keyset pagination (personal API keys may not use OFFSET); only UUID-shaped IDs are
        # selected, and each is checked, so the next page's key is safe to inline.
        after = install_ids[-1] if install_ids else ""
        query = (
            f"SELECT distinct_id FROM events WHERE event = '{EVENT}' AND length(distinct_id) = 36 "
            f"AND match(distinct_id, '^[0-9a-f-]+$') AND distinct_id > '{after}' GROUP BY distinct_id "
            f"HAVING min(timestamp) < now() - INTERVAL {ID_LIFETIME_MONTHS} MONTH - INTERVAL {GRACE_DAYS} DAY "
            f"ORDER BY distinct_id LIMIT {QUERY_PAGE}"
        )
        response = post("query/", {"query": {"kind": "HogQLQuery", "query": query}, "name": "usage-reporting retention"})
        page = [_install_id(row[0]) for row in response["results"]]
        install_ids.extend(page)
        if len(page) < QUERY_PAGE:
            return install_ids


def _install_id(value: object) -> str:
    if not isinstance(value, str) or not _INSTALL_ID.fullmatch(value):
        raise ValueError(f"unexpected distinct_id from PostHog: {value!r}")
    return value


def delete_installs(install_ids: list[str], post: Post) -> tuple[int, list[Any]]:
    """Delete each install's person and events; return how many persons PostHog found, and its errors."""
    found = 0
    errors: list[Any] = []
    for start in range(0, len(install_ids), BATCH):
        response = post("persons/bulk_delete/", {"distinct_ids": install_ids[start:start + BATCH], "delete_events": True})
        if "persons_found" not in response:
            errors.append({"batch_start": start, "unexpected_response": sorted(response)})
        found += response.get("persons_found", 0)
        errors.extend(response.get("deletion_errors", []))
    return found, errors


def main(env: Mapping[str, str] = os.environ, post: Post | None = None) -> int:
    key = env.get("POSTHOG_PERSONAL_API_KEY", "")
    if not key:
        print("POSTHOG_PERSONAL_API_KEY is not set", file=sys.stderr)
        return 2
    post = post or functools.partial(_post, key=key)
    requested = env.get("INSTALL_ID", "").strip()
    try:
        install_ids = [str(uuid.UUID(requested))] if requested else []
    except ValueError:
        print(f"not an install ID: {requested!r}", file=sys.stderr)
        return 2
    try:
        if not requested:
            install_ids = expired_install_ids(post)
        found, errors = delete_installs(install_ids, post)
    except urllib.error.HTTPError as exc:
        print(f"PostHog returned {exc.code}: {exc.read().decode(errors='replace')[:1000]}", file=sys.stderr)
        return 1
    # Fewer found than requested is expected: events sent without a person, or deletions still queued.
    print(f"{len(install_ids)} install(s) requested, {found} found and queued for deletion; {len(errors)} error(s)")
    if errors:
        print(json.dumps(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
