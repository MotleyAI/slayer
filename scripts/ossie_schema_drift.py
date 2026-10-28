"""Compare the vendored Ossie core-spec schema with upstream main.

Run weekly by `.github/workflows/ossie-schema-drift.yml`; stdlib only.
Exit 0 = identical, 1 = schema drifted (diff printed), 2 = upstream fetch failed.
"""

import difflib
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPO = "open-semantic-interchange/OSI"
SCHEMA_PATH = "core-spec/ossie-schema.json"
VENDORED = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "osi" / "ossie-schema.json"
RAW_URL = f"https://raw.githubusercontent.com/{REPO}/main/{SCHEMA_PATH}"
COMMITS_URL = f"https://api.github.com/repos/{REPO}/commits?path={SCHEMA_PATH}&per_page=1"
TIMEOUT = 60.0

EXIT_SAME, EXIT_DRIFT, EXIT_FETCH = 0, 1, 2


def canonical(schema: Any) -> list[str]:
    """Key-sorted, indented JSON lines, so formatting never reads as drift."""
    return json.dumps(schema, sort_keys=True, indent=2).splitlines()


def drift(*, vendored: Any, upstream: Any) -> list[str]:
    """Unified diff of the canonical forms; empty when the schemas agree."""
    return list(difflib.unified_diff(canonical(vendored), canonical(upstream),
                                     fromfile="vendored", tofile="upstream", lineterm=""))


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response:  # noqa: S310 — fixed https URLs
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    try:
        upstream = _get_json(RAW_URL)
        commits = _get_json(COMMITS_URL)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Could not fetch the upstream Ossie schema: {exc}", file=sys.stderr)
        return EXIT_FETCH
    sha = commits[0]["sha"] if commits else "unknown"
    print(f"Upstream {REPO}@{sha} {SCHEMA_PATH}")
    diff = drift(vendored=json.loads(VENDORED.read_text(encoding="utf-8")), upstream=upstream)
    if not diff:
        print("Vendored schema matches upstream.")
        return EXIT_SAME
    print("Upstream schema drifted from tests/fixtures/osi/ossie-schema.json:")
    print("\n".join(diff))
    return EXIT_DRIFT


if __name__ == "__main__":
    sys.exit(main())
