"""Compare the vendored Ossie schema and examples with upstream main.

Run weekly by `.github/workflows/ossie-schema-drift.yml`; the file list comes from the provenance file.
Exit 0 = all identical, 1 = any file drifted (diffs printed), 2 = any upstream fetch failed.
"""

import difflib
import http.client
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

import yaml

REPO = "apache/ossie"
SCHEMA_PATH = "core-spec/ossie-schema.json"
ROOT = Path(__file__).resolve().parent.parent
PROVENANCE = ROOT / "tests" / "fixtures" / "osi" / "ossie-schema.source.yaml"
COMMITS_URL = f"https://api.github.com/repos/{REPO}/commits?per_page=1"
TIMEOUT = 60.0

EXIT_SAME, EXIT_DRIFT, EXIT_FETCH = 0, 1, 2


def raw_url(path: str) -> str:
    """The raw URL of ``path`` on upstream main."""
    return f"https://raw.githubusercontent.com/{REPO}/main/{path}"


def vendored_files() -> list[tuple[str, Path]]:
    """(upstream path, vendored path) for every file the provenance lists."""
    provenance = yaml.safe_load(PROVENANCE.read_text(encoding="utf-8"))
    return [(upstream, PROVENANCE.parent / local) for upstream, local in provenance["files"].items()]


def canonical(schema: Any) -> list[str]:
    """Key-sorted, indented JSON lines, so formatting never reads as drift."""
    return json.dumps(schema, sort_keys=True, indent=2).splitlines()


def drift(*, vendored: Any, upstream: Any) -> list[str]:
    """Unified diff of the canonical forms; empty when the schemas agree."""
    return _diff(canonical(vendored), canonical(upstream))


def _diff(vendored: list[str], upstream: list[str]) -> list[str]:
    return list(difflib.unified_diff(vendored, upstream, fromfile="vendored", tofile="upstream", lineterm=""))


def _get_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response:  # noqa: S310 — fixed https URLs
        return response.read().decode("utf-8")


# OSError: URL, timeout and connection errors; ValueError: bad JSON or UTF-8; HTTPException: a truncated body.
_FETCH_ERRORS = (OSError, ValueError, http.client.HTTPException)


def _upstream_sha() -> str:
    """The last upstream commit, or ``unknown``; informational only."""
    try:
        commits = json.loads(_get_text(COMMITS_URL))
    except _FETCH_ERRORS:
        return "unknown"
    return commits[0]["sha"] if commits else "unknown"


def _file_drift(upstream_path: str, local: Path, upstream_text: str) -> list[str]:
    vendored_text = local.read_bytes().decode("utf-8")
    if upstream_path == SCHEMA_PATH:
        return drift(vendored=json.loads(vendored_text), upstream=json.loads(upstream_text))
    if vendored_text == upstream_text:
        return []
    return _diff(vendored_text.splitlines(), upstream_text.splitlines()) or ["Line endings differ."]


def main() -> int:
    print(f"Upstream {REPO}@{_upstream_sha()}")
    failed, drifted = False, False
    for upstream_path, local in vendored_files():
        try:
            diff = _file_drift(upstream_path, local, _get_text(raw_url(upstream_path)))
        except _FETCH_ERRORS as exc:
            print(f"Could not fetch upstream {upstream_path}: {exc}", file=sys.stderr)
            failed = True
            continue
        if diff:
            drifted = True
            print(f"Upstream {upstream_path} drifted from {local.relative_to(ROOT)}:")
            print("\n".join(diff))
    if failed:
        return EXIT_FETCH
    if drifted:
        return EXIT_DRIFT
    print("Every vendored file matches upstream.")
    return EXIT_SAME


if __name__ == "__main__":
    sys.exit(main())
