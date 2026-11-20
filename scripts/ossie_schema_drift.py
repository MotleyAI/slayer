"""Compare the vendored Ossie corpus with upstream main, and harvest upstream's validator test documents.

Run weekly by `.github/workflows/ossie-schema-drift.yml`; the file list comes from the corpus provenance file.
Exit 0 = all identical, 1 = any file drifted (diffs printed), 2 = any upstream fetch failed.

`--harvest <checkout> [--out DIR]` runs upstream `validation/test_validate.py` and `validation/tests/` through
`uv run` with `ossie_harvest_plugin.py`, writing every semantic-model document they pass to `validate_document` and
upstream accepts to DIR (default: the corpus `harvested/`). Exit 0 = harvested, 3 = the harvest failed.
"""

import argparse
import difflib
import http.client
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path
from typing import Any

import yaml

REPO = "apache/ossie"
SCHEMA_PATH = "core-spec/ossie-schema.json"
ROOT = Path(__file__).resolve().parent.parent
CORPUS = ROOT / "tests" / "fixtures" / "ossie_corpus"
PROVENANCE = CORPUS / "provenance.yaml"
HARVESTED = CORPUS / "harvested"
HARVEST_PLUGIN = "ossie_harvest_plugin"
HARVEST_TESTS = ("validation/test_validate.py", "validation/tests")
COMMITS_URL = f"https://api.github.com/repos/{REPO}/commits?per_page=1"
TIMEOUT = 60.0
HARVEST_TIMEOUT = 1800

EXIT_SAME, EXIT_DRIFT, EXIT_FETCH, EXIT_HARVEST = 0, 1, 2, 3


def raw_url(path: str) -> str:
    """The raw URL of ``path`` on upstream main."""
    return f"https://raw.githubusercontent.com/{REPO}/main/{path}"


def vendored_files() -> list[tuple[str, Path]]:
    """(upstream path, vendored path) for every file the provenance lists; the corpus keeps upstream's layout."""
    provenance = yaml.safe_load(PROVENANCE.read_text(encoding="utf-8"))
    return [(upstream, CORPUS / upstream) for upstream in provenance["files"]]


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


_SCRIPT_BLOCK = re.compile(r"^# /// script\s*$\n(?P<body>(?:^#(?: .*)?$\n)+?)^# ///\s*$", re.MULTILINE)


def script_dependencies(script: Path) -> list[str]:
    """The ``dependencies`` of a script's inline (PEP 723) metadata block."""
    match = _SCRIPT_BLOCK.search(script.read_text(encoding="utf-8"))
    if match is None:
        raise ValueError(f"{script} has no inline script metadata")
    body = "\n".join(line[2:] if line.startswith("# ") else line[1:] for line in match["body"].splitlines())
    return list(tomllib.loads(body).get("dependencies", []))


def harvest_command(checkout: Path, *, uv: str) -> list[str]:
    """``uv run`` of upstream's validator tests under pytest with the harvest plugin and validate.py's dependencies."""
    deps = ["pytest", *script_dependencies(checkout / "validation" / "validate.py")]
    return [uv, "run", "--no-project", *(arg for dep in deps for arg in ("--with", dep)), "python", "-m", "pytest",
            "-p", HARVEST_PLUGIN, "-p", "no:cacheprovider", "--import-mode=importlib", "-q",
            "--rootdir", str(checkout), *(str(checkout / test) for test in HARVEST_TESTS)]


def harvest(*, checkout: Path, out: Path) -> int:
    """Harvest upstream's accepted validator test documents from ``checkout`` into ``out``."""
    uv = shutil.which("uv")
    if uv is None:
        print("The harvest runs upstream's tests through `uv run`, and uv is not on PATH.", file=sys.stderr)
        return EXIT_HARVEST
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / "harvested"
        env = {**os.environ, "OSSIE_HARVEST_OUT": str(staged),
               "OSSIE_HARVEST_VALIDATE": str(checkout / "validation" / "validate.py"),
               "PYTHONPATH": os.pathsep.join([str(Path(__file__).resolve().parent), str(checkout / "validation")]),
               "PYTHONDONTWRITEBYTECODE": "1"}
        run = subprocess.run(harvest_command(checkout, uv=uv), cwd=checkout, env=env, capture_output=True, text=True,
                             check=False, timeout=HARVEST_TIMEOUT)
        print(run.stdout[-4000:])
        if run.returncode != 0:
            print(f"Upstream's validator tests did not pass (pytest exit {run.returncode}):\n{run.stderr[-4000:]}",
                  file=sys.stderr)
            return EXIT_HARVEST
        texts = {path.name: path.read_text(encoding="utf-8") for path in sorted(staged.glob("*.yaml"))}
    if not texts:
        print("The harvest captured no accepted document.", file=sys.stderr)
        return EXIT_HARVEST
    out.mkdir(parents=True, exist_ok=True)
    for stale in out.glob("*.yaml"):
        stale.unlink()
    for name, text in texts.items():
        (out / name).write_text(text, encoding="utf-8")
    print(f"Harvested {len(texts)} document(s) into {out}.")
    return EXIT_SAME


def cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--harvest", type=Path, metavar="CHECKOUT", help="an apache/ossie checkout to harvest")
    parser.add_argument("--out", type=Path, default=HARVESTED, help="where harvested documents go")
    args = parser.parse_args(argv)
    if args.harvest is not None:
        return harvest(checkout=args.harvest.resolve(), out=args.out.resolve())
    return main()


if __name__ == "__main__":
    sys.exit(cli(sys.argv[1:]))
