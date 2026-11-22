"""Compare the vendored Ossie corpus with upstream main, and harvest upstream's validator test documents.

Run weekly by `.github/workflows/ossie-schema-drift.yml`; the file list comes from the corpus provenance file.
Exit 0 = all identical, 1 = any file drifted (diffs printed), 2 = any upstream fetch failed.

`--harvest <checkout> [--out DIR]` runs upstream `validation/test_validate.py` and `validation/tests/` through
`uv run` with `ossie_harvest_plugin.py`, writing every semantic-model document they pass to `validate_document` and
upstream accepts to DIR (default: the corpus `harvested/`). Exit 0 = harvested, 3 = the harvest failed.

`--vendor <checkout> [--out DIR]` re-vendors the corpus (default DIR: the corpus) from an apache/ossie checkout: copies
every managed file byte for byte, harvests, then swaps the snapshot in and writes the provenance last. Exit 0 =
vendored, 3 = the harvest failed, 4 = refused (a path escaping the checkout or the corpus, a missing managed file).
"""

import argparse
import difflib
import hashlib
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
from pathlib import Path, PurePosixPath
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

EXIT_SAME, EXIT_DRIFT, EXIT_FETCH, EXIT_HARVEST, EXIT_VENDOR = 0, 1, 2, 3, 4

# The managed file set: these files, plus every tracked file under these directories.
MANAGED_FILES = (
    "LICENSE", "NOTICE", SCHEMA_PATH, "core-spec/spec.md", "core-spec/spec.yaml", "core-spec/expression_language.md",
    "core-spec/relational_semantics.md", "core-spec/wide_table_semantics.md", "core-spec/spec_layers.md",
    "compliance/expressions/valid.txt", "compliance/expressions/invalid.txt", "validation/validate.py",
    "python/src/ossie/_loading.py",
)
MANAGED_DIRS = ("examples/", "compliance/portability/", "compliance/wide_table/", "compliance/query/scott/")
PROVENANCE_HEADER = (
    "# Provenance of the vendored apache/ossie corpus (byte-identical copies at upstream paths, relative to this"
    " directory).\n# Each file maps to its upstream git blob sha; harvested/ holds documents harvested from upstream"
    " validation/tests.\n# Written by `python scripts/ossie_schema_drift.py --vendor <checkout>`.\n"
)


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


class VendorRefused(Exception):
    """A re-vendor input that would read or write outside its roots."""


def blob_sha(data: bytes) -> str:
    """The git blob sha of ``data``."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def _git(checkout: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=checkout, check=True, capture_output=True, text=True).stdout


def _is_managed(path: str) -> bool:
    return path in MANAGED_FILES or path.startswith(MANAGED_DIRS)


def _inside(root: Path, relative: str) -> Path:
    """``root / relative``, refused when it is absolute or resolves outside ``root``."""
    if PurePosixPath(relative).is_absolute() or ".." in PurePosixPath(relative).parts:
        raise VendorRefused(f"{relative!r} escapes {root}")
    target = root / relative
    if not target.resolve().is_relative_to(root.resolve()):
        raise VendorRefused(f"{relative!r} resolves outside {root}")
    return target


def _managed_snapshot(checkout: Path) -> dict[str, bytes]:
    """Every managed file tracked at the checkout's HEAD, read through the work tree."""
    tracked = [p for p in _git(checkout, "ls-files", "-z").split("\0") if p and _is_managed(p)]
    missing = sorted(set(MANAGED_FILES) - set(tracked))
    if missing:
        raise VendorRefused(f"the checkout lacks managed file(s): {', '.join(missing)}")
    return {path: _inside(checkout, path).read_bytes() for path in sorted(tracked)}


def _previous_listing(out: Path) -> list[str]:
    provenance = out / PROVENANCE.name
    if not provenance.is_file():
        return []
    listed = list((yaml.safe_load(provenance.read_text(encoding="utf-8")) or {}).get("files") or {})
    for path in listed:
        _inside(out, path)
    return listed


def _provenance_text(*, commit: str, committed: str, files: dict[str, bytes]) -> str:
    body = {"repository": REPO, "commit": commit, "committed": committed, "harvested": f"{HARVESTED.name}/",
            "files": {path: blob_sha(data) for path, data in sorted(files.items(), key=lambda kv: kv[0].lower())}}
    return PROVENANCE_HEADER + yaml.safe_dump(body, sort_keys=False, width=1000)


def _swap_in(*, out: Path, files: dict[str, bytes], staged_harvest: Path, removed: list[str]) -> None:
    for path in removed:
        target = out / path
        target.unlink(missing_ok=True)
        for parent in target.parents:
            if parent == out or not parent.is_dir() or any(parent.iterdir()):
                break
            parent.rmdir()
    for path, data in files.items():
        target = out / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    harvested = out / HARVESTED.name
    harvested.mkdir(parents=True, exist_ok=True)
    for stale in harvested.glob("*.yaml"):
        stale.unlink()
    for document in sorted(staged_harvest.glob("*.yaml")):
        shutil.copyfile(document, harvested / document.name)


def vendor(*, checkout: Path, out: Path) -> int:
    """Re-vendor the managed files of ``checkout`` into ``out``; ``out`` is untouched unless every step succeeds."""
    try:
        files = _managed_snapshot(checkout)
        removed = [path for path in _previous_listing(out) if path not in files]
        commit = _git(checkout, "rev-parse", "HEAD").strip()
        committed = _git(checkout, "log", "-1", "--format=%cI", "HEAD").strip()[:10]
    except (VendorRefused, OSError, subprocess.CalledProcessError) as exc:
        print(f"Re-vendor refused: {exc}", file=sys.stderr)
        return EXIT_VENDOR
    with tempfile.TemporaryDirectory() as tmp:
        staged_harvest = Path(tmp) / HARVESTED.name
        code = harvest(checkout=checkout, out=staged_harvest)
        if code != EXIT_SAME:
            return code
        _swap_in(out=out, files=files, staged_harvest=staged_harvest, removed=removed)
    (out / PROVENANCE.name).write_text(_provenance_text(commit=commit, committed=committed, files=files),
                                       encoding="utf-8")
    print(f"Vendored {len(files)} file(s) from {REPO}@{commit} into {out}.")
    return EXIT_SAME


def cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--harvest", type=Path, metavar="CHECKOUT", help="an apache/ossie checkout to harvest")
    mode.add_argument("--vendor", type=Path, metavar="CHECKOUT", help="an apache/ossie checkout to re-vendor from")
    parser.add_argument("--out", type=Path, help="where harvested documents (default: the corpus harvested/) or the "
                                                  "vendored corpus (default: the corpus) go")
    args = parser.parse_args(argv)
    if args.harvest is not None:
        return harvest(checkout=args.harvest.resolve(), out=(args.out or HARVESTED).resolve())
    if args.vendor is not None:
        return vendor(checkout=args.vendor.resolve(), out=(args.out or CORPUS).resolve())
    return main()


if __name__ == "__main__":
    sys.exit(cli(sys.argv[1:]))
