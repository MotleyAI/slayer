"""Forward-merge spec guard: every ``1.x`` requirement a pull request brings in survives verbatim or is ledgered.

Run from the repo root: ``python -m scripts.forward_merge_specs --base <sha> --head <sha> [--one-x origin/1.x]``.
"""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys

import yaml
from pydantic import BaseModel

SPECS_ROOT = "openspec/specs/"
SPEC_FILE = "/spec.md"
LEDGER = "openspec/forward-merge-ledger.yaml"
REQUIREMENT_PREFIX = "### Requirement:"


class GuardError(Exception):
    pass


class Requirement(BaseModel):
    capability: str
    title: str
    body_hash: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.title, self.body_hash)


class LedgerEntry(BaseModel):
    title: str
    one_x_hash: str
    reason: str = ""

    @property
    def key(self) -> tuple[str, str]:
        return (self.title, self.one_x_hash)


class Options(BaseModel):
    base: str
    head: str
    one_x: str


def _git(*args: str) -> str:
    done = subprocess.run(["git", *args], check=False, capture_output=True, text=True)
    if done.returncode != 0:
        raise GuardError(f"git {' '.join(args)} failed: {done.stderr.strip()}")
    return done.stdout


def _is_ancestor(*, ancestor: str, descendant: str) -> bool:
    done = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant], check=False, capture_output=True, text=True
    )
    if done.returncode not in (0, 1):
        raise GuardError(f"git merge-base --is-ancestor failed: {done.stderr.strip()}")
    return done.returncode == 0


def _merge_base(*, rev: str, one_x: str) -> str:
    """The one merge base of ``rev`` and ``one_x``."""
    done = subprocess.run(["git", "merge-base", "--all", rev, one_x], check=False, capture_output=True, text=True)
    bases = done.stdout.split()
    if not bases:
        raise GuardError(f"{rev} has no merge base with {one_x}.")
    if len(bases) > 1:
        raise GuardError(f"{rev} has several merge bases with {one_x}: {', '.join(bases)}.")
    return bases[0]


def normalised_hash(body: str) -> str:
    return hashlib.sha256(" ".join(body.split()).encode()).hexdigest()


def parse_requirements(*, capability: str, text: str) -> list[Requirement]:
    """Each ``### Requirement:`` block; its body runs to the next heading of level 3 or above."""
    found: list[Requirement] = []
    title: str | None = None
    body: list[str] = []

    def close() -> None:
        if title is not None:
            found.append(Requirement(capability=capability, title=title, body_hash=normalised_hash("\n".join(body))))

    for line in text.splitlines():
        if line.startswith(REQUIREMENT_PREFIX):
            close()
            title, body = line[len(REQUIREMENT_PREFIX):].strip(), []
        elif line.startswith(("# ", "## ", "### ")):
            close()
            title, body = None, []
        elif title is not None:
            body.append(line)
    close()
    return found


def requirements_at(rev: str) -> list[Requirement]:
    paths = _git("ls-tree", "-r", "--name-only", rev, "--", SPECS_ROOT).splitlines()
    found: list[Requirement] = []
    for path in paths:
        if path.endswith(SPEC_FILE):
            capability = path[len(SPECS_ROOT):-len(SPEC_FILE)]
            found.extend(parse_requirements(capability=capability, text=_git("show", f"{rev}:{path}")))
    return found


def ledger_at(rev: str) -> list[LedgerEntry]:
    if not _git("ls-tree", "--name-only", rev, "--", LEDGER).strip():
        return []
    raw = yaml.safe_load(_git("show", f"{rev}:{LEDGER}")) or []
    if not isinstance(raw, list):
        raise GuardError(f"{LEDGER} must be a list of {{title, one_x_hash, reason}} entries.")
    return [LedgerEntry.model_validate(e) for e in raw]


def check(options: Options) -> list[str]:
    """The failures; empty when the head keeps every ``1.x`` requirement its range brings in."""
    base_mb = _merge_base(rev=options.base, one_x=options.one_x)
    head_mb = _merge_base(rev=options.head, one_x=options.one_x)
    if base_mb == head_mb or not _is_ancestor(ancestor=base_mb, descendant=head_mb):
        return []
    before = {r.key for r in requirements_at(base_mb)}
    brought = requirements_at(head_mb)
    on_head = {r.key for r in requirements_at(options.head)}
    ledger = ledger_at(options.head)
    failures = [f"Ledger entry {e.title!r} ({e.one_x_hash}) has no reason." for e in ledger if not e.reason.strip()]
    one_x_keys = {r.key for r in brought}
    failures += [
        f"Ledger entry {e.title!r} ({e.one_x_hash}) is stale: no 1.x requirement at {head_mb} has that title and hash."
        for e in ledger
        if e.key not in one_x_keys
    ]
    ledgered = {e.key for e in ledger}
    for r in brought:
        if r.key in before or r.key in on_head or r.key in ledgered:
            continue
        accept = yaml.safe_dump(
            [{"title": r.title, "one_x_hash": r.body_hash, "reason": "<why main diverges>"}], sort_keys=False
        )
        failures.append(
            f"1.x requirement missing on the head: {r.capability} › {r.title!r} (1.x body hash {r.body_hash}). "
            f"Restore it verbatim, or accept it in {LEDGER}:\n{accept}"
        )
    return failures


def _parse_args(argv: list[str] | None) -> Options:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--one-x", default="origin/1.x")
    args = parser.parse_args(argv)
    return Options(base=args.base, head=args.head, one_x=args.one_x)


def main(argv: list[str] | None = None) -> int:
    try:
        failures = check(_parse_args(argv))
    except GuardError as exc:
        print(f"forward-merge spec guard: {exc}")
        return 1
    for failure in failures:
        print(failure)
    if failures:
        return 1
    print("forward-merge spec guard: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
