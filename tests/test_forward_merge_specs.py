"""The forward-merge spec guard: a merge of ``1.x`` keeps every requirement it brings in, or ledgers it."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parent.parent
LEDGER = "openspec/forward-merge-ledger.yaml"
FOO = "openspec/specs/models/foo/spec.md"
BAR = "openspec/specs/models/bar/spec.md"
OTHER = "openspec/specs/models/other/spec.md"
GIT_ENV = {
    **os.environ,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
}


class Requirement(BaseModel):
    title: str
    body: str


def req(title: str, text: str) -> Requirement:
    """A requirement with one scenario."""
    return Requirement(title=title, body=f"{text}\n\n#### Scenario: {title}\n- **WHEN** it is checked\n- **THEN** {text}")


def spec(*requirements: Requirement) -> str:
    blocks = "\n\n".join(f"### Requirement: {r.title}\n\n{r.body}" for r in requirements)
    return f"# Spec\n\n## Purpose\n\nTest capability.\n\n## Requirements\n\n{blocks}\n"


def body_hash(body: str) -> str:
    # Normalisation: whitespace runs collapse to one space, then strip.
    return hashlib.sha256(" ".join(body.split()).encode()).hexdigest()


def ledger(*entries: dict[str, str]) -> str:
    return yaml.safe_dump(list(entries))


def entry(requirement: Requirement, *, reason: str) -> dict[str, str]:
    return {"title": requirement.title, "one_x_hash": body_hash(requirement.body), "reason": reason}


A = req("Widgets are blue", "A widget SHALL be blue.")
A_MAIN = req("Widgets are blue", "A widget SHALL be blue or navy.")
A_ONE_X = req("Widgets are blue", "A widget SHALL be blue and round.")
B = req("Gadgets are red", "A gadget SHALL be red.")
B_ALTERED = req("Gadgets are red", "A gadget SHALL be crimson.")
C = req("Gizmos are green", "A gizmo SHALL be green.")
OTHER_REQ = req("Others exist", "Another thing SHALL exist.")
OTHER_REQ_MAIN = req("Others exist", "Another thing SHALL exist twice.")


class GitRepo(BaseModel):
    path: Path

    def git(self, *args: str) -> str:
        done = subprocess.run(["git", *args], cwd=self.path, env=GIT_ENV, check=True, capture_output=True, text=True)
        return done.stdout.strip()

    def rev(self, ref: str) -> str:
        return self.git("rev-parse", ref)

    def checkout(self, *, branch: str, start: str | None = None) -> None:
        self.git("checkout", "-q", *(["-b", branch, start] if start else [branch]))

    def write(self, *, files: dict[str, str | None]) -> None:
        """Write (``None``: delete) and stage each named file."""
        for rel, content in files.items():
            if content is None:
                self.git("rm", "-q", "--", rel)
                continue
            target = self.path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
            self.git("add", "--", rel)

    def commit(self, *, message: str, files: dict[str, str | None]) -> str:
        self.write(files=files)
        self.git("commit", "-q", "--allow-empty", "-m", message)
        return self.rev("HEAD")

    def merge(self, *, ref: str, files: dict[str, str | None]) -> str:
        """Merge ``ref`` with ``files`` as the whole resolution over this branch's tree."""
        self.git("merge", "-q", "--no-ff", "--no-commit", "-s", "ours", ref)
        return self.commit(message=f"Merge '{ref}'", files=files)


class Guard(BaseModel):
    returncode: int
    output: str


def run_guard(repo: GitRepo, *, base: str, head: str) -> Guard:
    done = subprocess.run(
        [sys.executable, "-m", "scripts.forward_merge_specs", "--base", base, "--head", head, "--one-x", "1.x"],
        cwd=repo.path,
        env={**GIT_ENV, "PYTHONPATH": str(REPO_ROOT)},
        capture_output=True,
        text=True,
        check=False,
    )
    return Guard(returncode=done.returncode, output=done.stdout + done.stderr)


def forward_merge(repo: GitRepo, *, branch: str, files: dict[str, str | None]) -> str:
    """A branch off ``main`` merging ``1.x``; returns its head."""
    repo.checkout(branch=branch, start="main")
    return repo.merge(ref="1.x", files=files)


def assert_names(guard: Guard, *, capability: str, requirement: Requirement) -> None:
    assert guard.returncode != 0, guard.output
    assert capability in guard.output
    assert requirement.title in guard.output
    assert body_hash(requirement.body) in guard.output
    # The ledger entry that would accept it.
    assert "one_x_hash" in guard.output and "reason" in guard.output


@pytest.fixture
def repo(tmp_path: Path) -> GitRepo:
    """``1.x`` cut from ``main``'s first commit; ``main`` then moves on."""
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    r = GitRepo(path=tmp_path / "repo")
    r.path.mkdir()
    r.git("init", "-q", "-b", "main")
    r.git("config", "commit.gpgsign", "false")
    r.git("config", "core.hooksPath", str(hooks))
    r.commit(message="init", files={FOO: spec(A), OTHER: spec(OTHER_REQ)})
    r.git("branch", "1.x")
    r.commit(message="2.0 work", files={OTHER: spec(OTHER_REQ_MAIN)})
    return r


def test_no_one_x_commits_enter(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    repo.checkout(branch="feature", start="main")
    head = repo.commit(message="drop A", files={FOO: spec()})

    guard = run_guard(repo, base=base, head=head)

    assert guard.returncode == 0, guard.output


def test_an_added_requirement_missing_fails(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    dropped = forward_merge(repo, branch="fwd", files={FOO: spec(A)})
    kept = forward_merge(repo, branch="fwd-ok", files={FOO: spec(A, B)})

    assert_names(run_guard(repo, base=base, head=dropped), capability="models/foo", requirement=B)
    ok = run_guard(repo, base=base, head=kept)
    assert ok.returncode == 0, ok.output


def test_a_whitespace_only_difference_passes(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    reflowed = Requirement(title=B.title, body=B.body.replace("A gadget SHALL be red.", "A gadget  SHALL\nbe red.  ", 1))
    head = forward_merge(repo, branch="fwd", files={FOO: spec(A, reflowed)})

    guard = run_guard(repo, base=base, head=head)

    assert guard.returncode == 0, guard.output


def test_a_modified_requirement_resolved_toward_base_fails(repo: GitRepo) -> None:
    repo.commit(message="2.0 rewords A", files={FOO: spec(A_MAIN)})
    repo.checkout(branch="1.x")
    repo.commit(message="1.x rewords A", files={FOO: spec(A_ONE_X)})
    base = repo.rev("main")
    head = forward_merge(repo, branch="fwd", files={FOO: spec(A_MAIN)})

    assert_names(run_guard(repo, base=base, head=head), capability="models/foo", requirement=A_ONE_X)


def test_a_requirement_under_a_moved_capability_passes(repo: GitRepo) -> None:
    repo.commit(message="move foo to bar", files={FOO: None, BAR: spec(A)})
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    moved = forward_merge(repo, branch="fwd", files={BAR: spec(A, B)})
    altered = forward_merge(repo, branch="fwd-altered", files={BAR: spec(A, B_ALTERED)})

    ok = run_guard(repo, base=base, head=moved)
    assert ok.returncode == 0, ok.output
    assert_names(run_guard(repo, base=base, head=altered), capability="models/foo", requirement=B)


def test_a_ledgered_divergence_passes(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    ledgered = forward_merge(
        repo, branch="fwd", files={FOO: spec(A), LEDGER: ledger(entry(B, reason="Superseded by the gadget redesign."))}
    )
    wrong_hash = forward_merge(
        repo, branch="fwd-wrong", files={FOO: spec(A), LEDGER: ledger(entry(B_ALTERED, reason="Superseded."))}
    )

    ok = run_guard(repo, base=base, head=ledgered)
    assert ok.returncode == 0, ok.output
    assert_names(run_guard(repo, base=base, head=wrong_hash), capability="models/foo", requirement=B)


def test_a_ledger_entry_without_a_reason_fails(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    empty = forward_merge(repo, branch="fwd", files={FOO: spec(A, B), LEDGER: ledger(entry(B, reason=""))})
    reasoned = forward_merge(repo, branch="fwd-ok", files={FOO: spec(A, B), LEDGER: ledger(entry(B, reason="Kept."))})

    guard = run_guard(repo, base=base, head=empty)
    assert guard.returncode != 0, guard.output
    assert B.title in guard.output
    assert "reason" in guard.output.lower()
    ok = run_guard(repo, base=base, head=reasoned)
    assert ok.returncode == 0, ok.output


@pytest.mark.parametrize(
    "stale",
    [
        {"title": "Sprockets are grey", "one_x_hash": body_hash("A sprocket SHALL be grey."), "reason": "Gone."},
        {"title": B.title, "one_x_hash": body_hash(B_ALTERED.body), "reason": "Outdated."},
    ],
    ids=["unknown-title", "outdated-hash"],
)
def test_a_stale_ledger_entry_fails(repo: GitRepo, stale: dict[str, str]) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.rev("main")
    head = forward_merge(repo, branch="fwd", files={FOO: spec(A, B), LEDGER: ledger(stale)})

    guard = run_guard(repo, base=base, head=head)

    assert guard.returncode != 0, guard.output
    assert stale["title"] in guard.output
    assert "stale" in guard.output.lower()


def test_a_removal_on_one_x_passes(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    repo.commit(message="drop A, add B", files={FOO: spec(B)})
    base = repo.rev("main")
    head = forward_merge(repo, branch="fwd", files={FOO: spec(A, B)})

    guard = run_guard(repo, base=base, head=head)

    assert guard.returncode == 0, guard.output


def test_a_branch_behind_one_x_passes_without_comparing(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    older = repo.commit(message="add B", files={FOO: spec(A, B)})
    base = repo.commit(message="drop B, add C", files={FOO: spec(A, C)})
    repo.checkout(branch="fix", start=older)
    head = repo.commit(message="drop B", files={FOO: spec(A)})

    guard = run_guard(repo, base=base, head=head)

    assert guard.returncode == 0, guard.output


def test_a_cherry_pick_is_checked_at_the_next_forward_merge(repo: GitRepo) -> None:
    repo.checkout(branch="1.x")
    picked = repo.commit(message="fix + B", files={FOO: spec(A, B), "fix.txt": "fixed\n"})
    repo.checkout(branch="main")
    before_pick = repo.rev("main")
    repo.git("cherry-pick", "--no-commit", picked)
    repo.git("checkout", "HEAD", "--", FOO)
    after_pick = repo.commit(message="cherry-pick fix without B", files={})

    pick = run_guard(repo, base=before_pick, head=after_pick)
    assert pick.returncode == 0, pick.output
    head = forward_merge(repo, branch="fwd", files={FOO: spec(A), "fix.txt": "fixed\n"})
    assert_names(run_guard(repo, base=after_pick, head=head), capability="models/foo", requirement=B)


def test_several_merge_bases_fail_naming_them(repo: GitRepo) -> None:
    main_tip = repo.rev("main")
    repo.checkout(branch="1.x")
    one_x_tip = repo.commit(message="add B", files={FOO: spec(A, B)})
    repo.git("merge", "-q", "--no-ff", "-m", "Merge branch 'main'", "main")
    repo.checkout(branch="fwd", start="main")
    repo.git("merge", "-q", "--no-ff", "-m", "Merge commit", one_x_tip)
    head = repo.rev("HEAD")

    guard = run_guard(repo, base=main_tip, head=head)

    assert guard.returncode != 0, guard.output
    assert main_tip[:7] in guard.output
    assert one_x_tip[:7] in guard.output
