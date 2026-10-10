"""Drift guard: CONTRIBUTING.md's paths, CI gates and plugin pin must match the repo."""

from __future__ import annotations

import re
import shlex
import tomllib
from collections.abc import Iterator
from pathlib import Path

import yaml
from packaging.version import InvalidVersion, Version

_REPO_ROOT = Path(__file__).resolve().parents[1]
_GUIDE = _REPO_ROOT / "CONTRIBUTING.md"
_CI = _REPO_ROOT / ".github" / "workflows" / "ci.yml"
_GATE_JOBS = ("lint-and-test", "type-check")
_SETUP_TOOLS = frozenset({"pip"})
_PATH_SUFFIXES = (".md", ".yaml", ".toml", ".c4", ".py", ".json")
_SKIP_MARKERS = ("://", "<", "#", "@", "$")
_LOCAL_OVERRIDE = "living-architecture.local.yaml"
_POINTER_URL = "https://github.com/MotleyAI/slayer/blob/main/CONTRIBUTING.md"
_GLOB_CHARS = frozenset("*?[")
_POETRY_RUN = re.compile(r"poetry run (\S+)")
_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
_SHELL_OPERATOR = re.compile(r"&&|\|\||[;|]")


def _tool(command: str) -> str:
    match = _POETRY_RUN.match(command)
    assert match, f"not a `poetry run` command: {command}"
    return match.group(1)


def _lines(markdown: str) -> Iterator[tuple[str, str]]:
    """Yield ``(kind, line)``, kind being ``text``, ``fence`` (a delimiter) or ``code``."""
    fence = ""
    for line in markdown.splitlines():
        match = _FENCE.match(line)
        if match:
            marker = match.group(1)
            if not fence:
                fence = marker
                yield "fence", line
                continue
            if marker[0] == fence[0] and len(marker) >= len(fence) and not line.strip()[len(marker):]:
                fence = ""
                yield "fence", line
                continue
        yield ("code" if fence else "text"), line


def _code_lines(markdown: str) -> list[str]:
    """Fenced-block lines plus inline code spans outside fences."""
    out: list[str] = []
    for kind, line in _lines(markdown):
        if kind == "code":
            out.append(line)
        elif kind == "text":
            out.extend(re.findall(r"`([^`]+)`", line))
    return out


def _path_token(token: str) -> str | None:
    """The repo path a token names, else None."""
    if token.startswith("-"):
        if "=" not in token:
            return None
        token = token.split("=", 1)[1]
    token = token.strip("\"'()[],;:").rstrip(".")
    if not token or token.startswith("/") or any(m in token for m in _SKIP_MARKERS):
        return None
    if "/" in token or token.endswith(_PATH_SUFFIXES):
        return token
    return None


def _guide_paths(markdown: str) -> list[str]:
    paths = [_path_token(tok) for line in _code_lines(markdown) for tok in line.split()]
    return sorted({p for p in paths if p is not None})


def _path_exists(path: str) -> bool:
    if _GLOB_CHARS & set(path):
        return any(_REPO_ROOT.glob(path))
    return (_REPO_ROOT / path).exists()


def _gitignore_lists(entry: str) -> bool:
    lines = {ln.strip() for ln in (_REPO_ROOT / ".gitignore").read_text().splitlines()}
    return entry in lines or f"/{entry}" in lines


def _section(markdown: str, heading: str) -> str:
    """The body of a ``## heading`` section; headings inside fences don't count."""
    body: list[str] = []
    found = inside = False
    for kind, line in _lines(markdown):
        if kind == "text" and line.startswith("## "):
            inside = line[3:].strip() == heading
            found = found or inside
        elif inside:
            body.append(line)
    assert found, f"CONTRIBUTING.md has no '## {heading}' section"
    return "\n".join(body)


def _fenced_commands(markdown: str) -> list[str]:
    """Fenced-block lines with trailing comments and whitespace stripped."""
    commands: list[str] = []
    for kind, line in _lines(markdown):
        if kind == "code":
            command = line.split(" #", 1)[0].strip()
            if command and not command.startswith("#"):
                commands.append(command)
    return commands


def _shell_poetry_commands(script: str) -> list[str]:
    """Every ``poetry run`` command in a shell script, comments excluded."""
    commands: list[str] = []
    for line in script.replace("\\\n", " ").splitlines():
        code = line.split(" #", 1)[0].strip()
        if code.startswith("#"):
            continue
        for part in _SHELL_OPERATOR.split(code):
            part = " ".join(part.split())
            if part.startswith("poetry run "):
                commands.append(part)
    return commands


def _ci_gate_commands() -> list[str]:
    """Every ``poetry run`` command in the gate jobs, setup tools excluded."""
    jobs = yaml.safe_load(_CI.read_text())["jobs"]
    commands: list[str] = []
    for job in _GATE_JOBS:
        assert job in jobs, f"ci.yml has no '{job}' job; update _GATE_JOBS to match the rename"
        for step in jobs[job]["steps"]:
            commands += [c for c in _shell_poetry_commands(step.get("run", "")) if _tool(c) not in _SETUP_TOOLS]
    assert commands, "found no `poetry run` gates in ci.yml"
    return commands


def _checklist() -> list[str]:
    return _fenced_commands(_section(_GUIDE.read_text(), "Pull Requests"))


def _selects_unit_tests(command: str) -> bool:
    args = shlex.split(command)
    return args[:3] == ["poetry", "run", "pytest"] and any(
        a == "-m" and b == "not integration" for a, b in zip(args, args[1:])
    )


def _living_architecture_pin() -> str:
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    spec = data["tool"]["poetry"]["group"]["dev"]["dependencies"]["living-architecture"]
    return spec["version"] if isinstance(spec, dict) else spec


def test_guide_paths_exist() -> None:
    missing = [p for p in _guide_paths(_GUIDE.read_text()) if p != _LOCAL_OVERRIDE and not _path_exists(p)]
    assert not missing, (
        f"CONTRIBUTING.md names paths that do not exist: {missing}. Paths are repo-root-relative; "
        "spell file kinds as globs (`architecture/**/*.c4`), not bare extensions."
    )


def test_local_override_is_gitignored_when_named() -> None:
    if _LOCAL_OVERRIDE in _guide_paths(_GUIDE.read_text()):
        assert _gitignore_lists(_LOCAL_OVERRIDE), f"{_LOCAL_OVERRIDE} is named but not in .gitignore"


def test_checklist_has_every_ci_gate() -> None:
    checklist = _checklist()
    checklist_tools = {_tool(c) for c in checklist if c.startswith("poetry run ")}
    for ci_command in _ci_gate_commands():
        tool = _tool(ci_command)
        assert tool in checklist_tools, f"checklist lacks `poetry run {tool}` (CI: `{ci_command}`)"
        if tool == "pytest":
            continue
        assert ci_command in checklist, f"checklist must run `{ci_command}` exactly as CI does"


def test_checklist_runs_unit_tests_only() -> None:
    assert any(map(_selects_unit_tests, _checklist())), 'checklist pytest line must select `-m "not integration"`'


def test_checklist_regenerates_diagrams() -> None:
    assert any(c.startswith("poetry run ") and _tool(c) == "la-arch-diagrams" for c in _checklist())


def test_docs_development_points_to_guide() -> None:
    assert _POINTER_URL in (_REPO_ROOT / "docs" / "development.md").read_text()


def test_living_architecture_pin_is_exact() -> None:
    pin = _living_architecture_pin()
    try:
        Version(pin)
    except InvalidVersion as exc:
        raise AssertionError(f"living-architecture pin {pin!r} is not an exact version") from exc


def test_guide_installs_pinned_plugin() -> None:
    pin = _living_architecture_pin()
    ref = re.compile(rf"MotleyAI/living-architecture#v{re.escape(pin)}(?![\w.])")
    assert ref.search(_GUIDE.read_text()), f"CONTRIBUTING.md must install MotleyAI/living-architecture#v{pin}"


class TestParsingRules:
    def test_counts_slash_and_known_suffixes(self) -> None:
        assert _path_token("slayer/sql/") == "slayer/sql/"
        assert _path_token("zensical.toml") == "zensical.toml"
        assert _path_token("architecture/model.c4") == "architecture/model.c4"

    def test_ignores_plain_words(self) -> None:
        assert _path_token("pytest") is None
        assert _path_token("architecture") is None

    def test_skips_urls_placeholders_refs_and_absolute(self) -> None:
        for token in ("http://localhost:8000", "<dir>/x.py", "repo#v1/x", "likec4@1.47.0", "$(poetry", "/la:pr"):
            assert _path_token(token) is None, token

    def test_option_value_is_checked(self) -> None:
        assert _path_token("--ignore=tests/integration/x.py") == "tests/integration/x.py"
        assert _path_token("--timeout=120") is None
        assert _path_token("-m") is None

    def test_strips_surrounding_punctuation(self) -> None:
        assert _path_token("(docs/index.md),") == "docs/index.md"

    def test_glob_resolves_against_repo(self) -> None:
        assert _path_exists("architecture/*.arc42.md")
        assert not _path_exists("architecture/*.nope")

    def test_fences_match_marker_and_length(self) -> None:
        doc = "````md\n```\nin/a.py\n````\n~~~\nin/b.py\n~~~\n`in/c.py` out/d.py"
        assert _code_lines(doc) == ["```", "in/a.py", "in/b.py", "in/c.py"]

    def test_section_ignores_fenced_headings(self) -> None:
        doc = "## A\n```\n## Pull Requests\n```\n## Pull Requests\nreal\n```\n## B\n```\ntail\n## C\n"
        assert _section(doc, "Pull Requests").split() == ["real", "```", "##", "B", "```", "tail"]

    def test_comment_stripped_commands(self) -> None:
        block = "```bash\n# heading\npoetry run la-arch-diagrams  # when the model changed\n```"
        assert _fenced_commands(block) == ["poetry run la-arch-diagrams"]

    def test_shell_commands_split_on_operators_and_skip_comments(self) -> None:
        script = "# poetry run ghost\npoetry run a x && poetry run b \\\n  y; echo # poetry run c\n"
        assert _shell_poetry_commands(script) == ["poetry run a x", "poetry run b y"]

    def test_unit_test_selection_is_exact(self) -> None:
        assert _selects_unit_tests('poetry run pytest -m "not integration"')
        assert not _selects_unit_tests('poetry run pytest-x -m "not integration"')
        assert not _selects_unit_tests('poetry run pytest -m "not integration and slow"')
