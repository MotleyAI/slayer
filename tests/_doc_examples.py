"""Extract the query examples and demo references from documentation pages, and check them on the demo."""

from __future__ import annotations

import ast
import json
import re
import shlex
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from slayer.async_utils import run_sync
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.resolver import resolve_entity
from slayer.storage.base import StorageBackend

REPO_ROOT = Path(__file__).resolve().parent.parent

IN_SCOPE_GLOBS = (
    "README.md",
    "docs/index.md",
    "docs/getting-started/*.md",
    "docs/examples/*.md",
    "docs/examples/*/*.md",
    "docs/reference/*.md",
)

JSON_LANGS = {"json", "jsonc", "json5"}
PYTHON_LANGS = {"python", "py", "python3", "pycon"}
SHELL_LANGS = {"bash", "sh", "shell", "zsh", "console", "shell-session"}

MARKER_RE = re.compile(r'^!!!\s+note\s+"Illustrative schema"\s*$')
NOT_A_QUERY_RE = re.compile(r"^<!--\s*doc-check:\s*not-a-query\s*-->$")
FENCE_RE = re.compile(r"^(?P<indent>[ \t]*)(?P<fence>`{3,}|~{3,})(?P<info>[^`]*)$")
SOURCE_MODEL_RE = re.compile(r"\bsource_model\b")
REFERENCE_RE = re.compile(r"(?<![\w/.\-])jaffle_shop(?:\.[A-Za-z_]\w*)+")

_UNPARSED = object()


class DocQuery(BaseModel):
    line: int
    query: Any


class DocBlock(BaseModel):
    line: int
    lang: str
    marked: bool
    not_a_query: bool
    source_model_mentions: int
    queries: list[DocQuery]

    def has_unextracted_mention(self) -> bool:
        return self.source_model_mentions > sum(_query_key_count(q.query) for q in self.queries)


class DocReference(BaseModel):
    line: int
    reference: str


class DocPage(BaseModel):
    path: str
    blocks: list[DocBlock]
    references: list[DocReference]

    def checked_queries(self) -> list[DocQuery]:
        return [q for b in self.blocks if not b.marked for q in b.queries]

    def unextracted_blocks(self) -> list[DocBlock]:
        return [b for b in self.blocks if b.has_unextracted_mention() and not b.marked and not b.not_a_query]


def in_scope_pages() -> list[str]:
    return sorted({str(p.relative_to(REPO_ROOT)) for g in IN_SCOPE_GLOBS for p in REPO_ROOT.glob(g)})


def load_page(path: str) -> DocPage:
    return parse_page(path=path, text=(REPO_ROOT / path).read_text(encoding="utf-8"))


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def _preceding_line(lines: list[str], fence_index: int, *, skip_indented: bool) -> str:
    """The last non-blank line before the fence at the fence's indent, or "" if there is none."""
    fence_indent = _indent(lines[fence_index])
    j = fence_index - 1
    # An admonition's indented body sits between the marker and the fence.
    while j >= 0 and (not lines[j].strip() or (skip_indented and _indent(lines[j]) > fence_indent)):
        j -= 1
    return lines[j].strip() if j >= 0 and _indent(lines[j]) == fence_indent else ""


def parse_page(*, path: str, text: str) -> DocPage:
    lines = text.splitlines()
    blocks: list[DocBlock] = []
    i = 0
    while i < len(lines):
        m = FENCE_RE.match(lines[i])
        if m is None:
            i += 1
            continue
        fence, indent = m["fence"], len(m["indent"])
        info = m["info"].strip().split()
        lang = info[0].strip("{}.").lower() if info else ""
        close = re.compile(rf"^[ \t]*{re.escape(fence[0])}{{{len(fence)},}}\s*$")
        j = i + 1
        while j < len(lines) and not close.match(lines[j]):
            j += 1
        body = "\n".join(line[indent:] if not line[:indent].strip() else line.lstrip() for line in lines[i + 1:j])
        blocks.append(DocBlock(
            line=i + 1,
            lang=lang,
            marked=bool(MARKER_RE.match(_preceding_line(lines, i, skip_indented=True))),
            not_a_query=bool(NOT_A_QUERY_RE.match(_preceding_line(lines, i, skip_indented=False))),
            source_model_mentions=len(SOURCE_MODEL_RE.findall(body)),
            queries=[DocQuery(line=i + 2 + rel, query=q) for rel, q in extract_queries(lang=lang, body=body)],
        ))
        i = j + 1
    references = [
        DocReference(line=n, reference=m.group(0))
        for n, line in enumerate(lines, start=1)
        for m in REFERENCE_RE.finditer(line)
        if not m.group(0).endswith(".duckdb")
    ]
    return DocPage(path=path, blocks=blocks, references=references)


# --- query extraction ---------------------------------------------------------


def _is_query(value: Any) -> bool:
    return isinstance(value, dict) and "source_model" in value


def _query_key_count(value: Any) -> int:
    """Number of ``source_model`` keys anywhere in ``value``."""
    if isinstance(value, dict):
        return ("source_model" in value) + sum(_query_key_count(v) for v in value.values())
    if isinstance(value, list):
        return sum(_query_key_count(v) for v in value)
    return 0


def _queries_in(value: Any) -> Iterator[Any]:
    """Queries (top-level ``source_model`` objects, or non-empty lists of them) inside a parsed literal."""
    if _is_query(value):
        yield value
    elif isinstance(value, list) and value and all(_is_query(v) for v in value):
        yield value
    elif isinstance(value, (dict, list)):
        for child in value.values() if isinstance(value, dict) else value:
            yield from _queries_in(child)


class _LineCursor:
    """Assigns each query the body line of its first ``source_model``, scanning forward."""

    def __init__(self, body: str) -> None:
        self.body = body
        self.pos = 0

    def line_of(self, query: Any, *, start: int = 0) -> int:
        self.pos = max(self.pos, start)
        first = SOURCE_MODEL_RE.search(self.body, self.pos)
        if first is None:
            return self.body.count("\n", 0, start)
        line = self.body.count("\n", 0, first.start())
        self.pos = first.end()
        for _ in range(repr(query).count("source_model") - 1):
            nxt = SOURCE_MODEL_RE.search(self.body, self.pos)
            if nxt is None:
                break
            self.pos = nxt.end()
        return line


def _balanced_end(text: str, start: int) -> int | None:
    """End offset of the bracket span opening at ``start``; double-quoted strings are skipped."""
    depth = 0
    in_string = False
    i = start
    while i < len(text):
        ch = text[i]
        if in_string:
            if ch == "\\":
                i += 1
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "{[":
            depth += 1
        elif ch in "}]":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return None


def _parse_literal(text: str, *, strict: bool) -> Any:
    """JSON, else (unless ``strict``) a Python literal, else ``_UNPARSED``."""
    try:
        return json.loads(text)
    except ValueError:
        if strict:
            return _UNPARSED
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        return _UNPARSED


def _scan_spans(text: str, *, strict: bool = False) -> list[tuple[int, Any]]:
    """``(offset, value)`` of every bracket span that parses as JSON or (unless ``strict``) a Python literal."""
    spans: list[tuple[int, Any]] = []
    i = 0
    while i < len(text):
        if text[i] in "{[":
            end = _balanced_end(text, i)
            if end is not None:
                value = _parse_literal(text[i:end], strict=strict)
                if value is not _UNPARSED:
                    spans.append((i, value))
                    i = end
                    continue
        i += 1
    return spans


def _callee_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _python_literals(body: str) -> list[tuple[int, Any]]:
    tree = ast.parse(body)
    starts = [0]
    for line in body.splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    found: list[tuple[int, Any]] = []

    def offset(node: ast.expr) -> int:
        return starts[node.lineno - 1]

    def visit(node: ast.AST) -> None:
        if isinstance(node, ast.Call) and _callee_name(node.func) == "SlayerQuery" and not node.args:
            try:
                kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords if kw.arg is not None}
            except (ValueError, SyntaxError, TypeError):
                pass
            else:
                found.append((offset(node), kwargs))
                return
        if isinstance(node, (ast.Dict, ast.List)):
            try:
                value = ast.literal_eval(node)
            except (ValueError, SyntaxError, TypeError):
                pass
            else:
                found.append((offset(node), value))
                return
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(tree)
    return found


def _shell_literals(body: str) -> list[tuple[int, Any]] | None:
    """Literals inside shell-unquoted arguments; offsets are unknown (0), the cursor orders them."""
    try:
        tokens = shlex.split(body, comments=True)
    except ValueError:
        return None
    return [(0, value) for token in tokens for _, value in _scan_spans(token)]


def _literals(*, lang: str, body: str) -> list[tuple[int, Any]]:
    # A ```json block is copied as JSON, so Python-literal syntax there is not a runnable query.
    strict = lang == "json"
    if lang in JSON_LANGS:
        whole = _parse_literal(body, strict=strict)
        if whole is not _UNPARSED:
            return [(0, whole)]
    elif lang in PYTHON_LANGS:
        try:
            return _python_literals(body)
        except SyntaxError:
            pass
    elif lang in SHELL_LANGS:
        shell = _shell_literals(body)
        if shell is not None:
            return shell
    return _scan_spans(body, strict=strict)


def extract_queries(*, lang: str, body: str) -> list[tuple[int, Any]]:
    """``(body_line, query)`` for every query literal in a code block body (0-based lines)."""
    cursor = _LineCursor(body)
    return [
        (cursor.line_of(query, start=offset), query)
        for offset, value in _literals(lang=lang, body=body)
        for query in _queries_in(value)
    ]


# --- checks -------------------------------------------------------------------


def _first_line(exc: BaseException) -> str:
    lines = str(exc).strip().splitlines()
    first = lines[0] if lines else ""
    name = type(exc).__name__
    return first if first.startswith(name) else f"{name}: {first}"


def query_failure(*, storage: StorageBackend, query: Any) -> str | None:
    """Why ``query`` fails on the demo (error, or no rows); ``None`` when it returns rows."""
    try:
        rows = SlayerQueryEngine(storage=storage).execute_sync(query).data
    except Exception as exc:  # noqa: BLE001 — any failure is reported with its location
        return _first_line(exc)
    return None if rows else "returned no rows"


def reference_failure(*, storage: StorageBackend, reference: str) -> str | None:
    """Why ``reference`` does not resolve as ``slayer inspect`` resolves it; ``None`` when it does."""
    try:
        run_sync(resolve_entity(reference, storage=storage))
    except Exception as exc:  # noqa: BLE001
        return _first_line(exc)
    return None


def page_failures(*, page: DocPage, storage: StorageBackend) -> list[str]:
    """Every ``page:line: reason`` failure of the doc-example check on ``page``."""
    failures = [
        f"{page.path}:{b.line}: code block mentions source_model outside any extracted query"
        for b in page.unextracted_blocks()
    ]
    for q in page.checked_queries():
        reason = query_failure(storage=storage, query=q.query)
        if reason is not None:
            failures.append(f"{page.path}:{q.line}: {reason}")
    for r in page.references:
        reason = reference_failure(storage=storage, reference=r.reference)
        if reason is not None:
            failures.append(f"{page.path}:{r.line}: {r.reference}: {reason}")
    return failures
