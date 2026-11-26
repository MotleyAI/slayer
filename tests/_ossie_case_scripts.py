"""The one reader of upstream's wide-table case scripts (``compliance/wide_table/<id>.ost``), failing closed."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

from pydantic import BaseModel

from tests._dev2217_fixtures import DATA, WIDE_TABLE

Cell = Decimal | str | None

_RAW_OPEN = re.compile(r"\{([A-Za-z0-9_]*)\|")
_RAW_LINE = re.compile(r">[ \t]*(?=\{[A-Za-z0-9_]*\|)")
_RAW_TYPE = re.compile(r"[ \t]*:[ \t]*\w+[ \t]*")
_COMMAND = re.compile(r"([A-Za-z_]\w*)[ \t]*\((.*)\)", re.DOTALL)
_ERROR = re.compile(r"> error: (E_[A-Z0-9_]+)(?::.*)?")
_BORDER = re.compile(r"\+(?:-+\+)+")
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")
_OUTPUT = (">", "+", "|")
#: ``set`` and ``skip`` are read and ignored: upstream's ``skip`` marks a gap of its own runner.
_COMMANDS = frozenset({"connect", "set", "modelFile", "skip", "executeLower"})


class CaseScriptError(ValueError):
    """A case script the reader cannot read."""


class WideCase(BaseModel):
    """A wide-table case: its data, model, query, and expected rows or error code."""

    name: str
    data: str
    data_path: Path
    model: str
    model_path: Path
    query: dict[str, Any]
    header: list[str] = []
    rows: list[list[Cell]] = []
    error: str | None = None


class _Output(BaseModel):
    kind: str  # "raw", "error", "table" or "other"
    line: int
    lines: list[str] = []


class _Script:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.text = path.read_text(encoding="utf-8")

    def line(self, pos: int) -> int:
        return self.text.count("\n", 0, pos) + 1

    def fail(self, message: str, line: int | None = None) -> NoReturn:
        where = f"{self.path}:{line}" if line is not None else str(self.path)
        raise CaseScriptError(f"{where}: {message}")

    def line_end(self, pos: int) -> int:
        end = self.text.find("\n", pos)
        return len(self.text) if end == -1 else end


def _atom_end(text: str, i: int) -> int | None:
    """The end of the string, raw string or comment starting at ``i``: None when none starts there, -1 if open."""
    char = text[i]
    if char in "\"'":
        j = i + 1
        while True:
            k = text.find(char, j)
            if k == -1:
                return -1
            if text.startswith(char, k + 1):
                j = k + 2
                continue
            return k + 1
    if text.startswith("/*", i):
        k = text.find("*/", i + 2)
        return -1 if k == -1 else k + 2
    if text.startswith("--", i):
        k = text.find("\n", i)
        return len(text) if k == -1 else k
    raw = _RAW_OPEN.match(text, i)
    if raw:
        close = f"|{raw.group(1)}}}"
        k = text.find(close, raw.end())
        return -1 if k == -1 else k + len(close)
    return None


def _scan_command(script: _Script, start: int) -> tuple[int, str]:
    """The end of the command at ``start`` (its line's end) and its text without comments or the ``;``."""
    text, line = script.text, script.line(start)
    out: list[str] = []
    depth, i = 0, start
    while i < len(text):
        end = _atom_end(text, i)
        if end == -1:
            script.fail("unterminated string, raw string or comment", line)
        if end is not None:
            out.append(" " if text[i] in "-/" else text[i:end])
            i = end
            continue
        char = text[i]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                script.fail(f"unmatched {char!r}", line)
        elif char == ";" and depth == 0:
            line_end = script.line_end(i)
            if not text[i + 1:line_end].strip():
                return line_end, "".join(out)
        out.append(char)
        i += 1
    script.fail("unterminated command", line)


def _split_args(script: _Script, args: str, line: int) -> list[str]:
    """Top-level comma-separated arguments; an unbalanced bracket fails."""
    out: list[str] = []
    depth, start, i = 0, 0, 0
    while i < len(args):
        end = _atom_end(args, i)
        if end == -1:
            script.fail("unterminated string", line)
        if end is not None:
            i = end
            continue
        char = args[i]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                script.fail(f"unmatched {char!r}", line)
        elif char == "," and depth == 0:
            out.append(args[start:i].strip())
            start = i + 1
        i += 1
    if depth:
        script.fail("unbalanced brackets", line)
    last = args[start:].strip()
    return [*out, last] if out or last else out


def _string(script: _Script, arg: str, line: int) -> str:
    quote = arg[:1]
    if quote not in ("'", '"') or len(arg) < 2 or arg[-1] != quote or quote in arg[1:-1].replace(quote * 2, ""):
        script.fail(f"expected a string, got {arg!r}", line)
    return arg[1:-1].replace(quote * 2, quote)


def _strings(script: _Script, args: list[str], count: int, name: str, line: int) -> list[str]:
    if len(args) != count:
        script.fail(f"{name} takes {count} argument(s), got {len(args)}", line)
    return [_string(script, a, line) for a in args]


def _read_outputs(script: _Script, pos: int) -> tuple[int, list[_Output]]:
    """The output lines directly after a command, from ``pos`` (a line start)."""
    text, outputs = script.text, []
    while pos < len(text) and text.startswith(_OUTPUT, pos):
        line, end = script.line(pos), script.line_end(pos)
        raw = _RAW_LINE.match(text, pos)
        if raw:
            close = _atom_end(text, raw.end())
            if close is None or close == -1:
                script.fail("unterminated raw string", line)
            end = script.line_end(close)
            if not _RAW_TYPE.fullmatch(text, close, end):
                script.fail("expected ': <type>' after the raw string", script.line(close))
            outputs.append(_Output(kind="raw", line=line))
        elif text.startswith(("+", "|"), pos):
            if outputs and outputs[-1].kind == "table":
                outputs[-1].lines.append(text[pos:end].rstrip())
            else:
                outputs.append(_Output(kind="table", line=line, lines=[text[pos:end].rstrip()]))
        else:
            kind = "error" if text.startswith("> error:", pos) else "other"
            outputs.append(_Output(kind=kind, line=line, lines=[text[pos:end].rstrip()]))
        pos = end + 1
    return pos, outputs


def _cells(script: _Script, text: str, line: int) -> list[str]:
    """A table line's cells, stripped; a quoted cell may hold ``|``."""
    if len(text) < 2 or not (text.startswith("|") and text.endswith("|")):
        script.fail("malformed table row", line)
    inner, cells, current, i = text[1:-1], [], "", 0
    while i < len(inner):
        char = inner[i]
        if char == '"' and not current.strip():
            j = i + 1
            while j < len(inner) and inner[j] != '"':
                j += 2 if inner[j] == "\\" else 1
            if j >= len(inner):
                script.fail("unterminated quoted cell", line)
            current += inner[i:j + 1]
            i = j + 1
            continue
        if char == "|":
            cells.append(current.strip())
            current = ""
        else:
            current += char
        i += 1
    return [*cells, current.strip()]


def _quoted(script: _Script, cell: str, line: int) -> str:
    try:
        value = json.loads(cell)
    except json.JSONDecodeError:
        value = None
    if not isinstance(value, str):
        script.fail(f"malformed quoted cell {cell}", line)
    return value


def _cell(script: _Script, cell: str, line: int) -> Cell:
    """Empty is NULL, a quoted cell its JSON text, a number its exact value, anything else the text."""
    if cell == "":
        return None
    if cell.startswith('"'):
        return _quoted(script, cell, line)
    return Decimal(cell) if _NUMBER.fullmatch(cell) else cell


def _table(script: _Script, table: _Output) -> tuple[list[str], list[list[Cell]]]:
    lines = [(table.line + k, text) for k, text in enumerate(table.lines)]
    if len(lines) < 4:
        script.fail("a table needs a border, a header, a border, rows and a border", lines[-1][0])
    for at in (0, 2, -1):
        line, text = lines[at]
        if not _BORDER.fullmatch(text) or text != lines[0][1]:
            script.fail(f"malformed table border {text!r}", line)
    line, text = lines[1]
    header = [_quoted(script, c, line) if c.startswith('"') else c for c in _cells(script, text, line)]
    if len(header) != lines[0][1].count("+") - 1:
        script.fail("the header does not match the border", line)
    rows: list[list[Cell]] = []
    for line, text in lines[3:-1]:
        cells = _cells(script, text, line)
        if len(cells) != len(header):
            script.fail(f"a row of {len(cells)} cells under a header of {len(header)}", line)
        rows.append([_cell(script, c, line) for c in cells])
    return header, rows


def _expected(script: _Script, outputs: list[_Output], line: int) -> dict[str, Any]:
    """``executeLower``'s output: one error line, or a raw-string lowering then a table."""
    if not outputs:
        script.fail("executeLower has no expected output", line)
    first = outputs[0]
    if first.kind == "error":
        match = _ERROR.fullmatch(first.lines[0])
        if not match:
            script.fail(f"an error line must start with an E_ code: {first.lines[0]!r}", first.line)
        if len(outputs) > 1:
            script.fail("an error case has no further output", outputs[1].line)
        return {"error": match.group(1)}
    if first.kind != "raw":
        if first.kind == "table":
            script.fail("expected a lowering before the table", line)
        script.fail(f"unexpected output {first.lines[0]!r}", first.line)
    if len(outputs) == 1:
        script.fail("expected a table after the lowering", line)
    if outputs[1].kind != "table":
        script.fail("expected a table after the lowering", outputs[1].line)
    if len(outputs) > 2:
        script.fail("unexpected output after the table", outputs[2].line)
    header, rows = _table(script, outputs[1])
    return {"header": header, "rows": rows}


def read_case(path: Path) -> WideCase:
    """The case ``path`` holds; anything the reader does not recognise raises ``CaseScriptError``."""
    script = _Script(path)
    text, pos = script.text, 0
    data: str | None = None
    models: dict[str, Path] = {}
    case: dict[str, Any] | None = None
    while pos < len(text):
        end = script.line_end(pos)
        stripped = text[pos:end].strip()
        if not stripped or stripped.startswith("#"):
            pos = end + 1
            continue
        line = script.line(pos)
        if stripped.startswith(_OUTPUT):
            script.fail("output does not follow a command", line)
        end, command = _scan_command(script, pos + len(text[pos:end]) - len(text[pos:end].lstrip()))
        parsed = _COMMAND.fullmatch(command.strip())
        if not parsed:
            script.fail(f"not a command: {command.strip()!r}", line)
        name = parsed.group(1)
        if name not in _COMMANDS:
            script.fail(f"unknown command {name}", line)
        args = _split_args(script, parsed.group(2), line)
        pos, outputs = _read_outputs(script, end + 1)
        if outputs and name != "executeLower":
            script.fail(f"{name} has no output", outputs[0].line)
        if name == "connect":
            (connected,) = _strings(script, args, 1, name, line)
            if data is not None or connected not in DATA:
                script.fail(f"unknown or repeated connect({connected!r}); known: {sorted(DATA)}", line)
            data = connected
        elif name == "modelFile":
            model, model_path = _strings(script, args, 2, name, line)
            if model in models:
                script.fail(f"model {model!r} loaded twice", line)
            models[model] = path.parent / model_path
        elif name == "executeLower":
            if case is not None:
                script.fail("a second executeLower", line)
            if len(args) != 2:
                script.fail("executeLower takes a model and a query", line)
            model = _string(script, args[0], line)
            if model not in models:
                script.fail(f"executeLower names model {model!r}, not one modelFile loads", line)
            try:
                query = json.loads(args[1])
            except json.JSONDecodeError as error:
                script.fail(f"the query is not JSON: {error}", line)
            if not isinstance(query, dict):
                script.fail("the query is not a JSON object", line)
            case = {"model": model, "model_path": models[model], "query": query,
                    **_expected(script, outputs, line)}
    if case is None:
        script.fail("no executeLower")
    if data is None:
        script.fail("no connect")
    return WideCase(name=path.stem, data=data, data_path=WIDE_TABLE / DATA[data], **case)


def cases() -> list[WideCase]:
    return [read_case(p) for p in sorted(WIDE_TABLE.glob("*.ost"))]


def row_cases() -> list[WideCase]:
    return [c for c in cases() if c.name.startswith("a")]
