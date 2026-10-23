"""The portable JSON path subset: ``$`` then ``.key``, ``."quoted key"`` and ``[n]`` steps."""

from __future__ import annotations

import re
from functools import lru_cache

from pydantic import BaseModel, ConfigDict

_KEY_RE = re.compile(r"[A-Za-z_]\w*", re.ASCII)
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_MODE_WORDS = ("lax", "strict")


class JsonPathError(ValueError):
    """A path outside the portable subset; ``construct`` names the offending part."""

    def __init__(self, construct: str) -> None:
        self.construct = construct
        super().__init__(f"{construct} is not in the portable JSON path subset")


class JsonPath(BaseModel):
    """Object keys (``str``) and array indices (``int``), outermost first."""

    model_config = ConfigDict(frozen=True)

    steps: tuple[str | int, ...]

    @property
    def text(self) -> str:
        """The canonical path text."""
        return json_path_text(self.steps)


def json_path_text(steps: tuple[str | int, ...]) -> str:
    """``$`` plus each step: ``.key`` for a plain key, ``."key"`` for any other, ``[n]`` for an index."""
    out = ["$"]
    for step in steps:
        if isinstance(step, int):
            out.append(f"[{step}]")
        elif _KEY_RE.fullmatch(step):
            out.append(f".{step}")
        else:
            out.append(f'."{step}"')
    return "".join(out)


def check_key(key: str) -> str:
    """``key`` when the subset can spell it (no double quote, backslash or control character)."""
    if '"' in key or "\\" in key:
        raise JsonPathError(f"the key {key!r} with a double quote or backslash")
    if _CONTROL_RE.search(key):
        raise JsonPathError(f"the key {key!r} with a control character")
    return key


class _Reader:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def peek(self) -> str:
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def steps(self) -> tuple[str | int, ...]:
        out: list = []
        while self.pos < len(self.text):
            ch = self.peek()
            if ch == ".":
                out.append(self.member())
            elif ch == "[":
                out.append(self.index())
            else:
                raise JsonPathError(f"the text {self.text[self.pos:]!r} after a step")
        return tuple(out)

    def member(self) -> str:
        self.pos += 1
        ch = self.peek()
        if ch == ".":
            raise JsonPathError("the recursive descent '..'")
        if ch == "*":
            raise JsonPathError("the wildcard '*'")
        if ch == '"':
            end = self.text.find('"', self.pos + 1)
            if end < 0:
                raise JsonPathError('the unterminated quote \'"\' of a quoted key')
            key = self.text[self.pos + 1:end]
            self.pos = end + 1
            return check_key(key)
        match = _KEY_RE.match(self.text, self.pos)
        if match is None:
            raise JsonPathError(f"the member step '.{self.text[self.pos:self.pos + 8]}' (quote the key: .\"…\")")
        self.pos = match.end()
        return match.group(0)

    def index(self) -> int:
        end = self.text.find("]", self.pos)
        body = self.text[self.pos + 1:end] if end > 0 else self.text[self.pos + 1:]
        stripped = body.strip()
        if stripped == "*":
            raise JsonPathError("the wildcard '[*]'")
        if stripped.startswith("?"):
            raise JsonPathError("the filter expression '[?…]'")
        if stripped.startswith(("'", '"')):
            raise JsonPathError("the bracket key '[...]' (write .key or .\"key\")")
        if stripped.startswith("-") and stripped[1:].isdigit():
            raise JsonPathError(f"the negative index '[{stripped}]'")
        if end < 0 or not stripped.isdigit():
            raise JsonPathError(f"the array index '[{body}]' (an index is a non-negative integer)")
        self.pos = end + 1
        return int(stripped)


@lru_cache(maxsize=4096)
def parse_json_path(text: str) -> JsonPath:
    """``text`` as a portable path; ``JsonPathError`` names the first construct outside the subset."""
    head = text.strip().split(" ", 1)[0].lower() if text.strip() else ""
    if head in _MODE_WORDS:
        raise JsonPathError(f"the path mode word '{head}'")
    if not text.startswith("$"):
        raise JsonPathError("a path not starting with '$'")
    return JsonPath(steps=_Reader(text[1:]).steps())


__all__ = ["JsonPath", "JsonPathError", "check_key", "json_path_text", "parse_json_path"]
