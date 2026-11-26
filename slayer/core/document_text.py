"""Reading a document's text as Ossie's loader does, and round-trip editing of it: only the nodes whose values change
are rewritten."""

from __future__ import annotations

import io
import json
import re
from collections.abc import Callable, MutableMapping, Sequence
from difflib import SequenceMatcher
from typing import Any

import yaml
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.util import load_yaml_guess_indent
from yaml.constructor import ConstructorError

from slayer.core.errors import UnreadableDocumentError

__all__ = ["edit_list_items", "edit_text", "parse_document_text", "plain", "without_vendor"]

_MERGE_TAG = "tag:yaml.org,2002:merge"
_VALUE_TAG = "tag:yaml.org,2002:value"
_IN_MAPPING = "while constructing a mapping"


class _Loader(yaml.SafeLoader):
    """``SafeLoader`` reporting a scalar it cannot build as a ``ConstructorError`` at its line, with YAML 1.1 merge
    precedence (as upstream Ossie's loader)."""

    def construct_object(self, node: yaml.Node, deep: bool = False) -> Any:
        try:
            return super().construct_object(node, deep)
        except (ValueError, AttributeError, LookupError) as exc:
            raise ConstructorError(None, None, f"cannot build {node.value!r} as {node.tag.rsplit(':', 1)[-1]}",
                                   node.start_mark) from exc

    def flatten_mapping(self, node: yaml.MappingNode) -> None:
        merge: list[tuple[yaml.Node, yaml.Node]] = []
        index = 0
        while index < len(node.value):
            key_node, value_node = node.value[index]
            if key_node.tag == _MERGE_TAG:
                del node.value[index]
                merge += self._merge_sources(node, value_node)
            else:
                if key_node.tag == _VALUE_TAG:
                    key_node.tag = "tag:yaml.org,2002:str"
                index += 1
        seen: set[int] = set()
        kept: list[tuple[yaml.Node, yaml.Node]] = []
        for pair in reversed(merge):  # one copy of each key node, so repeated aliases cannot grow the mapping
            if id(pair[0]) not in seen:
                seen.add(id(pair[0]))
                kept.append(pair)
        node.value = kept[::-1] + node.value

    def _merge_sources(self, node: yaml.MappingNode, value: yaml.Node) -> list[tuple[yaml.Node, yaml.Node]]:
        """The pairs ``<<: value`` merges, later sources first so earlier ones win."""
        if isinstance(value, yaml.MappingNode):
            self.flatten_mapping(value)
            return list(value.value)
        if not isinstance(value, yaml.SequenceNode):
            raise ConstructorError(_IN_MAPPING, node.start_mark,
                                   f"expected a mapping or list of mappings for merging, but found {value.tag}",
                                   value.start_mark)
        out: list[tuple[yaml.Node, yaml.Node]] = []
        for sub in reversed(value.value):
            if not isinstance(sub, yaml.MappingNode):
                raise ConstructorError(_IN_MAPPING, node.start_mark,
                                       f"expected a mapping for merging, but found {sub.tag}", sub.start_mark)
            self.flatten_mapping(sub)
            out += sub.value
        return out


def _line(node: yaml.Node) -> int:
    return node.start_mark.line + 1


def _built(loader: _Loader, node: yaml.ScalarNode, reasons: list[str]) -> tuple[bool, Any]:
    """``node`` built, or ``(False, None)`` with its reason recorded."""
    try:
        if node.tag == _VALUE_TAG:
            return True, loader.construct_scalar(node)
        return True, loader.construct_object(node)
    except ConstructorError as exc:
        reasons.append(f"line {_line(node)}: {exc.problem}")
        return False, None


def _problems(loader: _Loader, node: yaml.Node, *, reasons: list[str], visited: set[int]) -> None:
    """Every repeated explicit key and unbuildable scalar under ``node``; merge directives may repeat."""
    if id(node) in visited:
        return
    visited.add(id(node))
    if isinstance(node, yaml.ScalarNode):
        if node.tag != _MERGE_TAG:
            _built(loader, node, reasons)
        return
    if isinstance(node, yaml.SequenceNode):
        for child in node.value:
            _problems(loader, child, reasons=reasons, visited=visited)
        return
    seen: dict[Any, int] = {}
    for key, value in node.value:
        if key.tag != _MERGE_TAG:
            _key_problems(loader, key, seen=seen, reasons=reasons, visited=visited)
        _problems(loader, value, reasons=reasons, visited=visited)


def _key_problems(loader: _Loader, key: yaml.Node, *, seen: dict[Any, int], reasons: list[str],
                  visited: set[int]) -> None:
    """``key`` unbuildable, non-scalar or already in ``seen`` (its mapping's keys so far)."""
    if not isinstance(key, yaml.ScalarNode):
        reasons.append(f"line {_line(key)}: a mapping key must be a scalar")
        return
    visited.add(id(key))
    ok, built = _built(loader, key, reasons)
    if ok and built in seen:
        reasons.append(f"line {_line(key)}: duplicate key {built!r} (first at line {seen[built]})")
    elif ok:
        seen[built] = _line(key)


def _declared_name(root: yaml.Node) -> str | None:
    if not isinstance(root, yaml.MappingNode):
        return None
    return next((v.value for k, v in root.value
                 if isinstance(k, yaml.ScalarNode) and k.value == "name" and isinstance(v, yaml.ScalarNode)), None)


def parse_document_text(text: str) -> Any:
    """A document's YAML or JSON text as data, read as Ossie's loader reads it; raises ``UnreadableDocumentError``
    listing every repeated key and unbuildable scalar."""
    try:
        loader = _Loader(text)
    except yaml.YAMLError as exc:
        raise UnreadableDocumentError(reasons=[f"the document is not valid YAML or JSON: {exc}"]) from exc
    try:
        root = loader.get_single_node()
    except yaml.YAMLError as exc:
        raise UnreadableDocumentError(reasons=[f"the document is not valid YAML or JSON: {exc}"]) from exc
    if root is None:
        return None
    reasons: list[str] = []
    _problems(loader, root, reasons=reasons, visited=set())
    if not reasons:
        try:
            return loader.construct_document(root)
        except yaml.YAMLError as exc:
            reasons.append(f"the document is not valid YAML or JSON: {exc}")
    raise UnreadableDocumentError(reasons=reasons, name=_declared_name(root))


def without_vendor(node: Any, vendor: str) -> Any:
    """``node`` (plain data) with every ``nonportable_extensions`` entry of ``vendor`` removed, emptied lists too."""
    if isinstance(node, list):
        return [without_vendor(v, vendor) for v in node]
    if not isinstance(node, dict):
        return node
    out = {}
    for key, value in node.items():
        if key == "nonportable_extensions" and isinstance(value, list):
            value = [e for e in value if not (isinstance(e, dict) and str(e.get("vendor_name", "")).strip().upper()
                                              == vendor)]
            if not value:
                continue
        out[key] = without_vendor(value, vendor)
    return out

_JSON_INDENT = re.compile(r"\n([ \t]+)\S")


def plain(node: Any) -> Any:
    """``node`` (a round-trip tree) as plain dicts, lists and scalars."""
    if isinstance(node, MutableMapping):
        return {str(k): plain(v) for k, v in node.items()}
    if isinstance(node, list):
        return [plain(v) for v in node]
    if isinstance(node, bool) or node is None:
        return node
    for kind in (int, float, str):
        if isinstance(node, kind):
            return kind(node)
    return node


_ITEM_KEYS = ("name", "concept")


def _named(items: list[Any], key: str) -> bool:
    names = [i.get(key) for i in items if isinstance(i, MutableMapping)]
    return len(names) == len(items) and all(isinstance(n, str) for n in names) and len(set(names)) == len(names)


def _apply_named_list(node: list[Any], desired: list[Any], key: str) -> None:
    wanted = {d[key]: d for d in desired}
    for i in reversed(range(len(node))):
        if node[i][key] not in wanted:
            del node[i]
    held = {item[key]: item for item in node}
    for item in desired:
        if item[key] in held:
            _apply(held[item[key]], item)
        else:
            node.append(item)


def _apply_list(node: list[Any], desired: list[Any]) -> None:
    key = next((k for k in _ITEM_KEYS if node and desired and _named(node, k) and _named(desired, k)), None)
    if key is not None:
        _apply_named_list(node, desired, key)
        return
    kept = _subsequence(node, desired)
    if kept is not None:
        for i in reversed([i for i in range(len(node)) if i not in kept]):
            del node[i]
        return
    node[:] = desired


def _subsequence(node: list[Any], desired: list[Any]) -> set[int] | None:
    """Indices of ``node`` whose items, in order, equal ``desired``; ``None`` when ``desired`` is not a subsequence."""
    kept: set[int] = set()
    i = 0
    for want in desired:
        while i < len(node) and plain(node[i]) != want:
            i += 1
        if i == len(node):
            return None
        kept.add(i)
        i += 1
    return kept


def _apply(node: MutableMapping[str, Any], desired: dict[str, Any]) -> None:
    """Rewrite ``node`` in place to equal ``desired``, keeping every unchanged node (and its comments) as it is."""
    for key in [k for k in node if k not in desired]:
        del node[key]
    for key, value in desired.items():
        if key in node and plain(node[key]) == value:
            continue
        current = node.get(key)
        if isinstance(current, MutableMapping) and isinstance(value, dict):
            _apply(current, value)
        elif isinstance(current, list) and isinstance(value, list) and (isinstance(current, CommentedSeq) or value):
            _apply_list(current, value)
        else:
            node[key] = value


def _yaml(text: str) -> YAML:
    _, indent, offset = load_yaml_guess_indent(text)
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 1 << 16
    sequence = indent or 2
    yaml.indent(mapping=2, sequence=sequence, offset=offset if offset is not None else max(sequence - 2, 0))
    return yaml


def _is_json(text: str) -> bool:
    return text.lstrip().startswith("{")


class _Pins:
    """Where base lines fall in ``ours``, located through the base lines matched to it."""

    def __init__(self, *, base: list[str], ours: list[str]) -> None:
        self.to_ours: dict[int, int] = {}
        for i, j, n in SequenceMatcher(a=base, b=ours, autojunk=False).get_matching_blocks():
            self.to_ours.update({i + k: j + k for k in range(n)})
        self.base_len = len(base)
        self.ours_len = len(ours)

    def start_of(self, s: int) -> int | None:
        """Where base line ``s`` starts in ``ours``, when a pinned neighbour locates it."""
        if s in (0, self.base_len):
            return 0 if s == 0 else self.ours_len
        if s - 1 in self.to_ours:
            return self.to_ours[s - 1] + 1
        return self.to_ours.get(s)

    def end_of(self, lo: int, e: int) -> int | None:
        """Where base lines ``lo:e`` end in ``ours``, when a pinned line locates it."""
        if e > lo and e - 1 in self.to_ours:
            return self.to_ours[e - 1] + 1
        if e == lo:
            return self.start_of(lo)
        return self.ours_len if e == self.base_len else self.to_ours.get(e)


def _regions(edits: Sequence[tuple[str, int, int, int, int]], *, pins: _Pins) -> list[list[int]]:
    """The base line ranges the edits touch, widened to pinned bounds and merged when they meet."""
    regions: list[list[int]] = []
    for tag, i1, i2, _, _ in edits:
        if tag == "equal":
            continue
        while pins.start_of(i1) is None:
            i1 -= 1
        while pins.end_of(i1, i2) is None:
            i2 += 1
        if regions and i1 <= regions[-1][1]:
            regions[-1][1] = max(regions[-1][1], i2)
        else:
            regions.append([i1, i2])
    return regions


def _to_theirs(i: int, *, edits: Sequence[tuple[str, int, int, int, int]], end: bool) -> int:
    """Where base line ``i`` falls in ``theirs``; at a hunk boundary, after (``end``) or before an insertion."""
    hits = [j1 + (i - i1) if tag == "equal" else (j2 if i == i2 else j1) if end else (j1 if i == i1 else j2)
            for tag, i1, i2, j1, j2 in edits if i1 <= i <= i2]
    return hits[-1] if end else hits[0]


def _splice(*, ours: str, base: str, theirs: str) -> str:
    """``ours`` with the line hunks turning ``base`` into ``theirs``; ``base`` is ``ours`` re-emitted, so its
    formatting-only differences from ``ours`` outside the hunks keep ``ours``' text."""
    o, b, t = ours.splitlines(keepends=True), base.splitlines(keepends=True), theirs.splitlines(keepends=True)
    pins = _Pins(base=b, ours=o)
    edits = SequenceMatcher(a=b, b=t, autojunk=False).get_opcodes()
    out: list[str] = []
    cursor = 0
    for lo, hi in _regions(edits, pins=pins):
        start, end = pins.start_of(lo), pins.end_of(lo, hi)
        assert start is not None and end is not None
        out += o[cursor:start] + t[_to_theirs(lo, edits=edits, end=False):_to_theirs(hi, edits=edits, end=True)]
        cursor = end
    return "".join(out + o[cursor:])


def edit_text(text: str, edit: Callable[[dict[str, Any]], dict[str, Any]]) -> str:
    """``text`` (YAML or JSON) after ``edit`` maps its plain data to the desired data; unchanged parts keep their text.
    Raises ``UnreadableDocumentError`` when ``parse_document_text`` refuses ``text``."""
    parse_document_text(text)
    if _is_json(text):
        data = json.loads(text)
        found = _JSON_INDENT.search(text)
        indent = found.group(1) if found else None
        newline = "\n" if text.endswith("\n") else ""
        base = json.dumps(data, indent=indent, ensure_ascii=False) + newline
        _apply(data, edit(json.loads(text)))
        return _faithful(ours=text, base=base, theirs=json.dumps(data, indent=indent, ensure_ascii=False) + newline)
    yaml = _yaml(text)
    tree = yaml.load(text)
    base, theirs = io.StringIO(), io.StringIO()
    yaml.dump(tree, base)
    _apply(tree, edit(plain(tree)))
    yaml.dump(tree, theirs)
    return _faithful(ours=text, base=base.getvalue(), theirs=theirs.getvalue())


def _faithful(*, ours: str, base: str, theirs: str) -> str:
    """``_splice``'s text when it reads as ``theirs`` does, else ``theirs`` itself."""
    spliced = _splice(ours=ours, base=base, theirs=theirs)
    try:
        if parse_document_text(spliced) == parse_document_text(theirs):
            return spliced
    except UnreadableDocumentError:
        pass
    return theirs


def _item_spans(text: str, *, key: str) -> tuple[list[str], list[int], int, int] | None:
    """Block YAML ``text``'s lines, the first line of each item of its top-level block list ``key``, the end of the
    list and the column of its dashes; ``None`` for any other layout."""
    if _is_json(text):
        return None
    tree = _yaml(text).load(text)
    if not isinstance(tree, CommentedMap):
        return None
    seq = tree.get(key)
    if not isinstance(seq, CommentedSeq) or not seq or seq.fa.flow_style():
        return None
    lines = text.splitlines(keepends=True)
    starts: list[int] = []
    dashes: set[int] = set()
    for i in range(len(seq)):
        line, col = seq.lc.item(i)
        before = lines[line][:col].rstrip()
        if not before.endswith("-") or before[:-1].strip():
            return None
        starts.append(line)
        dashes.add(len(before) - 1)
    keys = list(tree)
    following = keys[keys.index(key) + 1:]
    end = tree.lc.key(following[0])[0] if following else len(lines)
    return (lines, starts, end, dashes.pop()) if len(dashes) == 1 else None


def _item_text(item_lines: list[str], *, dash: int, desired: dict[str, Any]) -> str:
    """One list item's text edited to ``desired``, its unchanged parts kept as written."""
    body = [line[dash + 2:] if i == 0 or not line[:dash + 2].strip() else line.lstrip(" ")
            for i, line in enumerate(item_lines)]
    edited = edit_text("".join(body), lambda _doc: desired)
    out = edited.splitlines(keepends=True)
    return "".join(f"{' ' * dash}{'- ' if i == 0 else '  '}{line}" if line.strip() else line
                   for i, line in enumerate(out))


def _new_item_text(desired: dict[str, Any], *, dash: int) -> str:
    dumped = yaml.safe_dump(desired, sort_keys=False, allow_unicode=True, width=1 << 16).splitlines(keepends=True)
    return "".join(f"{' ' * dash}{'- ' if i == 0 else '  '}{line}" for i, line in enumerate(dumped))


def edit_list_items(text: str, *, key: str, item_key: str, desired: list[dict[str, Any]]) -> str:
    """``text`` with its top-level list ``key`` rewritten to ``desired``, items matched by ``item_key``: an unchanged
    item keeps its text, a changed one is edited within its own span, a new one is written in the list's style."""
    spans = _item_spans(text, key=key)
    held = parse_document_text(text)
    if spans is None or not isinstance(held, dict):
        return edit_text(text, lambda doc: {**doc, key: desired})
    lines, starts, end, dash = spans
    items = held[key]
    chunks = {str(item.get(item_key)): (item, lines[start:stop]) for item, start, stop in
              zip(items, starts, [*starts[1:], end]) if isinstance(item, dict)}
    out: list[str] = []
    for item in desired:
        found = chunks.get(str(item.get(item_key)))
        if found is None:
            out.append(_new_item_text(item, dash=dash))
        elif found[0] == item:
            out.append("".join(found[1]))
        else:
            out.append(_item_text(found[1], dash=dash, desired=item))
    if out and not out[-1].endswith("\n"):
        out[-1] += "\n"
    result = "".join(lines[:starts[0]] + out + lines[end:])
    expected = {**held, key: desired}
    try:
        if parse_document_text(result) == expected:
            return result
    except UnreadableDocumentError:
        pass
    return edit_text(text, lambda doc: {**doc, key: desired})
