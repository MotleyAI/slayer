"""Round-trip editing of a stored document's text: only the nodes whose values change are rewritten."""

from __future__ import annotations

import io
import json
import re
from collections.abc import Callable, MutableMapping
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedSeq
from ruamel.yaml.util import load_yaml_guess_indent

__all__ = ["edit_text", "plain", "without_vendor"]


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


def _named(items: list[Any]) -> bool:
    names = [i.get("name") for i in items if isinstance(i, MutableMapping)]
    return len(names) == len(items) and all(isinstance(n, str) for n in names) and len(set(names)) == len(names)


def _apply_list(node: list[Any], desired: list[Any]) -> None:
    if node and desired and _named(node) and _named(desired):
        wanted = {d["name"]: d for d in desired}
        for i in reversed(range(len(node))):
            if node[i]["name"] not in wanted:
                del node[i]
        held = {item["name"]: item for item in node}
        for item in desired:
            if item["name"] in held:
                _apply(held[item["name"]], item)
            else:
                node.append(item)
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


def edit_text(text: str, edit: Callable[[dict[str, Any]], dict[str, Any]]) -> str:
    """``text`` (YAML or JSON) after ``edit`` maps its plain data to the desired data; unchanged parts keep their text."""
    if _is_json(text):
        data = json.loads(text)
        _apply(data, edit(json.loads(text)))
        found = _JSON_INDENT.search(text)
        indent = found.group(1) if found else None
        out = json.dumps(data, indent=indent, ensure_ascii=False)
        return out + "\n" if text.endswith("\n") else out
    yaml = _yaml(text)
    tree = yaml.load(text)
    _apply(tree, edit(plain(tree)))
    buffer = io.StringIO()
    yaml.dump(tree, buffer)
    return buffer.getvalue()
