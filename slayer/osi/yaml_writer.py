"""YAML output of an Ossie document that YAML 1.1 and YAML 1.2 core-schema loaders read back identically."""

import re
from typing import Any

import yaml

# YAML 1.2 core schema (§10.3.2) plain scalars that do not resolve to a string.
_YAML12_NON_STRING = re.compile(
    r"null|Null|NULL|~|true|True|TRUE|false|False|FALSE"
    r"|[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+"
    r"|[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?"
    r"|[-+]?\.(inf|Inf|INF)|\.(nan|NaN|NAN)"
)


class _Dumper(yaml.SafeDumper):
    """Quotes the YAML 1.2 non-strings; PyYAML already quotes what its YAML 1.1 resolver would retype."""


def _represent_str(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    style = "'" if _YAML12_NON_STRING.fullmatch(value) else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _represent_str)


def dump_yaml(document: dict[str, Any]) -> str:
    return yaml.dump(document, Dumper=_Dumper, sort_keys=False, allow_unicode=True)
