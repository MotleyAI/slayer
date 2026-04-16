"""Resolve dbt Jinja refs/sources in regular-model SQL bodies.

Used by :class:`slayer.dbt.converter.DbtToSlayerConverter` to inline
regular-model SQL into :attr:`slayer.core.models.SlayerModel.sql` when a
``semantic_model`` refers to a regular dbt model (a query) rather than a
physical source table. The resolver reads raw SQL from the project's
``.sql`` files (collected by :mod:`slayer.dbt.parser`) and produces SQL that
downstream :mod:`sqlglot` parsing can consume.

Threat model
------------
The input SQL comes from ``.sql`` files in a dbt project, which the developer
trusts. The resolver still treats captured identifiers carefully so that
ref/source arguments in a hostile or mistaken project cannot smuggle SQL
tokens into the output:

* Identifier captures use a strict ``\\w+`` pattern (letters, digits,
  underscore only). Anything outside that character class fails the regex.
* Anything inside ``{{ ... }}`` that doesn't match the strict patterns for
  ``ref()``, ``source()``, or ``config()`` is left in place verbatim and a
  warning is emitted — the resolver never silently interpolates an
  unrecognised Jinja construct into the SQL.
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# {{ config(...) }} — strip entirely. The argument list is always valid
# Python-like syntax but can span multiple lines; we match anything up to the
# closing braces that doesn't itself contain `}}`.
_CONFIG_RE = re.compile(
    r"\{\{\s*config\s*\([^}]*?\)\s*\}\}",
    re.DOTALL,
)

# {{ ref('name') }} / {{ ref('pkg', 'name') }} / versioned forms.
# ``\w+`` keeps captured identifiers injection-free.
_REF_RE = re.compile(
    r"\{\{\s*"
    r"ref\s*\(\s*"
    r"['\"](\w+)['\"]"                    # first positional arg (name or pkg)
    r"(?:\s*,\s*['\"](\w+)['\"])?"        # optional second positional arg (name when pkg given)
    r"\s*(?:,\s*\w+\s*=\s*[^)]+)?"        # optional trailing kwargs, e.g. v=1
    r"\s*\)\s*"
    r"\}\}"
)

# {{ source('schema', 'table') }} — strict two-arg form only.
_SOURCE_RE = re.compile(
    r"\{\{\s*"
    r"source\s*\(\s*"
    r"['\"](\w+)['\"]"                    # schema
    r"\s*,\s*"
    r"['\"](\w+)['\"]"                    # table
    r"\s*\)\s*"
    r"\}\}"
)

# Catches any remaining {{ ... }} block after the known patterns have been
# substituted. Used only to generate warnings for unrecognised Jinja;
# never for substitution.
_ANY_JINJA_RE = re.compile(r"\{\{[^}]*\}\}")


def resolve_refs(
    sql: str,
    regular_models_sql: Dict[str, str],
    *,
    max_depth: int = 16,
    _visited: Optional[Set[str]] = None,
) -> Tuple[str, List[str]]:
    """Resolve dbt Jinja refs/sources in a SQL body.

    Refs pointing at other regular models are recursively inlined as
    subqueries of the form ``(<inner sql>) AS <name>_ref_sub``; refs pointing
    at names that are not known regular models are treated as source tables
    and replaced with the bare identifier. ``{{ source(...) }}`` becomes
    ``schema.table``. ``{{ config(...) }}`` blocks are stripped.

    Args:
        sql: Raw SQL from a ``.sql`` file; may contain ``{{ ref() }}``,
            ``{{ source() }}``, ``{{ config() }}``.
        regular_models_sql: Map of regular-model name → raw SQL body. Used to
            recursively inline refs that point to other regular models.
        max_depth: Recursion cap for transitive ref resolution. Cycles are
            detected via ``_visited`` but the depth limit is a belt-and-
            braces guard against degenerate graphs.
        _visited: Internal — tracks the chain of regular-model names
            currently being resolved. Used to short-circuit cycles.

    Returns:
        ``(resolved_sql, warnings)`` — ``warnings`` is a list of human-readable
        strings describing unresolved Jinja blocks or cycles.
    """
    if _visited is None:
        _visited = set()

    warnings: List[str] = []

    if max_depth <= 0:
        warnings.append(
            f"resolve_refs: max recursion depth reached "
            f"(visited chain: {sorted(_visited)})"
        )
        return sql, warnings

    # 1. Strip {{ config(...) }} — pure metadata, no SQL.
    resolved = _CONFIG_RE.sub("", sql)

    # 2. Resolve {{ ref(...) }} calls.
    def replace_ref(match: "re.Match[str]") -> str:
        # Two-arg form: group(2) is the model name, group(1) is the package.
        # One-arg form: group(1) is the model name, group(2) is None.
        model_name = match.group(2) or match.group(1)
        if model_name in _visited:
            warnings.append(
                f"resolve_refs: cycle detected at ref('{model_name}') "
                f"(visited chain: {sorted(_visited)}); leaving bare name to break cycle"
            )
            return model_name
        if model_name in regular_models_sql:
            inner_sql, inner_warnings = resolve_refs(
                regular_models_sql[model_name],
                regular_models_sql,
                max_depth=max_depth - 1,
                _visited=_visited | {model_name},
            )
            warnings.extend(inner_warnings)
            return f"({inner_sql}) AS {model_name}_ref_sub"
        # Not a known regular model → treat as source table.
        return model_name

    resolved = _REF_RE.sub(replace_ref, resolved)

    # 3. Resolve {{ source('schema', 'table') }}.
    def replace_source(match: "re.Match[str]") -> str:
        schema, table = match.group(1), match.group(2)
        return f"{schema}.{table}"

    resolved = _SOURCE_RE.sub(replace_source, resolved)

    # 4. Anything left inside {{ ... }} is something we don't understand
    # (macros, vars, `this`, malformed refs, injection attempts). Emit a
    # warning for each occurrence but leave the token in place — downstream
    # sqlglot parsing will either reject it or the user will see the issue
    # surface in the first query.
    for leftover in _ANY_JINJA_RE.finditer(resolved):
        warnings.append(
            f"resolve_refs: unresolved Jinja block {leftover.group(0)!r} — "
            f"only ref(), source(), and config() are understood"
        )

    return resolved, warnings
