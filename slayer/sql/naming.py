"""Collision-safe alias allocator and single owner of every alias / result-key decision."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal, Optional, Tuple

import sqlglot
from pydantic import BaseModel, ConfigDict, PrivateAttr
from sqlglot import exp

from slayer.core.errors import IdentifierCollisionError
from slayer.core.keys import StarKey
from slayer.core.refs import (
    agg_kwarg_canonical_str,
    canonical_agg_name,
    partition_by_suffix,
)
from slayer.sql._identifier_fit import fit_identifier
from slayer.sql.naming_bijection import (  # noqa: F401
    _ALIAS_SEP,
    decode_alias,
    encode_alias,
)

if TYPE_CHECKING:  # pragma: no cover — typing only, keeps the import leaf clean
    from slayer.core.keys import AggregateKey

# Minted names are unquoted, so these dialects collide names differing only in case;
# fold key is ``str.lower()`` (``casefold`` over-equates ``ß``→``ss``).
CASE_FOLDING_SQLGLOT_DIALECTS: frozenset[str] = frozenset({
    "postgres", "redshift", "snowflake", "oracle", "mysql", "tsql",
    "sqlite", "duckdb", "trino", "presto", "databricks", "spark", "bigquery",
})

KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS: frozenset[str] = frozenset({"clickhouse"})


def dialect_folds_case(dialect: str) -> bool:
    return dialect.strip().lower() in CASE_FOLDING_SQLGLOT_DIALECTS


class AliasAllocator(BaseModel):
    """Per-generation collision-safe name allocator; ``folds_case`` makes ``shifted_Foo`` block ``shifted_foo``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    folds_case: bool = False

    # External names to avoid, stored folded when folds_case.
    _reserved: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    _used: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    # Monotonic (never reset per scope) so sibling scopes can't mint the same ``_val_0``.
    _val_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)
    _scope_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)
    # Join-alias registry memoized by (root, path); keyed by root so scopes are independent.
    _join_alias_memo: dict[Tuple[str, Tuple[str, ...]], str] = PrivateAttr(default_factory=dict)  # NOSONAR(S5890)
    _join_alias_used: dict[str, set[str]] = PrivateAttr(default_factory=dict)  # NOSONAR(S5890)

    def _fold(self, name: str) -> str:
        return name.lower() if self.folds_case else name

    def reserve(self, *names: str) -> None:
        self._reserved.update(self._fold(n) for n in names)

    def _taken(self, name: str) -> bool:
        key = self._fold(name)
        return key in self._reserved or key in self._used

    def allocate(self, preferred: str) -> str:
        candidate = preferred
        suffix = 2
        while self._taken(candidate):
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        self._used.add(self._fold(candidate))
        return candidate

    def allocate_val(self) -> str:
        while True:
            candidate = f"_val_{self._val_seq}"
            self._val_seq += 1
            if not self._taken(candidate):
                self._used.add(self._fold(candidate))
                return candidate

    def allocate_cte(self, preferred: str) -> str:
        return self.allocate(preferred)

    def alias_for(
        self, *, root: str, path: Tuple[str, ...], limit: Optional[int] = None,
    ) -> str:
        """JOIN alias for cumulative ``path`` under ``root``, memoized so a chain leaf
        (``("a","b")``→``a__b``) stays distinct from a model literally named ``a__b``."""
        if not path:
            return root
        key = (root, path)
        cached = self._join_alias_memo.get(key)
        if cached is not None:
            return cached
        if len(path) == 1:
            base = path[0]
        else:
            parent = self.alias_for(root=root, path=path[:-1], limit=limit)
            base = f"{parent}__{path[-1]}"
        used = self._join_alias_used.setdefault(root, set())
        candidate = fit_identifier(name=base, limit=limit)
        suffix = 2
        while self._fold(candidate) in used:
            candidate = fit_identifier(name=f"{base}_{suffix}", limit=limit)
            suffix += 1
        used.add(self._fold(candidate))
        self._join_alias_memo[key] = candidate
        return candidate

    def next_scope_id(self, root_relation: str) -> str:
        """A generation-local ``ScopeFrame`` id ``<root>#<seq>`` — never emitted into SQL."""
        scope_id = f"{root_relation}#{self._scope_seq}"
        self._scope_seq += 1
        return scope_id


# Final-stage keys are DOTTED; inner-stage bind names are ``__``-joined; these owners keep them apart.


def result_key(*, source_relation: str, path: Tuple[str, ...] = (), leaf: str) -> str:
    """Dotted final-stage key ``source_relation`` . ``path`` . ``leaf`` (``leaf`` has no dot)."""
    if "." in leaf:
        raise ValueError(
            f"result_key leaf must not contain '.': {leaf!r}. Pass hops via "
            f"`path`, or use result_key_from_alias for a canonical dotted alias."
        )
    return ".".join((source_relation, *path, leaf))


def result_key_from_alias(*, source_relation: str, alias: str) -> str:
    return f"{source_relation}.{alias}"


def flat_name(dotted: str, *, strip_relation: Optional[str] = None) -> str:
    """Flatten a dotted name to its ``__``-joined bind name; ``strip_relation`` removes an exact ``f"{strip_relation}."`` prefix first."""
    remainder = dotted
    if strip_relation is not None:
        prefix = f"{strip_relation}."
        if remainder.startswith(prefix):
            remainder = remainder[len(prefix):]
    return remainder.replace(".", "__")


# Structural aliases taken as constants: each scopes a derived table its own pass creates.

OUTER_WRAP_ALIAS = "_outer"
STAGE_INNER_ALIAS = "_stage_inner"
FILTERED_ALIAS = "_filtered"


# Written out, not ``\W``: Python's ``\W`` is Unicode-aware and would let non-ASCII letters into a bare ASCII identifier.
_NON_IDENT_CHAR_RE = re.compile(r"[^a-zA-Z0-9_]")  # NOSONAR(S6353) — see above: \W is Unicode-aware and would not be equivalent.


def cte_name_from_alias(
    *,
    prefix: str,
    alias: str,
    allocator: "AliasAllocator",
    dialect: str = "postgres",
    limit: Optional[int] = None,
) -> str:
    """Mint a collision-safe CTE name for ``alias``, length-fitted to ``limit`` BEFORE
    allocation so a deep name can't truncate and collide. Only a PREFERRED name (flatten
    + sanitise are lossy); dedup is the caller's decision on structural identity, not this."""
    sanitized = _NON_IDENT_CHAR_RE.sub("_", flat_name(alias))
    preferred = fit_identifier(name=prefix + sanitized, limit=limit)
    name = allocator.allocate_cte(preferred)
    if limit is not None and len(name.encode("utf-8")) > limit:
        raise IdentifierCollisionError(
            first=preferred, second=name, emitted=name,
            dialect=dialect, limit=limit, namespace="CTE name",
        )
    return name


# The four alias-derivation profiles, named so impossible combinations can't be built.
AggAliasProfile = Literal[
    # ``_cm_`` CTE + projection alias: prefixes source relation AND path; no leaf → star.
    "cross_model_cte",
    # The aggregate's output column inside its producer CTE: bare canonical name.
    "cte_schema",
    # A hidden slot's declared name: bare; no leaf → explicit ``_agg_<name>``, not a star.
    "declared_name",
    # A formula's public alias: path relative to the stage; DECLINES (None) on a no-leaf source.
    "stage_formula",
]

_PROFILES_WITHOUT_RELATION = ("cte_schema", "declared_name", "stage_formula")


def canonical_aggregate_alias(  # NOSONAR(S3776) — sequential dispatch over the four frozen alias profiles; each branch IS that profile's contract, and extracting per-profile helpers would restore the four-copy drift this function removes.
    key: "AggregateKey",
    *,
    profile: AggAliasProfile,
    source_relation: Optional[str] = None,
) -> Optional[str]:
    """The single canonical-aggregate-alias derivation; ``profile`` picks the caller's contract."""
    if profile == "cross_model_cte":
        if source_relation is None:
            raise ValueError(
                "canonical_aggregate_alias(profile='cross_model_cte') requires "
                "source_relation — the alias is anchored at the query root.",
            )
    elif profile in _PROFILES_WITHOUT_RELATION:
        if source_relation is not None:
            raise ValueError(
                f"canonical_aggregate_alias(profile={profile!r}) does not take "
                f"source_relation: that profile emits no relation prefix.",
            )
    else:
        raise ValueError(
            f"Unknown canonical-aggregate-alias profile {profile!r}; "
            f"expected one of {('cross_model_cte', *_PROFILES_WITHOUT_RELATION)}.",
        )

    is_star = isinstance(key.source, StarKey)
    leaf = getattr(key.source, "leaf", None) or getattr(
        key.source, "column_name", None,
    )

    if profile in ("cross_model_cte", "cte_schema"):
        measure_name: Optional[str] = leaf or "*"
    elif profile == "declared_name":
        if is_star:
            measure_name = "*"
        elif leaf is None:
            # Explicit placeholder, NOT the star form, so it stays distinguishable.
            return f"_agg_{key.agg}"
        else:
            measure_name = leaf
    else:  # stage_formula
        measure_name = "*" if is_star else leaf
        if measure_name is None:
            return None

    canonical = canonical_agg_name(
        measure_name=measure_name,
        aggregation_name=key.agg,
        agg_args=[agg_kwarg_canonical_str(a) for a in key.args] or None,
        agg_kwargs={
            k: agg_kwarg_canonical_str(v) for k, v in key.kwargs
        } or None,
    )

    # Host-grain and target-grain aggregates intern separately; ``_host`` keeps their columns distinct.
    if getattr(key, "grain", "target") == "host":
        canonical = f"{canonical}_host"

    canonical = f"{canonical}{partition_by_suffix(getattr(key, 'partition_keys', None))}"

    if profile in ("cte_schema", "declared_name"):
        return canonical

    # Every source kind carries its join path, so ``customers.*:count`` keeps the hop.
    path: Tuple[str, ...] = tuple(getattr(key.source, "path", ()))

    if profile == "stage_formula":
        return (".".join(path) + "." if path else "") + canonical

    assert source_relation is not None  # guaranteed by the validation above
    return result_key(
        source_relation=source_relation, path=path, leaf=canonical,
    )




# Case-folding dialects reach the wrong physical object unless mixed-case is quoted.


def maybe_quote_ident(ident: Optional[exp.Expression]) -> None:
    """Set ``quoted=True`` in place on an unquoted ``Identifier`` with an uppercase letter."""
    if (
        isinstance(ident, exp.Identifier)
        and not ident.quoted
        and any(c.isupper() for c in ident.this)
    ):
        ident.set("quoted", True)


def quote_mixed_case_identifiers(node: exp.Expression) -> exp.Expression:
    """Quote mixed-case DB identifiers (``.transform`` callback): a ``Column``'s name leaf and a ``Table``'s physical parts only. Idempotent."""
    if isinstance(node, exp.Column):
        maybe_quote_ident(node.this)
    elif isinstance(node, exp.Table):
        maybe_quote_ident(node.this)
        maybe_quote_ident(node.args.get("db"))
        maybe_quote_ident(node.args.get("catalog"))
    return node


def assert_unique_cte_names(sql: str, *, dialect: str = "postgres") -> None:
    """Assert every CTE name is unique within each ``WITH`` scope. On case-folding dialects
    names are compared folded regardless of quoting (SLayer never quotes its own CTE names)."""
    fold = dialect_folds_case(dialect)
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    for with_node in parsed.find_all(exp.With):
        names = [cte.alias_or_name for cte in with_node.expressions]
        seen: dict[str, str] = {}
        for name in names:
            key = name.lower() if fold else name
            if key in seen:
                first = seen[key]
                fold_note = (
                    f" ({first!r} and {name!r} case-fold to {key!r} on "
                    f"{dialect})"
                    if first != name
                    else ""
                )
                raise ValueError(
                    f"Duplicate CTE name {name!r} within one WITH scope"
                    f"{fold_note}: {names}"
                )
            seen[key] = name
