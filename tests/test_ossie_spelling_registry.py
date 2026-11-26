"""One registry of scalar spellings decides what the parser rewrites, what is reserved and what the strict binding
admits (spec: queries/scalar-functions › The strict binding admits exactly the Ossie functions that run)."""

from __future__ import annotations

import pytest

from slayer.core import syntax
from slayer.core.enums import DATE_PART_ALIASES
from slayer.core.keys import SCALAR_SPECS, SCALAR_SPELLING_REGISTRY, SCALAR_SPELLINGS
from slayer.core.ossie_conformance import OSSIE_CALL_SPELLINGS
from tests._ossie_gate_spec import argument_counts, spec_syntaxes, spec_text

SPEC = spec_text()
#: Spellings Ossie Layer 1 defines.
OSSIE = ("if", "iff", "nvl", "nvl2", "startswith", "endswith", "charindex", "truncate", "to_date", "to_timestamp",
         "datediff", "dateadd", "current_timestamp", "date_trunc")
#: Spellings SLayer accepts that Ossie does not define.
NON_OSSIE = ("strpos", "cardinality", "array_length", "array_contains", "json_extract", "json_extract_scalar",
             "json_extract_path_text", "jsonb_extract_path_text")
REWRITE = "rewrite"


def _ossie_counts() -> frozenset[str]:
    return frozenset(n for n, s in SCALAR_SPELLING_REGISTRY.items() if s.ossie_arg_counts)


def test_reserved_spellings_are_the_registry() -> None:
    assert SCALAR_SPELLINGS == frozenset(SCALAR_SPELLING_REGISTRY)


def test_parser_builders_are_the_rewrites() -> None:
    assert set(syntax._SPELLINGS) == {n for n, s in SCALAR_SPELLING_REGISTRY.items() if s.dispatch == REWRITE}


def test_every_rename_names_a_library_function() -> None:
    renames = {n: s.dispatch for n, s in SCALAR_SPELLING_REGISTRY.items() if s.dispatch != REWRITE}
    assert {n: d for n, d in renames.items() if d not in SCALAR_SPECS} == {}


def test_keys_are_lower_case_and_not_library_names() -> None:
    assert all(n == n.lower() for n in SCALAR_SPELLING_REGISTRY)
    assert set(SCALAR_SPELLING_REGISTRY) & set(SCALAR_SPECS) == set()


def test_strict_call_spellings_derive_from_the_registry() -> None:
    assert _ossie_counts() <= OSSIE_CALL_SPELLINGS
    assert OSSIE_CALL_SPELLINGS - _ossie_counts() <= set(DATE_PART_ALIASES)


@pytest.mark.parametrize("name", OSSIE)
def test_ossie_spelling_counts_match_the_spec(name: str) -> None:
    expected = frozenset(c for c, _variadic in filter(None, map(argument_counts, spec_syntaxes(name, SPEC))))
    assert SCALAR_SPELLING_REGISTRY[name].ossie_arg_counts == expected


@pytest.mark.parametrize("name", NON_OSSIE)
def test_non_ossie_spellings_have_no_counts(name: str) -> None:
    assert SCALAR_SPELLING_REGISTRY[name].ossie_arg_counts == frozenset()
