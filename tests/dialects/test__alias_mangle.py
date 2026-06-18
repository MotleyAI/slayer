"""DEV-1571: shared dotted-alias encode/decode bijection.

The same bijection backs ``BigqueryDialect`` (backtick-anchored regex over
the emitted SQL) and ``TsqlDialect`` (bracket-anchored regex). These tests
pin the bijection in isolation; per-dialect regex anchoring is covered by
``test_bigquery.py`` and ``test_tsql.py``.
"""

from __future__ import annotations

import pytest

from slayer.sql.dialects._alias_mangle import (
    _ALIAS_SEP,
    decode_alias,
    encode_alias,
)


def test_separator_is_triple_underscore() -> None:
    """``___`` is the chosen separator. Same as BigQuery's pre-DEV-1571
    private constant; T-SQL reuses it (different identifier-quote anchors
    mean the two dialects' regexes never collide).
    """
    assert _ALIAS_SEP == "___"


# ---------------------------------------------------------------------------
# encode_alias — basic shape
# ---------------------------------------------------------------------------


def test_encode_single_dot() -> None:
    """A single dot maps to a single ``___``."""
    assert encode_alias("orders.id") == "orders___id"


def test_encode_multi_hop() -> None:
    """Multi-hop dotted paths get one ``___`` per dot."""
    assert encode_alias("orders.products.category") == "orders___products___category"


def test_encode_leading_underscore_leaf() -> None:
    """``orders._count`` (the canonical alias for ``*:count``) encodes to
    ``orders____count`` — three underscores from the dot plus the literal
    leading underscore of the leaf."""
    assert encode_alias("orders._count") == "orders____count"


def test_encode_no_dot_returns_input() -> None:
    """A bare identifier with no dot and no pre-existing ``___`` is
    returned verbatim."""
    assert encode_alias("plain_col") == "plain_col"


def test_encode_user_triple_underscore_in_leaf() -> None:
    """A user-named measure containing ``___`` (e.g.
    ``orders.my___metric``) escape-doubles the inner ``___`` BEFORE the
    dot rewrite so the round-trip is unambiguous.

    Forward: ``orders.my___metric`` -> ``orders.my______metric`` (escape
    inner ``___``) -> ``orders___my______metric`` (dot rewrite).
    """
    assert encode_alias("orders.my___metric") == "orders___my______metric"


def test_encode_user_triple_underscore_only() -> None:
    """A bare name containing ``___`` (no dots) still escape-doubles.

    Edge case: the escape happens even when no dot rewrite follows. This
    pins the escape as a property of ``___`` in the input, not of the
    overall mangling shape.
    """
    assert encode_alias("my___metric") == "my______metric"


# ---------------------------------------------------------------------------
# decode_alias — direct inverse
# ---------------------------------------------------------------------------


def test_decode_single_separator() -> None:
    assert decode_alias("orders___id") == "orders.id"


def test_decode_multi_hop() -> None:
    assert decode_alias("orders___products___category") == "orders.products.category"


def test_decode_escape_doubled() -> None:
    """``______`` decodes to literal ``___`` (the escape, NOT a separator).

    Distinguishes the escape from the separator: ``______`` is a single
    escape token, not two consecutive separators.
    """
    assert decode_alias("orders______metric") == "orders___metric"


def test_decode_no_separator_is_identity() -> None:
    """A key with neither ``___`` nor a dot passes through unchanged."""
    assert decode_alias("plain_col") == "plain_col"


# ---------------------------------------------------------------------------
# Round-trip bijection — pins the inverse property on SLayer's alias space
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "original",
    [
        "orders.id",                                # simple
        "orders._count",                            # leading-underscore leaf
        "orders.products.category",                 # multi-hop
        "orders.my___metric",                       # user ``___`` in leaf
        "a.b.c___d",                                # ``___`` mid-string
        "orders.customers.regions.population_sum",  # 4-hop cross-model
    ],
)
def test_round_trip_on_dotted_aliases(original: str) -> None:
    """``decode_alias(encode_alias(x)) == x`` for every realistic SLayer
    alias shape.

    SLayer's projection aliases are always model-qualified (``<model>.X``),
    so they always contain at least one dot. The bijection is exact on
    that subset — see ``test_decode_corrupts_no_dot_key_with_triple_underscore``
    for the documented out-of-domain case.
    """
    assert decode_alias(encode_alias(original)) == original


def test_decode_corrupts_no_dot_key_with_triple_underscore() -> None:
    """Characterisation: a bare key with ``___`` and NO dot decodes to a
    dotted name — the bijection is NOT defined on this subset.

    Pins the documented domain constraint so a future "make it
    everywhere-defined" refactor is an explicit, reviewable change rather
    than a silent docstring-vs-behaviour drift. Mirrors BigQuery's
    pre-DEV-1571 characterisation pin (``test_decode_corrupts_no_dot_key_with_triple_underscore``
    in ``test_bigquery.py``).
    """
    assert decode_alias("my___metric") == "my.metric"
