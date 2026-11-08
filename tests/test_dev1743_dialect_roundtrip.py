"""DEV-1743 category 9 — dialect wire-encoding round-trip for ``__``-named models.

BigQuery and T-SQL cannot emit dotted output-column names, so SLayer mangles
projection aliases ``.`` -> ``___`` on emit and decodes them back on result-row
keys (``slayer/sql/naming_bijection.py``, escape-doubling pre-existing ``___``).

Once model names may contain ``__`` (the flip this issue lifts), a result key
like ``orders.customer__region.label`` must still survive that wire round-trip
without corruption — the model's literal ``__`` stays ``__`` while the dot
separators become ``___`` — and a genuinely ambiguous encode collision
(``a__`` + ``col`` vs ``a`` + ``__col``, which both encode to ``a_____col``)
must raise a loud ``IdentifierCollisionError`` rather than silently collapse.

Each test is labelled:
* FAIL-FIRST — fails today because constructing the ``__``-named model raises;
  after the flip the emitted SQL must be structurally valid.
* INVARIANT LOCK — passes today; pins wire-layer behaviour the flip must NOT
  disturb (the bijection already escape-doubles, so names with ``__``/``___``
  round-trip and the ambiguous case already raises). These prove the wire
  layer needs no change.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import IdentifierCollisionError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import get_dialect
from slayer.sql.naming_bijection import decode_alias, encode_alias

from tests._dev1743_fixtures import dunder_target
from tests._engine_helpers import _engine_generate


#: The dotted result key for the ``label`` column on the ``__``-named join
#: target ``customer__region``, reached from the ``orders`` host.
DUNDER_KEY = "orders.customer__region.label"


def _host_with_label() -> SlayerModel:
    """``orders`` host with a Mode-A derived column reading the ``__``-named
    join target ``customer__region`` (exact-match, single hop).

    The host itself carries no ``__`` in its name, so it constructs today; the
    feature-missing raise comes from :func:`dunder_target` (the join target,
    literally named ``customer__region``) at query time.
    """
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="cr_id", type=DataType.INT),
            Column(name="region_label", type=DataType.TEXT,
                   sql="customer__region.label"),
        ],
        joins=[ModelJoin(target_model="customer__region",
                         join_pairs=[["cr_id", "id"]])],
    )


# --------------------------------------------------------------------------- #
# 1-2. End-to-end BigQuery / T-SQL emission over a ``__``-named model.
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_bigquery_emits_over_dunder_named_model() -> None:
    """FAIL-FIRST: a query over a ``__``-named join target emits valid BigQuery.

    Fails today: building the join target ``dunder_target()`` raises at
    ``SlayerModel(name="customer__region")`` construction. After the flip the
    BigQuery SQL must be structurally valid (``_engine_generate`` parses it with
    sqlglot's bigquery dialect) and must reference the target's physical table.
    """
    sql = await _engine_generate(
        query=SlayerQuery(source_model="orders", dimensions=["region_label"]),
        model=_host_with_label(),
        extra_models=[dunder_target()],
        dialect="bigquery",
    )
    assert "customer_region_dim" in sql
    assert "label" in sql


@pytest.mark.asyncio
async def test_tsql_emits_over_dunder_named_model() -> None:
    """FAIL-FIRST: same as the BigQuery case, for T-SQL.

    Fails today at ``dunder_target()`` construction; after the flip the emitted
    T-SQL must parse (bracket-anchored dotted-alias mangling) and reference the
    target's physical table.
    """
    sql = await _engine_generate(
        query=SlayerQuery(source_model="orders", dimensions=["region_label"]),
        model=_host_with_label(),
        extra_models=[dunder_target()],
        dialect="tsql",
    )
    assert "customer_region_dim" in sql
    assert "label" in sql


# --------------------------------------------------------------------------- #
# 3. Result-key decode for a ``__``-named model round-trips on the wire.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
def test_dunder_result_key_round_trips_through_dialect(dialect: str) -> None:
    """INVARIANT LOCK: the wire form of ``orders.customer__region.label``
    decodes back to the dotted canonical.

    A pure unit test on the dialect object (no ``__``-named model construction
    needed), so it passes today — locking that a model name's literal ``__``
    survives the ``.``->``___`` wire round-trip. The dots become ``___``
    separators while the model's ``__`` passes through verbatim, keeping the two
    encodings distinct.
    """
    d = get_dialect(dialect)
    wire = d.emit_alias(DUNDER_KEY)
    # Dots were mangled to ``___``, but the model's literal ``__`` is preserved.
    assert wire != DUNDER_KEY
    assert "customer__region" in wire
    got = d.decode_result_keys([{wire: "north"}], aliases=[DUNDER_KEY])
    assert got == [{DUNDER_KEY: "north"}]


# --------------------------------------------------------------------------- #
# 4. Bijection escape-doubling round-trips names with ``__`` and ``___``.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "original",
    [
        DUNDER_KEY,             # ``__`` inside a model-name segment
        "a___b.c",              # pre-existing ``___`` (escape-doubled)
        "orders.a__b.c__d",     # ``__`` in two segments across dots
        "orders.my___metric",   # ``___`` in the leaf
    ],
)
def test_encode_decode_bijection_survives_dunder(original: str) -> None:
    """INVARIANT LOCK: ``decode_alias(encode_alias(s)) == s`` for names carrying
    ``__`` and ``___``.

    Passes today — the bijection already escape-doubles ``___`` before the dot
    rewrite, so the wire layer needs no change for the flip. This is the
    regression lock proving that.
    """
    assert decode_alias(encode_alias(original)) == original


# --------------------------------------------------------------------------- #
# 5. A forced encode collision raises loudly instead of silently collapsing.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
def test_ambiguous_wire_collision_raises(dialect: str) -> None:
    """INVARIANT LOCK: ``a__`` + ``col`` and ``a`` + ``__col`` both encode to
    ``a_____col`` (five underscores: 2 + 3-for-the-dot vs 3-for-the-dot + 2), so
    ``encode_alias`` is NOT injective across them — and ``decode_alias_map``
    must raise ``IdentifierCollisionError`` (namespace ``result key``) rather
    than let ``a__.col`` silently decode as ``a.__col``.

    Constructible against the dialect method directly, and it already raises
    today, so this locks the guard the flip relies on — a ``__``-named model
    whose emitted key would collide with a differently-shaped one fails loudly.
    """
    d = get_dialect(dialect)
    twin_a = "a__.col"   # model ``a__``, column ``col``
    twin_b = "a.__col"   # model ``a``, column ``__col``
    assert d.emit_alias(twin_a) == d.emit_alias(twin_b)  # non-injective encode
    with pytest.raises(IdentifierCollisionError, match="result key"):
        d.decode_result_keys([{}], aliases=[twin_a, twin_b])
