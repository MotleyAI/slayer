"""Shared fixtures for DEV-1743 — the dotted-canonical `__` flip.

Model shapes exercising the four corners of the flip. Every model is built by a
FUNCTION, never a module-level constant: constructing a ``__``-named model
raises today (the ban this issue lifts), so importing this module must stay
safe — the feature-missing failure belongs inside the test that calls the
builder, not at collection time.

Underscore-prefixed so pytest skips collection here (like
``tests/_engine_helpers.py`` / ``tests/_dev1750_fixtures.py``).

The scenarios:

* :func:`chain_models` — plain ``orders → customers → regions``. Dotted-canonical
  Mode-A resolution and registry byte-stability (no ``__`` in any model name).
* :func:`dunder_target_models` — ``orders`` with a DIRECT join to a model
  literally named ``customer__region``. The headline: a ``__``-named model is a
  legal exact-match join target in both modes.
* :func:`ambiguity_impossible_models` — ``orders`` joins BOTH a model literally
  named ``a__b`` (direct) AND a chain ``a → b`` reaching a column. Mode-A
  ``a__b.val`` is the direct model; ``a.b.val`` is the chain. Both resolve, no
  conflict — the demonstration that ambiguity is structurally impossible, and
  the registry collision case (path ``(a, b)`` alias ``a__b`` vs direct model
  ``a__b``) must mint DISTINCT emitted aliases.
* :func:`long_chain_models` — a 4-hop chain of long model names whose naive
  ``__`` alias exceeds Postgres's 63-byte identifier limit.
"""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)

DS = "test"


def _col(name: str, type_: DataType = DataType.STRING, *, pk: bool = False) -> DatasetField:
    return DatasetField(name=name, datatype=type_, primary_key=pk)


# --------------------------------------------------------------------------- #
# Scenario 1 — plain chain orders → customers → regions.
# --------------------------------------------------------------------------- #
def chain_regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source=DS, sql_table="regions",
        fields=[_col("id", DataType.INTEGER, pk=True), _col("name")],
    )


def chain_customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers",
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("region_id", DataType.INTEGER),
            _col("name"),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def chain_orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("customer_id", DataType.INTEGER),
            _col("amount", DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


def chain_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` order (host first, as ``_engine_generate`` wants)."""
    return [chain_orders(), chain_customers(), chain_regions()]


# --------------------------------------------------------------------------- #
# Scenario 2 — a ``__``-named model as a DIRECT join target.
# --------------------------------------------------------------------------- #
def dunder_target() -> SlayerDataset:
    """A denormalised dim literally named ``customer__region``.

    Constructing this raises today (``_validate_model_name`` rejects ``__``);
    the flip must let it through.
    """
    return SlayerDataset(
        name="customer__region", data_source=DS, sql_table="customer_region_dim",
        fields=[_col("id", DataType.INTEGER, pk=True), _col("label")],
    )


def dunder_target_host() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("cr_id", DataType.INTEGER),
            _col("amount", DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(to="customer__region", from_columns=["cr_id"], to_columns=["id"]),
        ],
    )


def dunder_target_datasets() -> List[SlayerDataset]:
    return [dunder_target_host(), dunder_target()]


# --------------------------------------------------------------------------- #
# Scenario 3 — ambiguity is impossible: direct ``a__b`` model AND a chain a→b.
# --------------------------------------------------------------------------- #
def ai_b() -> SlayerDataset:
    return SlayerDataset(
        name="b", data_source=DS, sql_table="b",
        fields=[_col("id", DataType.INTEGER, pk=True), _col("val")],
    )


def ai_a() -> SlayerDataset:
    return SlayerDataset(
        name="a", data_source=DS, sql_table="a",
        fields=[_col("id", DataType.INTEGER, pk=True), _col("b_id", DataType.INTEGER)],
        relationships=[DatasetRelationship(to="b", from_columns=["b_id"], to_columns=["id"])],
    )


def ai_a__b() -> SlayerDataset:
    """A model literally named ``a__b`` — collides in SPELLING with the alias the
    chain ``a → b`` mints (``a__b``), but is a different relation."""
    return SlayerDataset(
        name="a__b", data_source=DS, sql_table="a_b_direct",
        fields=[_col("id", DataType.INTEGER, pk=True), _col("val")],
    )


def ai_host() -> SlayerDataset:
    return SlayerDataset(
        name="host", data_source=DS, sql_table="host",
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("a_id", DataType.INTEGER),
            _col("ab_id", DataType.INTEGER),
            # Distinctly-named derived columns so a query can project BOTH the
            # chain leaf and the direct model in one SELECT without tripping the
            # (orthogonal) flat public-name namespace — isolating the D4
            # join-alias-distinctness concern.
            DatasetField(name="chain_val", datatype=DataType.STRING, sql="a.b.val"),
            DatasetField(name="direct_val", datatype=DataType.STRING, sql="a__b.val"),
        ],
        relationships=[
            DatasetRelationship(to="a", from_columns=["a_id"], to_columns=["id"]),
            DatasetRelationship(to="a__b", from_columns=["ab_id"], to_columns=["id"]),
        ],
    )


def ambiguity_impossible_datasets() -> List[SlayerDataset]:
    return [ai_host(), ai_a(), ai_b(), ai_a__b()]


# --------------------------------------------------------------------------- #
# Scenario 4 — 4-hop chain of long names crossing Postgres's 63-byte limit.
#
# Names chosen so the 3-hop naive alias is already long and the 4-hop one
# exceeds 63 bytes:
#   subscription__customer__consumer__household  (44 + separators) — the last
# hop pushes the naive ``__``-joined alias over the limit.
# --------------------------------------------------------------------------- #
_L1 = "subscription_entity"
_L2 = "customer_entity_record"
_L3 = "consumer_entity_record"
_L4 = "household_entity_record"


def long_household() -> SlayerDataset:
    return SlayerDataset(
        name=_L4, data_source=DS, sql_table=_L4,
        fields=[_col("id", DataType.INTEGER, pk=True), _col("bucket")],
    )


def long_consumer() -> SlayerDataset:
    return SlayerDataset(
        name=_L3, data_source=DS, sql_table=_L3,
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("household_id", DataType.INTEGER),
        ],
        relationships=[DatasetRelationship(to=_L4, from_columns=["household_id"], to_columns=["id"])],
    )


def long_customer() -> SlayerDataset:
    return SlayerDataset(
        name=_L2, data_source=DS, sql_table=_L2,
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("consumer_id", DataType.INTEGER),
        ],
        relationships=[DatasetRelationship(to=_L3, from_columns=["consumer_id"], to_columns=["id"])],
    )


def long_subscription() -> SlayerDataset:
    return SlayerDataset(
        name=_L1, data_source=DS, sql_table=_L1,
        fields=[
            _col("id", DataType.INTEGER, pk=True),
            _col("customer_id", DataType.INTEGER),
            _col("amount", DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to=_L2, from_columns=["customer_id"], to_columns=["id"])],
    )


def long_chain_datasets() -> List[SlayerDataset]:
    """Host-first: subscription → customer → consumer → household."""
    return [long_subscription(), long_customer(), long_consumer(), long_household()]


#: The full 4-hop dotted path from the host to ``household.bucket``.
LONG_DOTTED_PATH = f"{_L2}.{_L3}.{_L4}.bucket"
#: What the naive ``__`` alias of that 4-hop path would be (over 63 bytes).
LONG_NAIVE_ALIAS = f"{_L2}__{_L3}__{_L4}"


def datasource(dialect: str = "postgres") -> DatasourceConfig:
    return DatasourceConfig(name=DS, type=dialect)


__all__ = [
    "DS",
    "chain_datasets", "chain_orders", "chain_customers", "chain_regions",
    "dunder_target_datasets", "dunder_target", "dunder_target_host",
    "ambiguity_impossible_datasets", "ai_host", "ai_a", "ai_b", "ai_a__b",
    "long_chain_datasets", "long_subscription", "LONG_DOTTED_PATH",
    "LONG_NAIVE_ALIAS",
    "datasource",
]
