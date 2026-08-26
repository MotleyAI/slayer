"""DEV-1743 — validator relaxation (WP1) and the ``__slayer_`` reservation (P3).

The flip lifts the ``__`` ban on model/query names while keeping ``.``/``:``
banned, and reserves the ``__slayer_`` prefix (which the colon-agg preprocessor
mints internally) so user input cannot spoof a placeholder.

These fail today because the ban is still in force: constructing a ``__``-named
model raises, and the parser lets a literal ``__slayer_agg_0__`` through (the
pre-existing spoof hole [C3] closes).
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.syntax import parse_expr
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1743_fixtures import datasource


# --------------------------------------------------------------------------- #
# The ban is lifted: __ is legal in model / query names.
# --------------------------------------------------------------------------- #
class TestDunderNamesAccepted:
    def test_model_name_with_dunder_constructs(self) -> None:
        m = SlayerModel(name="customer__region", data_source="ds",
                        sql_table="customer_region")
        assert m.name == "customer__region"

    def test_query_name_with_dunder_constructs(self) -> None:
        q = SlayerQuery(name="stage__two", source_model="orders")
        assert q.name == "stage__two"

    def test_triple_underscore_model_name_constructs(self) -> None:
        m = SlayerModel(name="a___b", data_source="ds", sql_table="a___b")
        assert m.name == "a___b"


# --------------------------------------------------------------------------- #
# The remaining bans stay: dots and colons are still illegal in names.
# --------------------------------------------------------------------------- #
class TestDotAndColonStillRejected:
    @pytest.mark.parametrize("bad", ["a.b", "orders.customers", "a:b"])
    def test_dotted_or_coloned_model_name_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError):
            SlayerModel(name=bad, data_source="ds", sql_table="t")

    @pytest.mark.parametrize("bad", ["a.b", "a:b"])
    def test_dotted_or_coloned_column_name_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError):
            Column(name=bad, type=DataType.TEXT)

    @pytest.mark.parametrize("bad", ["a.b", "a:b"])
    def test_dotted_or_coloned_measure_name_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError):
            ModelMeasure(name=bad, formula="amount:sum")

    @pytest.mark.parametrize("bad", ["a.b", "a:b"])
    def test_dotted_or_coloned_query_name_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError):
            SlayerQuery(name=bad, source_model="orders")


# --------------------------------------------------------------------------- #
# Column names with __ stay legal (D5 carve-out — unchanged).
# --------------------------------------------------------------------------- #
class TestFlatColumnNamesUnchanged:
    def test_flat_column_name_still_allowed(self) -> None:
        col = Column(name="stores__name", type=DataType.TEXT)
        assert col.name == "stores__name"


# --------------------------------------------------------------------------- #
# P3 — the __slayer_ prefix is reserved on every user-facing name surface.
# --------------------------------------------------------------------------- #
class TestSlayerPrefixReservedOnNames:
    def test_model_name_reserved(self) -> None:
        with pytest.raises(ValueError):
            SlayerModel(name="__slayer_x", data_source="ds", sql_table="t")

    def test_query_name_reserved(self) -> None:
        with pytest.raises(ValueError):
            SlayerQuery(name="__slayer_x", source_model="orders")

    def test_column_name_reserved(self) -> None:
        with pytest.raises(ValueError):
            Column(name="__slayer_agg_0__", type=DataType.TEXT)

    def test_measure_name_reserved(self) -> None:
        with pytest.raises(ValueError):
            ModelMeasure(name="__slayer_x", formula="amount:sum")


# --------------------------------------------------------------------------- #
# P3 — the parser rejects a literal __slayer_ spoof but keeps colon-aggs working.
# The spoof scan runs on the RAW input, BEFORE _preprocess_colons substitutes
# its own __slayer_agg_N__ placeholders [C3].
# --------------------------------------------------------------------------- #
class TestParserSlayerPrefixReservation:
    def test_literal_placeholder_spoof_rejected(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("__slayer_agg_0__")

    def test_slayer_prefixed_identifier_rejected(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("__slayer_secret + 1")

    def test_colon_aggregation_still_parses(self) -> None:
        # revenue:sum is rewritten to a __slayer_agg_ placeholder INTERNALLY;
        # the raw-input scan must not mistake that for a spoof.
        parsed = parse_expr("revenue:sum")
        assert parsed is not None

    def test_multiple_aggregations_still_parse(self) -> None:
        parsed = parse_expr("revenue:sum / *:count")
        assert parsed is not None

    def test_dotted_colon_aggregation_still_parses(self) -> None:
        parsed = parse_expr("customers.spend:sum")
        assert parsed is not None

    def test_dunder_bearing_identifier_now_parses(self) -> None:
        # A bare __-bearing reference is no longer rejected by the parser
        # (binding legality is the binder's concern, not the parser's). A
        # colon-agg like ``a__b.col:sum`` would hide the dunder inside the
        # placeholder map, so a BARE name is the real probe for the relaxation.
        parsed = parse_expr("a__b")
        assert parsed is not None


# --------------------------------------------------------------------------- #
# A __-named model round-trips through both storage backends.
# --------------------------------------------------------------------------- #
class TestDunderNameRoundTripsStorage:
    @pytest.mark.asyncio
    async def test_yaml_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(datasource())
            m = SlayerModel(name="a__b", data_source="test", sql_table="a_b",
                            columns=[Column(name="id", type=DataType.INT,
                                            primary_key=True)])
            await storage.save_model(m)
            loaded = await storage.get_model("a__b", data_source="test")
            assert loaded is not None
            assert loaded.name == "a__b"

    @pytest.mark.asyncio
    async def test_sqlite_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = SQLiteStorage(db_path=f"{d}/s.db")
            await storage.save_datasource(datasource())
            m = SlayerModel(name="a__b", data_source="test", sql_table="a_b",
                            columns=[Column(name="id", type=DataType.INT,
                                            primary_key=True)])
            await storage.save_model(m)
            loaded = await storage.get_model("a__b", data_source="test")
            assert loaded is not None
            assert loaded.name == "a__b"
