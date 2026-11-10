"""Every vendored upstream Ossie example: core models validate and import, ontology documents are skipped (spec:
importers/osi-import › Upstream examples import)."""

import json
import logging
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml

from slayer.core.models import DatasourceConfig
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import FIXTURES, SCHEMA_PATH, by_name, query, run_converter, seed_db, visible

UPSTREAM = FIXTURES / "upstream"
DATASOURCE = "osids"

_RENAMED_DDL = [
    "CREATE SCHEMA public",
    "CREATE TABLE public.orders (o_orderkey INTEGER, o_custkey INTEGER, o_orderdate DATE, "
    "o_totalprice DECIMAL(12,2), total_cents INTEGER)",
    "CREATE TABLE public.customers (c_custkey INTEGER, c_name VARCHAR, c_mktsegment VARCHAR)",
    "INSERT INTO public.orders VALUES (1, 10, '2026-01-05', 10.50, 1050), (2, 10, '2026-01-06', 4.25, 425), "
    "(3, 11, '2026-02-01', 7.00, 700)",
    "INSERT INTO public.customers VALUES (10, 'Ann', 'b2b'), (11, 'Bo', 'b2c')",
]
_TPCDS_DDL = [
    "CREATE SCHEMA public",
    "CREATE TABLE public.store_sales (ss_sold_date_sk INTEGER, ss_item_sk INTEGER, ss_ticket_number INTEGER, "
    "ss_customer_sk INTEGER, ss_store_sk INTEGER, ss_quantity INTEGER, ss_sales_price DECIMAL(7,2), "
    "ss_ext_sales_price DECIMAL(7,2), ss_net_profit DECIMAL(7,2))",
    "CREATE TABLE public.date_dim (d_date_sk INTEGER, d_date DATE, d_year INTEGER, d_quarter_name VARCHAR, "
    "d_moy INTEGER)",
    "CREATE TABLE public.customer (c_customer_sk INTEGER, c_customer_id VARCHAR, c_first_name VARCHAR, "
    "c_last_name VARCHAR, c_email_address VARCHAR)",
    "CREATE TABLE public.item (i_item_sk INTEGER, i_item_id VARCHAR, i_item_desc VARCHAR, i_brand VARCHAR, "
    "i_category VARCHAR, i_current_price DECIMAL(7,2))",
    "CREATE TABLE public.store (s_store_sk INTEGER, s_store_id VARCHAR, s_store_name VARCHAR, s_city VARCHAR, "
    "s_state VARCHAR, s_number_employees INTEGER)",
]
_FLIGHTS_DDL = [
    'CREATE SCHEMA "SCHEMA"',
    'CREATE TABLE "SCHEMA".RUNWAYS (airport_code VARCHAR, length INTEGER, shape VARCHAR, designator VARCHAR)',
    'CREATE TABLE "SCHEMA".AIRCRAFT (serial_nr VARCHAR, name VARCHAR, nr_seats INTEGER, tail_nr VARCHAR, '
    "carrier_code VARCHAR, manufacturer VARCHAR, model VARCHAR, year INTEGER, capacity INTEGER)",
    'CREATE TABLE "SCHEMA".AIRPORTS (state_code VARCHAR, market_nm VARCHAR, longitude DOUBLE, opened DATE, '
    "state_nm VARCHAR, city_nm VARCHAR, name VARCHAR, code VARCHAR, latitude DOUBLE)",
    'CREATE TABLE "SCHEMA".FLIGHTS (dep_delay INTEGER, air_time INTEGER, nr INTEGER, carrier_code VARCHAR, '
    "duration INTEGER, scheduled_departure TIMESTAMP, arr_delay INTEGER, cancelled BOOLEAN, cancel_code VARCHAR, "
    "distance INTEGER, id INTEGER, scheduled_arrival TIMESTAMP, wheels_on TIMESTAMP, date DATE, "
    "wheels_off TIMESTAMP, scheduled_duration INTEGER, tail_nr VARCHAR, departure VARCHAR, route_id INTEGER, "
    "diverted BOOLEAN, arrival VARCHAR)",
    'CREATE TABLE "SCHEMA".CARRIERS (code VARCHAR, name VARCHAR)',
    'CREATE TABLE "SCHEMA".ROUTES (orig_airport_code VARCHAR, dest_airport_code VARCHAR, distance INTEGER, '
    "dist_grp VARCHAR, id INTEGER, name VARCHAR)",
]
_CORE = ["renamed_fields.yaml", "tpcds_semantic_model.yaml", "flights.semantic_model.yaml"]
_ONTOLOGY = ["flights.yaml", "flights.ontology.yaml", "flights.mapping.yaml"]


def _document(name: str) -> dict[str, Any]:
    return yaml.safe_load((UPSTREAM / name).read_text(encoding="utf-8"))


def _import(tmp_path: Path, name: str, ddl: list[str]) -> tuple[ConversionResult, Path]:
    db = seed_db(directory=tmp_path, statements=ddl)
    converter = OsiToSlayerConverter(documents=parse_osi_path(UPSTREAM / name), data_source=DATASOURCE,
                                     target_dialect="duckdb")
    return run_converter(converter, datasource=DatasourceConfig(name=DATASOURCE, type="duckdb", database=str(db))), db


def _declared_fields(name: str) -> dict[str, set[str]]:
    return {d["name"]: {x["name"] for x in d.get("fields") or []} for d in _document(name)["datasets"]}


def test_every_upstream_example_is_vendored() -> None:
    assert sorted(p.name for p in UPSTREAM.iterdir()) == sorted([*_CORE, *_ONTOLOGY])


@pytest.mark.parametrize("name", _CORE)
def test_core_example_validates(name: str) -> None:
    jsonschema.Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))).validate(_document(name))


async def test_renamed_fields_example(tmp_path: Path) -> None:
    result, db = _import(tmp_path, "renamed_fields.yaml", _RENAMED_DDL)
    models = by_name(result)
    assert {name: visible(model) for name, model in models.items()} == _declared_fields("renamed_fields.yaml")
    assert not result.unconverted_metrics
    rows = await query(result=result, db=db, payload={"source_model": "orders", "measures": ["gross_revenue"]})
    assert float(rows[0]["orders.gross_revenue"]) == pytest.approx(21.75)


def test_tpcds_example(tmp_path: Path) -> None:
    result, _ = _import(tmp_path, "tpcds_semantic_model.yaml", _TPCDS_DDL)
    assert {name: visible(model) for name, model in by_name(result).items()} == \
        _declared_fields("tpcds_semantic_model.yaml")
    assert sorted(e.metric_name or "" for e in result.unconverted_metrics) == [
        "brand_rank_in_store", "cumulative_sales", "monthly_sales_change"]


def test_flights_semantic_model_example(tmp_path: Path) -> None:
    result, _ = _import(tmp_path, "flights.semantic_model.yaml", _FLIGHTS_DDL)
    assert {name: visible(model) for name, model in by_name(result).items()} == \
        _declared_fields("flights.semantic_model.yaml")
    assert not result.unconverted_metrics


@pytest.mark.parametrize("name", _ONTOLOGY)
def test_ontology_example_is_skipped(name: str, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        assert parse_osi_path(UPSTREAM / name) == []
    assert any(name in r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)
