"""Ossie's §8 error codes under the strict binding: each code, precedence, diagnostics, the typed error.

Spec: queries/ossie-l3-binding › Errors are Ossie's, in Ossie's order; Reachability errors name what fails to reach;
Model-scoped metrics are judged by their components; Paths decide reachability and ambiguity; Strict refusals are one
typed error on every surface.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core import errors
from slayer.core.errors import QueryTypeError, SlayerError, UnevaluableEntityError
from slayer.core.models import DatasourceConfig
from tests._dev2217_fixtures import CODES, DS, fixture_engine, fixture_texts, ossie_query_error, rows, strict, strict_error
from tests._dev2243_fixtures import store_document

WINDOW_FILTER = "ROW_NUMBER() OVER (ORDER BY orders.amount) = 1"
#: The relationships of the two paths from ``store_sales`` to ``date_dim`` on ``stores_ambiguous``.
PATHS_TO_DATE_DIM = ("store_sales_to_sale_date", "store_sales_to_store", "store_to_closing_date")

#: (fixture, query, code): each code on retail / stores beyond upstream's cases.
EACH_CODE: list[Any] = [
    pytest.param("retail", {"dimensions": ["orders.customers.region"]}, "E_NAME_NOT_FOUND", id="three-segment-path"),
    pytest.param("retail", {"dimensions": ["region"]}, "E_NAME_NOT_FOUND", id="bare-field"),
    pytest.param("retail", {"dimensions": ["nowhere.region"]}, "E_NAME_NOT_FOUND", id="unknown-dataset"),
    pytest.param("retail", {"metrics": ["no_such_metric"]}, "E_NAME_NOT_FOUND", id="unknown-bare-metric"),
    pytest.param("retail", {"metrics": ["orders.total_revenue"], "filter": "orders.no_such_field > 1"},
                 "E_NAME_NOT_FOUND", id="unknown-filter-reference"),
    pytest.param("retail", {"dimensions": ["orders.total_revenue", "customers.no_such_field"]}, "E_NAME_NOT_FOUND",
                 id="judged-across-the-whole-query"),
    pytest.param("retail", {"dimensions": ["revenue_per_customer"]}, "E_METRIC_IN_DIMENSIONS",
                 id="model-scoped-metric-in-dimensions"),
    pytest.param("stores", {"dimensions": ["store_sales.total_sales"]}, "E_METRIC_IN_DIMENSIONS",
                 id="dataset-metric-in-dimensions"),
    pytest.param("stores", {"metrics": ["store_sales.sales_amount"]}, "E_FIELD_IN_METRICS", id="field-in-metrics"),
    pytest.param("retail", {"metrics": ["orders.total_revenue"], "filter": "orders.total_revenue > 100"},
                 "E_AGGREGATE_IN_WHERE", id="metric-reference-in-filter"),
    pytest.param("retail", {"metrics": ["orders.total_revenue"], "filter": "revenue_per_customer > 1"},
                 "E_AGGREGATE_IN_WHERE", id="model-scoped-metric-in-filter"),
    pytest.param("retail", {"dimensions": ["customers.region"], "filter": "COUNT(*) > 0"}, "E_AGGREGATE_IN_WHERE",
                 id="count-star-in-filter"),
    pytest.param("retail", {"metrics": ["orders.total_revenue"], "filter": "RANK() OVER (ORDER BY orders.amount) = 1"},
                 "E_WINDOW_IN_WHERE", id="rank-in-filter"),
    pytest.param("retail", {"dimensions": ["customers.region"],
                            "filter": "orders.amount > 1 OR customers.region = 'EAST'"},
                 "E_CROSS_DATASET_FILTER", id="cross-dataset-filter-without-metrics"),
    pytest.param("retail", {"dimensions": ["visits.visit_id"], "metrics": ["orders.total_revenue"]}, "E_NO_PATH",
                 id="sibling-dataset"),
    pytest.param("stores", {"dimensions": ["item.brand"],
                            "metrics": ["store_sales.total_sales", "customers.customer_count"]},
                 "E_PARTIAL_DIMENSION_REACHABILITY", id="partial-stores"),
    pytest.param("retail", {"dimensions": ["customers.region", "tickets.ticket_id"]}, "E_NO_ANCHOR",
                 id="unconnected-dimensions"),
    pytest.param("stores", {"metrics": ["store_returns.total_returns"], "filter": "store.state = 'CA'"},
                 "E_UNSUPPORTED_FILTER_PATH", id="filter-dataset-the-metric-does-not-reach"),
    pytest.param("stores_ambiguous", {"dimensions": ["store_sales.sale_id", "date_dim.year"]}, "E_AMBIGUOUS_PATH",
                 id="anchor-with-two-paths"),
]

#: (fixture, query, code): two rows hold, the earlier wins.
PRECEDENCE: list[Any] = [
    pytest.param("retail", {"dimensions": ["customers.no_such_field"], "limit": 10}, "E_INVALID_QUERY", id="1>3"),
    pytest.param("retail", {"dimensions": ["customers.no_such_field"],
                            "filter": "orders.amount > 100 AND customers.region = 'EAST'"},
                 "E_NAME_NOT_FOUND", id="3>8"),
    pytest.param("retail", {"dimensions": ["orders.no_such_field"], "filter": WINDOW_FILTER}, "E_NAME_NOT_FOUND",
                 id="3>7"),
    pytest.param("retail", {"metrics": ["orders.total_revenue"],
                            "filter": "SUM(orders.amount) > 100 AND customers.region = 'EAST'"},
                 "E_AGGREGATE_IN_WHERE", id="6>8"),
    pytest.param("retail", {"dimensions": ["orders.order_id", {"field": "customers.region", "alias": "order_id"}],
                            "metrics": ["customers.customer_count"]},
                 "E_NAME_COLLISION", id="9>13"),
    pytest.param("retail", {"dimensions": ["customers.region", "orders.order_id"],
                            "metrics": ["customers.customer_count", "tickets.ticket_count"]},
                 "E_NO_PATH", id="13>14"),
    pytest.param("stores_ambiguous", {"dimensions": ["date_dim.year"],
                                      "metrics": ["store_sales.total_sales", "store_returns.total_returns"],
                                      "filter": "store.state = 'CA'"},
                 "E_UNSUPPORTED_FILTER_PATH", id="16>17"),
    pytest.param("retail", {"metrics": ["running_revenue"], "filter": "SUM(orders.amount) > 1"},
                 "E_AGGREGATE_IN_WHERE", id="6>11"),
    pytest.param("retail", {"dimensions": ["customers.region", {"field": "customers.customer_id", "alias": "region"}],
                            "metrics": ["amount_less_order"]},
                 "E_NAME_COLLISION", id="9>10"),
    pytest.param("retail", {"dimensions": ["tickets.ticket_id"], "metrics": ["row_count"]},
                 "E_AMBIGUOUS_MEASURE_GRAIN", id="12>13"),
    pytest.param("retail", {"metrics": ["no_such_metric", "order_less_return"]}, "E_NAME_NOT_FOUND", id="3>12"),
]


async def _code(tmp_path: Path, fixture: str, query: Any, code: str) -> Any:
    async with fixture_engine(tmp_path, fixture) as engine:
        return await strict_error(engine, query, code)


@pytest.mark.parametrize(("fixture", "query", "code"), EACH_CODE)
async def test_each_code(tmp_path: Path, fixture: str, query: Any, code: str) -> None:
    await _code(tmp_path, fixture, query, code)


@pytest.mark.parametrize(("fixture", "query", "code"), PRECEDENCE)
async def test_later_rows_yield_to_earlier_ones(tmp_path: Path, fixture: str, query: Any, code: str) -> None:
    """Later rows yield to earlier ones."""
    await _code(tmp_path, fixture, query, code)


async def test_unknown_names_before_a_window_in_the_filter(tmp_path: Path) -> None:
    """Unknown names before a window in the filter."""
    async with fixture_engine(tmp_path, "retail") as engine:
        await strict_error(engine, {"dimensions": ["orders.no_such_field"], "filter": WINDOW_FILTER},
                           "E_NAME_NOT_FOUND")
        await strict_error(engine, {"dimensions": ["orders.order_id"], "filter": WINDOW_FILTER}, "E_WINDOW_IN_WHERE")


async def test_an_unquoted_dataset_name_matches_case_insensitively(tmp_path: Path) -> None:
    """Identifying the datasource folds an unquoted dataset name and matches a quoted one exactly (Layer 1)."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await strict(engine, {"dimensions": ["CUSTOMERS.region"], "metrics": ["Orders.total_revenue"]})
        assert rows(response) == [("EAST", 350.0), ("WEST", 300.0)]
        await strict_error(engine, {"dimensions": ['"CUSTOMERS".region']}, "E_NAME_NOT_FOUND")


async def test_unknown_names_before_filter_errors(tmp_path: Path) -> None:
    """Unknown names before filter errors."""
    await _code(tmp_path, "retail", {"dimensions": ["customers.no_such_field"],
                                     "filter": "orders.amount > 100 AND customers.region = 'EAST'"},
                "E_NAME_NOT_FOUND")


# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #
async def test_partial_reachability_names_the_dimension_and_the_metric(tmp_path: Path) -> None:
    """Partial reachability names the dimension and the metric."""
    exc = await _code(tmp_path, "retail", {"dimensions": ["customers.region"],
                                           "metrics": ["orders.total_revenue", "tickets.ticket_count"]},
                      "E_PARTIAL_DIMENSION_REACHABILITY")
    assert (exc.dimension, exc.metric) == ("customers.region", "tickets.ticket_count")
    assert "customers.region" in str(exc)
    assert "tickets.ticket_count" in str(exc)


async def test_one_to_many_paths_are_refused(tmp_path: Path) -> None:
    """One-to-many paths are refused."""
    query = {"dimensions": ["orders.order_id"], "metrics": ["customers.customer_count"]}
    async with fixture_engine(tmp_path, "retail") as engine:
        exc = await strict_error(engine, query, "E_NO_PATH")
        native = await engine.execute(query)
    assert (exc.dimension, exc.metric) == ("orders.order_id", "customers.customer_count")
    assert "orders.order_id" in str(exc)
    assert "customers.customer_count" in str(exc)
    assert native.data
    assert native.warnings


async def test_no_path_names_the_first_dimension_and_metric_in_query_order(tmp_path: Path) -> None:
    exc = await _code(tmp_path, "retail", {"dimensions": ["customers.region", "orders.order_id"],
                                           "metrics": ["customers.customer_count", "tickets.ticket_count"]},
                      "E_NO_PATH")
    assert (exc.dimension, exc.metric) == ("orders.order_id", "customers.customer_count")


async def test_a_filter_path_names_the_anchor(tmp_path: Path) -> None:
    """A filter path names the anchor."""
    exc = await _code(tmp_path, "retail", {"dimensions": ["customers.region"], "filter": "orders.amount > 100"},
                      "E_UNSUPPORTED_FILTER_PATH")
    assert (exc.filter, exc.anchor, exc.metric) == ("orders.amount > 100", "customers", None)
    assert "customers" in str(exc)


async def test_a_filter_path_names_the_metric(tmp_path: Path) -> None:
    exc = await _code(tmp_path, "retail", {"dimensions": ["customers.region"],
                                           "metrics": ["orders.total_revenue", "returns.total_returns"],
                                           "filter": "orders.amount > 100"},
                      "E_UNSUPPORTED_FILTER_PATH")
    assert (exc.filter, exc.metric) == ("orders.amount > 100", "returns.total_returns")


async def test_an_ambiguous_path_names_the_datasets(tmp_path: Path) -> None:
    exc = await _code(tmp_path, "stores_ambiguous", {"dimensions": ["date_dim.year"],
                                                     "metrics": ["store_sales.total_sales"]},
                      "E_AMBIGUOUS_PATH")
    assert (exc.dimension, exc.metric) == ("date_dim.year", "store_sales.total_sales")
    for name in ("store_sales", "date_dim", *PATHS_TO_DATE_DIM):
        assert name in str(exc)


async def test_an_ambiguous_filter_path_names_the_filter(tmp_path: Path) -> None:
    exc = await _code(tmp_path, "stores_ambiguous", {"metrics": ["store_sales.total_sales"],
                                                     "filter": "date_dim.year = 2024"},
                      "E_AMBIGUOUS_PATH")
    assert (exc.filter, exc.metric) == ("date_dim.year = 2024", "store_sales.total_sales")
    for name in PATHS_TO_DATE_DIM:
        assert name in str(exc)


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
async def test_a_shorter_path_does_not_settle_ambiguity(tmp_path: Path) -> None:
    """A shorter path does not settle ambiguity."""
    query = {"dimensions": ["date_dim.year"], "metrics": ["store_sales.total_sales"]}
    await _code(tmp_path / "ambiguous", "stores_ambiguous", query, "E_AMBIGUOUS_PATH")
    async with fixture_engine(tmp_path / "plain", "stores") as engine:
        assert rows(await strict(engine, query)) == [(2023.0, 125.0), (2024.0, 290.0)]


async def test_a_second_path_elsewhere_does_not_matter(tmp_path: Path) -> None:
    """A second path elsewhere does not matter."""
    async with fixture_engine(tmp_path, "stores_ambiguous") as engine:
        response = await strict(engine, {"dimensions": ["date_dim.year"], "metrics": ["store_returns.total_returns"]})
    assert rows(response) == [(2023.0, 10.0), (2024.0, 5.0)]


async def test_unconnected_metrics_without_dimensions(tmp_path: Path) -> None:
    """Unconnected metrics without dimensions."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await strict(engine, {"metrics": ["orders.total_revenue", "tickets.ticket_count"]})
    assert response.columns == ["total_revenue", "ticket_count"]
    assert rows(response) == [(650.0, 0.0)]


# --------------------------------------------------------------------------- #
# Model-scoped metrics
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("metric", "code"), [
    ("amount_less_order", "E_UNAGGREGATED_FINER_GRAIN_REFERENCE"),
    ("running_revenue", "E_DEFERRED_WINDOWED_METRIC"),
    ("order_less_return", "E_AMBIGUOUS_MEASURE_GRAIN"),
    ("row_count", "E_AMBIGUOUS_MEASURE_GRAIN"),
])
async def test_the_model_scoped_restrictions(tmp_path: Path, metric: str, code: str) -> None:
    """The model-scoped restrictions."""
    exc = await _code(tmp_path, "retail", {"metrics": [metric]}, code)
    assert not isinstance(exc, UnevaluableEntityError)


async def test_components_aggregate_their_own_datasets(tmp_path: Path) -> None:
    """Components aggregate their own datasets."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await strict(engine, {"metrics": ["revenue_per_customer"]})
    assert response.data == [{"revenue_per_customer": pytest.approx(162.5)}]


async def test_a_native_error_after_admission_propagates_unchanged(tmp_path: Path) -> None:
    """An admitted query the database then fails on raises the native error, not an Ossie code."""
    query = {"metrics": ["orders.total_revenue"], "filter": "CAST(customers.region AS INTEGER) > 1"}
    async with fixture_engine(tmp_path, "retail") as engine:
        with pytest.raises(Exception) as native:  # noqa: PT011 — whatever the database raises
            await engine.execute(query)
        with pytest.raises(type(native.value)) as admitted:
            await strict(engine, query)
    assert not issubclass(type(admitted.value), ossie_query_error())


# --------------------------------------------------------------------------- #
# Model identification
# --------------------------------------------------------------------------- #
async def test_the_model_is_the_named_datasource(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await strict(engine, {"metrics": ["orders.total_revenue"]}, data_source=DS)
    assert response.data == [{"total_revenue": 650}]


async def test_model_identification_follows_native_resolution(tmp_path: Path) -> None:
    """Two datasources hold the model: strict identifies it as native does, raising the native error if ambiguous."""
    query = {"metrics": ["orders.total_revenue"]}
    async with fixture_engine(tmp_path, "retail") as engine:
        source = await engine.storage.get_datasource(DS)
        assert source is not None
        await engine.storage.save_datasource(DatasourceConfig(name="other", type="duckdb", database=source.database))
        await store_document(engine.storage, fixture_texts("retail")[1], data_source="other")
        try:
            native: Any = await engine.execute(query)
        except Exception as exc:  # noqa: BLE001 — whichever error native raises, strict raises it too
            native = exc
        if isinstance(native, Exception):
            with pytest.raises(type(native)):
                await strict(engine, query)
        else:
            assert (await strict(engine, query)).data == [{"total_revenue": 650}]
        assert (await strict(engine, query, data_source="other")).data == [{"total_revenue": 650}]


# --------------------------------------------------------------------------- #
# The typed error
# --------------------------------------------------------------------------- #
def test_the_seventeen_codes_in_order() -> None:
    assert [str(code) for code in getattr(errors, "OssieErrorCode")] == list(CODES)


async def test_a_strict_refusal_is_one_typed_error(tmp_path: Path) -> None:
    exc = await _code(tmp_path, "retail", {"metrics": ["orders.no_such_metric"]}, "E_NAME_NOT_FOUND")
    assert issubclass(type(exc), ossie_query_error())
    assert issubclass(type(exc), SlayerError)
    assert not issubclass(type(exc), QueryTypeError)
    assert str(exc).splitlines()[0].startswith("OssieQueryError: E_NAME_NOT_FOUND: ")
    assert exc.code == getattr(errors, "OssieErrorCode")("E_NAME_NOT_FOUND")
    assert (exc.dimension, exc.filter, exc.anchor) == (None, None, None)
