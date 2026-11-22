"""The strict filter is an OSSIE_SQL_2026 expression: Ossie constructs SLayer implements are admitted, SLayer-only
constructs are ``E_INVALID_QUERY`` and keep answering under the default conformance.

Spec: queries/ossie-l3-binding › The filter is an Ossie expression. ``matched()`` has no surface syntax, so no case.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.keys import SCALAR_SPECS
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import DS, fixture_engine, rows, sql_engine, strict, strict_error

SQL = """
CREATE TABLE customers (customer_id VARCHAR PRIMARY KEY, region VARCHAR);
INSERT INTO customers VALUES ('C1', 'EAST'), ('C2', 'EAST'), ('C3', 'WEST');
CREATE TABLE orders (order_id VARCHAR PRIMARY KEY, customer_id VARCHAR, status VARCHAR, amount INTEGER,
                     order_date DATE, tags VARCHAR[], payload VARCHAR);
INSERT INTO orders VALUES
  ('O1', 'C1', 'complete', 100, DATE '2025-01-15', ['x', 'y'], '{"k": "a"}'),
  ('O2', 'C1', 'cancelled', 200, DATE '2025-02-10', ['y'], '{"k": "b"}'),
  ('O3', 'C2', 'complete', 50, DATE '2025-04-01', [], '{"k": "a"}'),
  ('O4', 'C3', NULL, 300, DATE '2024-12-31', ['x'], '{"k": "c"}');
"""


def _field(name: str, datatype: DataType, **extra: object) -> DatasetField:
    return DatasetField.model_validate({"name": name, "datatype": datatype, **extra})


DATASETS = [
    SlayerDataset(name="customers", sql_table="customers", data_source=DS, fields=[
        _field("customer_id", DataType.STRING, primary_key=True), _field("region", DataType.STRING),
    ]),
    SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[
            _field("order_id", DataType.STRING, primary_key=True), _field("customer_id", DataType.STRING),
            _field("status", DataType.STRING), _field("amount", DataType.INTEGER), _field("order_date", DataType.DATE),
            _field("tags", DataType.ARRAY, element_type=DataType.STRING), _field("payload", DataType.STRING),
        ],
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["customer_id"],
                                           cardinality=RelationshipCardinality.MANY_TO_ONE)],
    ),
]

#: Admitted filter → total revenue it keeps.
ADMITTED = {
    "orders.amount > 100": 500,
    "orders.amount BETWEEN 50 AND 200": 350,
    "orders.status IN ('complete', 'cancelled')": 350,
    "orders.status NOT IN ('complete')": 200,
    "orders.status LIKE 'comp%'": 150,
    "orders.status ILIKE 'COMP%'": 150,
    "orders.status IS NULL": 300,
    "orders.status IS NOT NULL": 350,
    "orders.status IS DISTINCT FROM 'complete'": 500,
    "orders.status IS NOT DISTINCT FROM 'complete'": 150,
    "CASE WHEN orders.amount > 150 THEN 1 ELSE 0 END = 1": 500,
    "orders.amount * 2 - 100 > 100": 500,
    "CAST(orders.amount AS DOUBLE) > 150": 500,
    "orders.order_date >= '2025-02-01'": 250,
    "orders.order_date < DATE '2025-01-01'": 300,
    "NOT orders.amount > 100 OR orders.status = 'cancelled'": 350,
    "customers.region = 'EAST'": 350,
}

#: Every scalar of the library → (a filter calling it, the total revenue native keeps).
SCALARS: dict[str, tuple[str, int]] = {
    "nullif": ("nullif(orders.status, 'cancelled') IS NULL", 500),
    "coalesce": ("coalesce(orders.status, 'none') = 'none'", 300),
    "ifnull": ("ifnull(orders.status, 'none') = 'none'", 300),
    "ln": ("ln(orders.amount) > 5", 500),
    "log10": ("log10(orders.amount) >= 2", 600),
    "log2": ("log2(orders.amount) > 7", 500),
    "log": ("log(orders.amount) > 0", 650),
    "exp": ("exp(orders.amount / 100.0) > 7", 500),
    "sqrt": ("sqrt(orders.amount) = 10", 100),
    "pow": ("pow(orders.amount, 2) > 30000", 500),
    "power": ("power(orders.amount, 2) < 20000", 150),
    "abs": ("abs(orders.amount - 300) < 1", 300),
    "floor": ("floor(orders.amount / 150.0) = 1", 200),
    "ceil": ("ceil(orders.amount / 150.0) = 1", 150),
    "ceiling": ("ceiling(orders.amount / 150.0) = 2", 500),
    "round": ("round(orders.amount / 120.0) = 1", 100),
    "sign": ("sign(orders.amount - 150) = 1", 500),
    "trunc": ("trunc(orders.amount / 120.0) = 0", 150),
    "mod": ("mod(orders.amount, 100) = 0", 600),
    "greatest": ("greatest(orders.amount, 150) = 150", 150),
    "least": ("least(orders.amount, 150) = 150", 500),
    "lower": ("lower(orders.status) = 'complete'", 150),
    "upper": ("upper(orders.status) = 'COMPLETE'", 150),
    "trim": ("trim(orders.status) = 'complete'", 150),
    "length": ("length(orders.status) = 9", 200),
    "ltrim": ("ltrim(orders.status) = 'complete'", 150),
    "rtrim": ("rtrim(orders.status) = 'complete'", 150),
    "replace": ("replace(orders.status, 'complete', 'done') = 'done'", 150),
    "substr": ("substr(orders.status, 1, 3) = 'can'", 200),
    "substring": ("substring(orders.status, 1, 4) = 'comp'", 150),
    "instr": ("instr(orders.status, 'cel') > 0", 200),
    "concat": ("concat(orders.status, '!') = 'complete!'", 150),
    "left": ("left(orders.status, 2) = 'ca'", 200),
    "right": ("right(orders.status, 2) = 'te'", 150),
    "starts_with": ("starts_with(orders.status, 'c')", 350),
    "ends_with": ("ends_with(orders.status, 'led')", 200),
    "contains": ("contains(orders.status, 'nce')", 200),
    "contains_any": ("contains_any(orders.tags, ['x'])", 400),
    "contains_all": ("contains_all(orders.tags, ['x', 'y'])", 100),
    "lpad": ("lpad(orders.status, 10, '*') = '**complete'", 150),
    "rpad": ("rpad(orders.status, 10, '*') = 'complete**'", 150),
    "reverse": ("reverse(orders.status) = 'etelpmoc'", 150),
    "repeat": ("repeat(orders.status, 2) = 'completecomplete'", 150),
    "split_part": ("split_part(orders.status, 'l', 1) = 'cance'", 200),
    "concat_ws": ("concat_ws('-', orders.status, 'x') = 'complete-x'", 150),
    "chr": ("chr(orders.amount - 1) = 'c'", 100),
    "ascii": ("ascii(orders.status) = 99", 350),
    "div": ("div(orders.amount, 100) = 2", 200),
    "pi": ("orders.amount > pi() * 50", 500),
    "sin": ("sin(orders.amount * 0) = 0", 650),
    "cos": ("cos(orders.amount * 0) = 1", 650),
    "tan": ("tan(orders.amount * 0) = 0", 650),
    "asin": ("asin(orders.amount / 1000.0) > 0.25", 300),
    "acos": ("acos(orders.amount / 1000.0) < 1.4", 500),
    "atan": ("atan(orders.amount / 100.0) > 1.2", 300),
    "atan2": ("atan2(orders.amount, 100) > 1.2", 300),
    "cbrt": ("cbrt(orders.amount) > 6", 300),
    "regexp_like": ("regexp_like(orders.status, '^can')", 200),
    "regexp_extract": ("regexp_extract(orders.status, '^(c[a-z])', 1) = 'co'", 150),
    "regexp_replace": ("regexp_replace(orders.status, 'e$', '') = 'complet'", 150),
    "json_value": ("json_value(orders.payload, '$.k') = 'a'", 150),
    "like": ("like(orders.status, 'comp%')", 150),
    "ilike": ("ilike(orders.status, 'COMP%')", 150),
    "iif": ("iif(orders.amount > 150, 1, 0) = 1", 500),
    "date_part": ("date_part('year', orders.order_date) = 2025", 350),
    "date_diff": ("date_diff('day', DATE '2025-01-01', orders.order_date) > 30", 250),
    "date_add": ("date_add(orders.order_date, 30, 'day') > DATE '2025-03-01'", 250),
    "interval": ("orders.order_date + interval(30, 'day') > DATE '2025-03-01'", 250),
    "current_date": ("orders.order_date < current_date()", 650),
    "now": ("orders.order_date < now()", 650),
}

#: SLayer-only filter → the total revenue native keeps (``None``: answers, value clock-dependent).
SLAYER_ONLY = {
    "orders.status ~ '^c'": 350,
    "contains(orders.tags, 'x')": 400,
    "contains_any(orders.tags, ['x'])": 400,
    "contains_all(orders.tags, ['x', 'y'])": 100,
    "orders.payload ->> 'k' = 'a'": 150,
    "orders.order_date >= '2025-Q1'": 350,
    "orders.order_date in '2025'": 350,
    "orders.order_date >= 'last month'": None,
    "orders.status == 'complete'": 150,
    "orders.status is None": 300,
    "orders.status IN ['complete']": 150,
}

#: Refused under strict, with no native answer to compare.
REFUSED_ONLY = [
    "cumsum(sum(orders.amount)) > 100",
    "orders.status = '{s}'",
]


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with sql_engine(tmp_path, sql=SQL, datasets=DATASETS) as engine:
        yield engine


def _query(filter_text: str) -> dict[str, object]:
    return {"metrics": ["orders.total_revenue"], "filter": filter_text}


@pytest.mark.parametrize("filter_text", sorted(ADMITTED))
async def test_ossie_constructs_are_admitted(engine: SlayerQueryEngine, filter_text: str) -> None:
    """Ossie constructs are admitted."""
    response = await strict(engine, _query(filter_text))
    assert rows(response) == [(float(ADMITTED[filter_text]),)]


def test_every_scalar_has_a_case() -> None:
    assert set(SCALARS) == set(SCALAR_SPECS)


@pytest.mark.parametrize("name", sorted(SCALARS))
async def test_native_answers_every_scalar_case(engine: SlayerQueryEngine, name: str) -> None:
    filter_text, expected = SCALARS[name]
    assert rows(await engine.execute(_query(filter_text)), "orders.total_revenue") == [(float(expected),)]


@pytest.mark.parametrize("name", sorted(SCALARS))
async def test_a_scalar_is_admitted_exactly_when_flagged_ossie(engine: SlayerQueryEngine, name: str) -> None:
    """Each scalar Ossie defines with SLayer's meaning is admitted; every other one is ``E_INVALID_QUERY``."""
    filter_text, expected = SCALARS[name]
    if SCALAR_SPECS[name].ossie:
        assert rows(await strict(engine, _query(filter_text))) == [(float(expected),)]
    else:
        await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")


@pytest.mark.parametrize("filter_text", sorted(SLAYER_ONLY))
async def test_slayer_only_constructs_are_refused(engine: SlayerQueryEngine, filter_text: str) -> None:
    """SLayer-only constructs are refused."""
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")
    native = await engine.execute(_query(filter_text))
    expected = SLAYER_ONLY[filter_text]
    if expected is not None:
        assert rows(native, "orders.total_revenue") == [(float(expected),)]


@pytest.mark.parametrize("filter_text", REFUSED_ONLY)
async def test_other_slayer_only_constructs_are_refused(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")


async def test_a_filter_without_fields_reaches_unconnected_metrics(tmp_path: Path) -> None:
    """A filter without fields reaches unconnected metrics."""
    async with fixture_engine(tmp_path, "retail") as engine:
        response = await strict(engine, {"metrics": ["orders.total_revenue", "tickets.ticket_count"],
                                         "filter": "FALSE"})
    assert response.data == [{"total_revenue": None, "ticket_count": 0}]
