"""OSI document models (slayer/osi/models.py) against the current Ossie core spec."""

import pytest
from pydantic import ValidationError

from slayer.osi.models import OSIDataset, OSIDialectExpression, OSIDocument, OSIExpression, OSIField, OSIMetric
from tests._osi_helpers import ds, f, flat, m


def test_expression_with_only_sql() -> None:
    expression = OSIExpression.model_validate({"sql": "amount"})
    assert expression.sql == "amount"
    assert not expression.dialects


def test_expression_with_only_dialects() -> None:
    expression = OSIExpression.model_validate({"dialects": [{"dialect": "ANSI_SQL", "expression": "amount"}]})
    assert expression.sql is None
    assert expression.dialects is not None
    assert [(d.dialect, d.expression) for d in expression.dialects] == [("ANSI_SQL", "amount")]


def test_expression_with_sql_and_dialects() -> None:
    expression = OSIExpression.model_validate(
        {"sql": "amount", "dialects": [{"dialect": "SNOWFLAKE", "expression": "amount::NUMBER"}]})
    assert expression.sql == "amount"
    assert expression.dialects is not None
    assert expression.dialects[0].dialect == "SNOWFLAKE"


def test_expression_with_neither_form_is_accepted() -> None:
    expression = OSIExpression.model_validate({})
    assert expression.sql is None
    assert not expression.dialects


@pytest.mark.parametrize("token", ["OSSIE_SQL_2026", "ANSI_SQL", "BIGQUERY", "DB_DIALECT", "dax", "Postgres"])
def test_open_dialect_token(token: str) -> None:
    assert OSIDialectExpression.model_validate({"dialect": token, "expression": "x"}).dialect == token


@pytest.mark.parametrize("token", ["", "   "])
def test_blank_dialect_token_rejected(token: str) -> None:
    with pytest.raises(ValidationError):
        OSIDialectExpression.model_validate({"dialect": token, "expression": "x"})


def test_flat_document_fields() -> None:
    doc = OSIDocument.model_validate({
        **flat([ds("orders", fields=[f("amount")])],
               relationships=[], metrics=[m("total", "SUM(orders.amount)")]),
        "description": "d", "ai_context": "ctx",
        "custom_extensions": [{"vendor_name": "COMMON", "data": "{}"}],
    })
    assert doc.name == "shop"
    assert doc.version == "0.2.0.dev0"
    assert doc.description == "d"
    assert doc.ai_context == "ctx"
    assert [d.name for d in doc.datasets] == ["orders"]
    assert [x.name for x in doc.metrics or []] == ["total"]
    assert doc.custom_extensions is not None
    assert doc.legacy is False


def test_dataset_scoped_metrics_and_datatypes() -> None:
    dataset = OSIDataset.model_validate(ds(
        "orders", fields=[f("amount", datatype="Decimal")],
        metrics=[m("total_amount", "SUM(amount)", datatype="Decimal")]))
    assert dataset.fields is not None
    assert dataset.fields[0].datatype == "Decimal"
    assert dataset.metrics is not None
    assert [(x.name, x.datatype) for x in dataset.metrics] == [("total_amount", "Decimal")]


@pytest.mark.parametrize("datatype", ["String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time",
                                      "DateTime", "DateTimeTz", "Opaque"])
def test_every_spec_datatype_parses(datatype: str) -> None:
    assert OSIField.model_validate(f("x", datatype=datatype)).datatype == datatype
    assert OSIMetric.model_validate(m("x", "COUNT(*)", datatype=datatype)).datatype == datatype


def test_empty_keys_are_kept_distinct_from_absent() -> None:
    empty = OSIDataset.model_validate(ds("orders", primary_key=[], unique_keys=[[], ["email"]]))
    assert empty.primary_key == []
    assert empty.unique_keys == [[], ["email"]]
    absent = OSIDataset.model_validate(ds("orders"))
    assert absent.primary_key is None
    assert absent.unique_keys is None
