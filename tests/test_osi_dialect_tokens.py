"""One datasource-type ↔ Ossie-token ↔ reader map, shared by export and import."""

import pytest

from slayer.osi.dialects import Candidate, ossie_token, read, reader_for, render, select_expression
from slayer.osi.models import OSIExpression
from slayer.sql.dialects import _BY_DS_TYPE, dialect_for_ds_type

_DS_TYPES = sorted(_BY_DS_TYPE)


@pytest.mark.parametrize("ds_type", _DS_TYPES)
def test_token_reads_as_the_datasource_dialect(ds_type: str) -> None:
    assert reader_for(ossie_token(ds_type)) == dialect_for_ds_type(ds_type).sqlglot_name


@pytest.mark.parametrize("ds_type", _DS_TYPES)
def test_token_is_selected_as_target_native(ds_type: str) -> None:
    token = ossie_token(ds_type)
    expression = OSIExpression.model_validate({"dialects": [{"dialect": "MDX", "expression": "[x]"},
                                                            {"dialect": token, "expression": "amount*4"}]})
    selection = select_expression(expression, override=None, target=dialect_for_ds_type(ds_type).sqlglot_name)
    assert selection.candidate is not None
    assert selection.candidate.dialect == token
    assert selection.notes == []


@pytest.mark.parametrize("ds_type", _DS_TYPES)
def test_token_entry_is_kept_verbatim(ds_type: str) -> None:
    candidate = Candidate(dialect=ossie_token(ds_type), text="amount*4")
    target = dialect_for_ds_type(ds_type).sqlglot_name
    assert render(candidate, read(candidate), target=target) == "amount*4"


@pytest.mark.parametrize(("ds_type", "token"), [
    ("postgres", "POSTGRES"), ("postgresql", "POSTGRES"), ("duckdb", "DUCKDB"), ("sqlite", "SQLITE"),
    ("sqlserver", "TSQL"), ("mssql", "TSQL"), ("mariadb", "MYSQL"), ("athena", "PRESTO"),
])
def test_alias_tokens(ds_type: str, token: str) -> None:
    assert ossie_token(ds_type) == token
