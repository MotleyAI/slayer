"""Tests for DatasourceConfig.postgres_schema (DEV-1594).

``postgres_schema`` controls the schema name the Postgres facade advertises a
datasource's models under. It is distinct from ``schema_name`` (the upstream
physical source schema) and defaults to ``None`` (=> "public").
"""

from __future__ import annotations

import pytest

from slayer.core.models import DatasourceConfig


def test_postgres_schema_defaults_to_none() -> None:
    assert DatasourceConfig(name="sales").postgres_schema is None


def test_postgres_schema_absent_on_old_v2_dict() -> None:
    # Configs persisted before the field existed still load.
    ds = DatasourceConfig.model_validate({"name": "sales", "version": 2})
    assert ds.postgres_schema is None


def test_postgres_schema_valid_identifier_round_trips() -> None:
    ds = DatasourceConfig(name="sales", postgres_schema="marketing")
    assert ds.postgres_schema == "marketing"
    assert DatasourceConfig.model_validate(ds.model_dump()).postgres_schema == "marketing"


def test_postgres_schema_independent_of_schema_name() -> None:
    ds = DatasourceConfig(name="sf", schema_name="RAW", postgres_schema="raw_facade")
    assert ds.schema_name == "RAW"
    assert ds.postgres_schema == "raw_facade"


@pytest.mark.parametrize(
    "bad",
    ["Marketing", "1bad", "has space", "pg-dash", "a.b", "a:b", ""],
)
def test_postgres_schema_rejects_non_lowercase_identifier(bad: str) -> None:
    with pytest.raises(ValueError):
        DatasourceConfig(name="x", postgres_schema=bad)
