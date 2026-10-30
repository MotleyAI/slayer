"""``SqlDialect.reflect_default_schema``: the one owner of the connection's default schema."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.bigquery import BigqueryDialect
from tests._engine_helpers import disposable_engine

pytest.importorskip("sqlalchemy_bigquery")

_BQ_URL = "bigquery://p/ds_a?user_supplied_client=true"
_BQ_URL_NO_DATASET = "bigquery://p?user_supplied_client=true"


class _Inspector:
    def __init__(self, default_schema_name: Any = None) -> None:
        self.default_schema_name = default_schema_name


class _RaisingInspector:
    @property
    def default_schema_name(self) -> str:
        raise RuntimeError("not supported")


def _probe_engine(value: Any) -> MagicMock:
    """An engine whose ``current_schema()`` probe returns ``value``."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (value,)
    return engine


def _probe_sql(engine: MagicMock) -> str:
    conn = engine.connect.return_value.__enter__.return_value
    return str(conn.execute.call_args.args[0])


class TestBaseHook:
    @pytest.mark.parametrize("inspector_value", ["public", " "])
    def test_inspector_value_wins_without_probing(self, inspector_value: str) -> None:
        engine = _probe_engine("probed")
        got = SqlDialect().reflect_default_schema(sa_engine=engine, inspector=_Inspector(inspector_value))
        assert got == inspector_value
        engine.connect.assert_not_called()

    @pytest.mark.parametrize("inspector_value", [None, "", 42, MagicMock()])
    def test_probe_runs_when_the_inspector_value_is_not_a_name(self, inspector_value: Any) -> None:
        engine = _probe_engine("probed")
        got = SqlDialect().reflect_default_schema(sa_engine=engine, inspector=_Inspector(inspector_value))
        assert got == "probed"
        assert "current_schema()" in _probe_sql(engine)

    def test_a_raising_inspector_falls_through_to_the_probe(self) -> None:
        engine = _probe_engine("probed")
        assert SqlDialect().reflect_default_schema(sa_engine=engine, inspector=_RaisingInspector()) == "probed"

    @pytest.mark.parametrize("probed", [None, "", 7])
    def test_a_non_name_probe_result_is_none(self, probed: Any) -> None:
        engine = _probe_engine(probed)
        assert SqlDialect().reflect_default_schema(sa_engine=engine, inspector=_Inspector(None)) is None

    def test_a_failing_probe_is_none(self) -> None:
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("no current_schema()")
        assert SqlDialect().reflect_default_schema(sa_engine=engine, inspector=_Inspector(None)) is None

    def test_no_engine_means_no_probe(self) -> None:
        assert SqlDialect().reflect_default_schema(sa_engine=None, inspector=_Inspector(None)) is None


@pytest.fixture
def bq_engine() -> Iterator[sa.Engine]:
    with disposable_engine(_BQ_URL) as engine:
        yield engine


class TestBigqueryHook:
    def test_returns_the_url_dataset_without_running_sql(self, bq_engine: sa.Engine) -> None:
        with patch.object(bq_engine, "connect", MagicMock(side_effect=AssertionError("ran SQL"))) as connect:
            got = BigqueryDialect().reflect_default_schema(sa_engine=bq_engine, inspector=_Inspector(None))
        assert got == "ds_a"
        connect.assert_not_called()

    def test_a_url_without_a_dataset_is_none(self) -> None:
        with (
            disposable_engine(_BQ_URL_NO_DATASET) as engine,
            patch.object(engine, "connect", MagicMock(side_effect=AssertionError("ran SQL"))) as connect,
        ):
            got = BigqueryDialect().reflect_default_schema(sa_engine=engine, inspector=_Inspector(None))
        assert got is None
        connect.assert_not_called()

    def test_schema_description_resolves_the_dataset_via_the_hook(self, bq_engine: sa.Engine) -> None:
        client = MagicMock()
        client.get_dataset.return_value.description = "Dataset description"
        conn = MagicMock()
        conn.__enter__.return_value.connection._client = client
        with (
            patch.object(bq_engine, "connect", return_value=conn),
            patch.object(BigqueryDialect, "reflect_default_schema", return_value="from_hook"),
        ):
            got = BigqueryDialect().reflect_schema_description(
                sa_engine=bq_engine, schema=None, configured_schema="configured",
            )
        assert got == "Dataset description"
        client.get_dataset.assert_called_once_with("from_hook")
