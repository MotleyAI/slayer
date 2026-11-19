"""Shared fixtures and helpers for dialect-emission tests.

The SQLGenerator-surface tests and multi-dialect end-to-end
tests share helpers and an ``orders_model`` fixture. Consolidating them here
keeps each ``tests/dialects/test_*.py`` file focused on the dialect concern.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.generator import SQLGenerator

from tests._engine_helpers import _engine_generate


async def _noop_async(**kw):  # NOSONAR(S7503) — must remain async to match resolver-callback contract
    """Async no-op used as a resolver-callback fixture. Stays ``async`` so
    callers can ``await`` it through the resolver-callback contract; the
    body has no real awaitable work to do, so it just returns None."""
    return None


def _norm(s: str) -> str:
    return " ".join(s.split())


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    dataset: SlayerDataset,
) -> str:
    """Helper: run ``query`` against ``dataset`` on the typed engine pipeline
    and return the SQL emitted for ``generator``'s dialect.

    Kept as a thin shim over :func:`tests._engine_helpers._engine_generate`
    so the existing ``(generator, query, dataset)`` call shape survives the
    move off the legacy enrichment stack; SQL validity is asserted inside
    ``_engine_generate``.
    """
    return await _engine_generate(
        query=query, dataset=dataset, dialect=generator.dialect,
    )


@pytest.fixture
def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="quantity", sql="quantity", datatype=DataType.FLOAT),
        ],
    )
