"""The async connection URL keeps an async-capable driver the user named and swaps only plain / default-sync URLs."""

from __future__ import annotations

import pytest
from sqlalchemy.engine.url import make_url

from slayer.sql.client import _async_connection_string

_URL_TAIL = "u:p%40ss@h:5432/d?sslmode=require"


@pytest.mark.parametrize(
    "db_type,connection_string",
    [
        ("postgres", f"postgresql+psycopg://{_URL_TAIL}"),
        ("postgresql", f"postgresql+psycopg://{_URL_TAIL}"),
        ("postgres", f"postgresql+asyncpg://{_URL_TAIL}"),
        ("mysql", f"mysql+aiomysql://{_URL_TAIL}"),
        ("mysql", f"mysql+asyncmy://{_URL_TAIL}"),
        ("mariadb", f"mysql+asyncmy://{_URL_TAIL}"),
    ],
)
def test_async_capable_driver_kept_unchanged(db_type, connection_string) -> None:
    assert _async_connection_string(
        connection_string=connection_string, db_type=db_type,
    ) == connection_string


@pytest.mark.parametrize(
    "db_type,connection_string,expected",
    [
        ("postgres", f"postgresql://{_URL_TAIL}", f"postgresql+asyncpg://{_URL_TAIL}"),
        ("postgresql", f"postgresql+psycopg2://{_URL_TAIL}", f"postgresql+asyncpg://{_URL_TAIL}"),
        ("mysql", f"mysql://{_URL_TAIL}", f"mysql+aiomysql://{_URL_TAIL}"),
        ("mysql", f"mysql+pymysql://{_URL_TAIL}", f"mysql+aiomysql://{_URL_TAIL}"),
        ("mariadb", f"mysql+pymysql://{_URL_TAIL}", f"mysql+aiomysql://{_URL_TAIL}"),
    ],
)
def test_plain_or_default_sync_url_moves_to_async_driver(db_type, connection_string, expected) -> None:
    assert _async_connection_string(
        connection_string=connection_string, db_type=db_type,
    ) == expected


def test_swap_preserves_every_other_url_part() -> None:
    cs = "postgresql+psycopg2://us%3Aer:p%40ss@db.example:6543/warehouse?sslmode=require&application_name=x"
    out = _async_connection_string(connection_string=cs, db_type="postgres")
    assert out is not None
    assert make_url(out) == make_url(cs).set(drivername="postgresql+asyncpg")


@pytest.mark.parametrize(
    "db_type,connection_string",
    [
        ("postgres", f"postgresql+pg8000://{_URL_TAIL}"),
        ("mysql", f"mysql+mysqldb://{_URL_TAIL}"),
        ("postgres", f"redshift+redshift_connector://{_URL_TAIL}"),
        ("snowflake", "snowflake://u:p@acct/db"),
        ("foo", f"postgresql://{_URL_TAIL}"),
    ],
)
def test_no_async_url(db_type, connection_string) -> None:
    assert _async_connection_string(connection_string=connection_string, db_type=db_type) is None


@pytest.mark.parametrize(
    "url,is_async",
    [
        ("postgresql+psycopg://", True),
        ("postgresql+asyncpg://", True),
        ("mysql+aiomysql://", True),
        ("mysql+asyncmy://", True),
        ("postgresql://", False),
        ("postgresql+psycopg2://", False),
        ("postgresql+pg8000://", False),
        ("mysql://", False),
        ("mysql+pymysql://", False),
        ("mysql+mysqldb://", False),
    ],
)
def test_sqlalchemy_async_capability_pin(url, is_async) -> None:
    """Pins the underscore ``get_dialect(_is_async=...)`` keyword the async-URL rule relies on."""
    assert make_url(url).get_dialect(_is_async=True).is_async is is_async
