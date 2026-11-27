"""A boolean ``last`` over an aggregate stays BOOLEAN on Postgres (spec: queries/semantics › first and last
over an aggregate rank their operand's cells — Boolean typing is preserved)."""

from __future__ import annotations

import pytest

from tests import _dev2046_fixtures as f2046
from tests._pg_helpers import pg_database, pg_query_engine
from tests.test_dev2436_ranked_keys_and_supply import BOOL_CASES, _bool_months, _flag_query

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)


@pytest.mark.parametrize("case", list(BOOL_CASES))
async def test_boolean_typing_on_postgres(postgresql_proc, tmp_path, case: str) -> None:
    kw, want = BOOL_CASES[case]
    with pg_database(postgresql_proc, statements=f2046.seed_statements("postgres"), name="test") as ds:
        async with pg_query_engine(ds, base_dir=str(tmp_path / "store"), datasets=f2046.dev2046_datasets(),
                                   validate=False) as eng:
            resp = await eng.execute(_flag_query(kw))
    assert _bool_months(resp) == want
    assert all(isinstance(r["v"], bool) for r in resp.data)
