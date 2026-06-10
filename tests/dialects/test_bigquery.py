"""BigqueryDialect unit tests.

BigQuery is a Tier-1 dialect: it has logic (output-alias mangling for the
dotted alias convention), not just scalar config, so it lives in its own
file under ``slayer/sql/dialects/`` rather than the data-shaped
``_tier2.py`` bucket.

These tests exercise the dialect class in isolation. Full
``SQLGenerator``-surface tests (verifying the rewrite fires through the
generator dispatch) live in ``tests/test_sql_generator.py``.
"""

from __future__ import annotations

import re
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import (
    BigqueryDialect,
    SqlDialect,
    dialect_for_ds_type,
    get_dialect,
)
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Registry / scalar config
# ---------------------------------------------------------------------------


def test_registry_lookup_by_sqlglot_name() -> None:
    """``get_dialect("bigquery")`` returns a ``BigqueryDialect`` instance."""
    assert isinstance(get_dialect("bigquery"), BigqueryDialect)


def test_registry_lookup_by_ds_type() -> None:
    """``dialect_for_ds_type("bigquery")`` returns the same singleton."""
    assert isinstance(dialect_for_ds_type("bigquery"), BigqueryDialect)


def test_sqlglot_name() -> None:
    assert BigqueryDialect().sqlglot_name == "bigquery"


def test_ds_type_aliases() -> None:
    assert "bigquery" in BigqueryDialect().ds_type_aliases


def test_explain_prefix_is_none() -> None:
    """BigQuery has no SQL-level EXPLAIN; ``explain_prefix is None``
    signals ``build_explain_sql`` to raise."""
    assert BigqueryDialect().explain_prefix is None


def test_log_native_flags() -> None:
    d = BigqueryDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_build_explain_sql_raises() -> None:
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        BigqueryDialect().build_explain_sql("SELECT 1")


# ---------------------------------------------------------------------------
# rewrite_emitted_sql — write-side hook
# ---------------------------------------------------------------------------


def test_rewrite_emitted_sql_mangles_dotted_alias() -> None:
    """A single dot inside a backticked alias is mangled to ``___``."""
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders._count`"
    out = d.rewrite_emitted_sql(sql)
    assert "`orders___count`" in out
    # The dotted form must NOT appear in any backticked identifier.
    assert "`orders._count`" not in out


def test_rewrite_emitted_sql_multi_hop_alias() -> None:
    """Multi-hop aliases like ``orders.products.category`` are fully mangled."""
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders.products.category`"
    out = d.rewrite_emitted_sql(sql)
    assert "`orders___products___category`" in out


def test_rewrite_emitted_sql_leaves_non_dotted_backticks_untouched() -> None:
    """Backticked identifiers with no dot are not modified."""
    d = BigqueryDialect()
    sql = "SELECT col FROM `my_table`"
    assert d.rewrite_emitted_sql(sql) == sql


def test_rewrite_emitted_sql_leaves_segmented_fq_table_refs_untouched() -> None:
    """Hyphen-segmented BigQuery FQ paths (``\\`bigquery-public-data\\`.thelook.orders``)
    are safe — each segment is its own backticked identifier and the dots
    live OUTSIDE the backticks, so the regex never matches.

    Note: a fully-backticked dotted path of word-only segments (e.g.
    ``\\`my_dataset.my_table\\``) WOULD false-positive. Users writing
    ``Column.sql`` for BigQuery must backtick segments individually rather
    than wrap an entire dotted path in a single pair of backticks; see
    docstring on ``BigqueryDialect.rewrite_emitted_sql``.
    """
    d = BigqueryDialect()
    sql = "SELECT col FROM `bigquery-public-data`.thelook_ecommerce.orders"
    assert d.rewrite_emitted_sql(sql) == sql


def test_rewrite_emitted_sql_idempotent_on_already_mangled() -> None:
    """An already-mangled alias (no dots inside backticks) is left alone.

    The regex requires at least one ``.`` inside the backticked identifier,
    so ``___``-form aliases never match it. This pins the
    ``rewrite_emitted_sql`` being safe to invoke on its own output if a
    future path ever ends up double-applying.
    """
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders___count`"
    assert d.rewrite_emitted_sql(sql) == sql


# ---------------------------------------------------------------------------
# decode_result_keys — read-side hook
# ---------------------------------------------------------------------------


def test_decode_result_keys_reverses_mangle() -> None:
    """Mangled keys are decoded back to SLayer's dotted alias shape."""
    d = BigqueryDialect()
    rows = [{"orders___count": 42, "orders___products___category": "shoes"}]
    out = d.decode_result_keys(rows)
    assert out == [{"orders._count": 42, "orders.products.category": "shoes"}]


def test_decode_result_keys_empty_rows() -> None:
    """An empty input returns an empty list (cheap fast-path via
    comprehension)."""
    assert BigqueryDialect().decode_result_keys([]) == []


def test_decode_result_keys_no_dotted_keys_identity() -> None:
    """Keys that contain neither ``___`` nor a dot are passed through.

    Engine-side dedup-relevant invariant: a key that was never mangled by
    ``rewrite_emitted_sql`` round-trips unchanged.
    """
    d = BigqueryDialect()
    rows = [{"plain_col": 1, "another_col": "x"}]
    assert d.decode_result_keys(rows) == rows


# ---------------------------------------------------------------------------
# Round-trip bijection on SLayer's realistic alias space
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "original",
    [
        "orders._count",                # simple
        "orders.products.category",     # multi-hop
        "orders.my___metric",           # ___ in leaf (user-named measure)
        "a.b.c___d",                    # ___ mid-string
        "orders.customers.regions.population_sum",  # multi-hop cross-model
    ],
)
def test_round_trip_preserves_legitimate_underscores(original: str) -> None:
    """The encode/decode pair is a bijection on SLayer's actual alias
    space — every projection alias has at least one dot from the model
    prefix, so the encode is always non-trivial AND the decode reverses it
    exactly.

    Note on the closure: ``decode_result_keys`` is the inverse of
    ``rewrite_emitted_sql`` ONLY on the image of the latter. A hypothetical
    no-dot key containing ``___`` (e.g. a top-level alias literally named
    ``my___metric``) is undefined under decode and would be corrupted. This
    case cannot arise in SLayer's emitted SQL because every projection
    alias is model-qualified with a dot prefix.
    """
    d = BigqueryDialect()
    sql = f"SELECT 1 AS `{original}`"
    mangled = d.rewrite_emitted_sql(sql)
    m = re.search(r"AS `([^`]+)`", mangled)
    assert m is not None, f"could not find alias in mangled SQL: {mangled}"
    decoded = d.decode_result_keys([{m.group(1): 1}])
    assert decoded == [{original: 1}]


# ---------------------------------------------------------------------------
# Base class defaults must remain identity (regression-pin)
# ---------------------------------------------------------------------------


def test_base_default_rewrite_emitted_sql_is_identity() -> None:
    """``SqlDialect.rewrite_emitted_sql`` is identity. Pins that adding the
    hook on the base doesn't accidentally alter SQL for non-overriding
    dialects (Postgres, DuckDB, Sqlite, MySQL, ClickHouse, T-SQL, every
    Tier-2 dialect except BigQuery)."""
    assert SqlDialect().rewrite_emitted_sql('SELECT 1 AS "orders.count"') == 'SELECT 1 AS "orders.count"'


def test_base_default_decode_result_keys_is_identity() -> None:
    """``SqlDialect.decode_result_keys`` is identity. Pins the same
    invariant on the read side."""
    rows = [{"orders.count": 42, "orders.products.category": "shoes"}, {}]
    assert SqlDialect().decode_result_keys(rows) == rows


# ---------------------------------------------------------------------------
# Engine-level integration: SlayerResponse round-trip for the BigQuery dialect
# ---------------------------------------------------------------------------


class _FakeBigQueryClient:
    """Stub SQL client that returns BigQuery-mangled row keys.

    Used to exercise ``engine.execute()``'s post-fetch decode hook end-to-end
    without depending on a live BigQuery instance.
    """

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002 — stub signature
        return [dict(row) for row in self._rows]


async def _build_bigquery_engine(rows: list[dict]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Build an engine pointing at a fake BigQuery datasource whose SQL
    client is pre-stubbed with ``rows``.

    Returns ``(engine, tmpdir, datasource)`` — caller owns the tmpdir.
    """
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    ds = DatasourceConfig(
        name="bq",
        type="bigquery",
        database="proj.dataset",
    )
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="proj.dataset.orders_t",
        data_source="bq",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    engine._sql_clients[ds.get_connection_string()] = _FakeBigQueryClient(rows)
    return engine, tmp, ds


class TestEngineDecodeIntegration:
    """End-to-end: stub client returns mangled keys; engine decodes them
    before packaging into ``SlayerResponse``.

    Pins Codex MEDIUM #4 — engine-level response-shape coverage.
    """

    async def test_non_empty_rows_decoded_in_response(self) -> None:
        rows = [{"orders___count": 42, "orders___status": "paid"}]
        engine, tmp, _ = await _build_bigquery_engine(rows)
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )
            resp = await engine.execute(query)
            # Keys must be decoded back to dotted form on the response.
            assert resp.data == [{"orders._count": 42, "orders.status": "paid"}]
        finally:
            tmp.cleanup()

    async def test_empty_rows_response_falls_back_to_expected_columns(self) -> None:
        """When rows are empty, ``columns = expected_columns`` per the
        engine's response shape contract. Decode is a cheap identity on
        ``[]`` and must not regress this branch."""
        engine, tmp, _ = await _build_bigquery_engine(rows=[])
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )
            resp = await engine.execute(query)
            assert resp.data == []
            # Expected-columns fallback fires when rows is empty.
            assert "orders._count" in resp.columns
            assert "orders.status" in resp.columns
        finally:
            tmp.cleanup()
