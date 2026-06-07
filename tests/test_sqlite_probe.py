"""DEV-1538: SQLite affinity-probe helper unit tests.

The helper lives in :mod:`slayer.sql.sqlite_introspect` and probes the actual
storage classes of stored values in a SQLite column declared with INTEGER
affinity. It returns:

* ``DataType.INT`` when every sampled value is integer-shaped (and the sample
  is not saturated).
* ``DataType.DOUBLE`` when REAL values are present, or non-integral integers
  are present, or every TEXT value coerces to a finite ``float``.
* ``DataType.TEXT`` when BLOB values are present, or any TEXT value is
  non-coercible (or non-finite), or distinct-text saturates the coerce cap.
* ``None`` when the probe fails, OR when the row sample saturates AND the
  verdict would otherwise be INT (we can't certify INT past the cap).

These tests pin every leaf of the decision tree against a real in-memory
SQLite connection.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType


# ---------------------------------------------------------------------------
# Constants — DEV-1538 fixes the documented defaults so callers (validate_models,
# the load-time refinement, ingest) can trust them. Pin the defaults to catch
# accidental shrinkage.
# ---------------------------------------------------------------------------


def test_probe_scan_cap_default_is_100k():
    from slayer.sql.sqlite_introspect import PROBE_SCAN_CAP
    assert PROBE_SCAN_CAP == 100_000


def test_coerce_distinct_limit_default_is_1000():
    from slayer.sql.sqlite_introspect import COERCE_DISTINCT_LIMIT
    assert COERCE_DISTINCT_LIMIT == 1_000


# ---------------------------------------------------------------------------
# Fixtures — pure SQLAlchemy engines on file-backed SQLite (so ATTACH DATABASE
# tests can use a real path). Each fixture provides an open ``Connection``
# scoped to the test.
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    """A file-backed SQLite engine. File-backed so multi-schema ATTACH tests
    work; in-memory ``StaticPool`` works for single-connection cases but the
    ATTACH-DATABASE tests need a path."""
    db_path = tmp_path / "probe.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")
    yield engine
    engine.dispose()


@pytest.fixture
def conn(sqlite_engine):
    with sqlite_engine.connect() as c:
        yield c


def _quote(name: str) -> str:
    """Quote a SQLite identifier — doubles any embedded double-quote."""
    return '"' + name.replace('"', '""') + '"'


def _insert_typed(conn, table: str, column: str, values: list) -> None:
    """Insert each value individually so the SQLite storage class is preserved
    per row (executemany would normalize). Python ``int`` → INTEGER storage,
    ``float`` → REAL storage, ``str`` → TEXT (unless the column has INTEGER
    affinity AND the text is losslessly convertible to an integer/real —
    SQLite's affinity rules convert it on the way in), ``bytes`` → BLOB,
    ``None`` → NULL.
    """
    qt = _quote(table)
    qc = _quote(column)
    for v in values:
        conn.execute(
            sa.text(f'INSERT INTO {qt} ({qc}) VALUES (:v)'),
            {"v": v},
        )
    conn.commit()


# ===========================================================================
# Test #1 — all-integer storage → INT
# ===========================================================================


class TestProbeAllInteger:
    def test_all_integer_returns_int(self, conn) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [1, 2, 3, 4, 5])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.INT


# ===========================================================================
# Test #2 — mixed REAL/INTEGER storage (the vaccine case) → DOUBLE
# ===========================================================================


class TestProbeMixedReal:
    def test_mixed_real_returns_double(self, conn) -> None:
        """Reproduces the issue's vaccine case: declared INTEGER affinity,
        actual storage is mostly REAL with a few INTEGER."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE sensordata (tempstabidx INTEGER)'))
        # 3 integer rows + 897 real rows — the exact distribution from the
        # Linear issue.
        _insert_typed(conn, "sensordata", "tempstabidx", [1, 2, 3])
        _insert_typed(
            conn,
            "sensordata",
            "tempstabidx",
            [0.99, 0.943, 0.969] + [0.9 + i * 0.001 for i in range(894)],
        )
        verdict = probe_sqlite_integer_column(
            conn=conn, table="sensordata", column="tempstabidx"
        )
        assert verdict is DataType.DOUBLE


# ===========================================================================
# Test #3 (new) — INT + non-coercible TEXT → TEXT (text-first decision order)
# ===========================================================================


class TestProbeIntegerPlusText:
    def test_integer_plus_non_coercible_text_returns_text(self, conn) -> None:
        """The decision tree must check TEXT before REAL; mixed INT + TEXT
        where the TEXT is non-coercible classifies as TEXT regardless of
        n_real / n_non_integral counts (Codex finding #1)."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [1, 2, 3, "abc", "xyz"])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT


# ===========================================================================
# Test #4 — text-coerce widens to DOUBLE
# ===========================================================================


class TestProbeTextCoerce:
    def test_text_coerce_widens_to_double(self, conn) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        # All TEXT, all numeric-coercible to finite floats.
        _insert_typed(conn, "t", "v", ["1", "2.5", "1e3", "-7.2", "  4  "])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.DOUBLE


# ===========================================================================
# Test #5 + #5b — text-non-coerce widens to TEXT, including NaN/Inf/empty
# ===========================================================================


class TestProbeTextNonCoerce:
    def test_text_non_coerce_widens_to_text(self, conn) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", ["1", "abc"])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT

    @pytest.mark.parametrize(
        "bad_value",
        ["nan", "NaN", "inf", "-inf", "Infinity", "  ", ""],
    )
    def test_nan_inf_whitespace_empty_classified_as_text(
        self, conn, bad_value: str
    ) -> None:
        """``float("nan")`` succeeds but ``math.isfinite`` is False; empty
        strings and whitespace fail ``float()``. All these widen to TEXT."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", ["1", bad_value])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT


# ===========================================================================
# Test #6, #7 — empty / all-NULL → INT
# ===========================================================================


class TestProbeEmptyAndNull:
    def test_empty_table_returns_none(self, conn) -> None:
        """Empty column → no evidence either way. Return None so refinement
        callers don't narrow a persisted DOUBLE based on a non-observation;
        ingest callers fall back to the SA-derived type (INT for declared
        INTEGER affinity, so the final persisted type is still INT).
        """
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        conn.commit()
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is None

    def test_all_null_returns_none(self, conn) -> None:
        """All-NULL rows → no evidence either way. Same rationale as
        empty-table: None means "no info"."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [None, None, None])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is None


# ===========================================================================
# Test #8 — probe failure → None + WARNING
# ===========================================================================


class TestProbeFailure:
    def test_probe_failure_returns_none_and_warns(self, conn, caplog) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        # Probe a nonexistent table. SQLAlchemy raises OperationalError.
        with caplog.at_level(logging.WARNING):
            verdict = probe_sqlite_integer_column(
                conn=conn, table="missing_table", column="v"
            )
        assert verdict is None
        # At least one WARNING about probe failure.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("probe" in r.getMessage().lower() for r in warnings)


# ===========================================================================
# Test #9 — identifier quoting safety
# ===========================================================================


class TestProbeIdentifierQuoting:
    def test_column_with_special_chars(self, conn) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        # Column name with a space and an embedded double-quote.
        conn.execute(sa.text('CREATE TABLE t ("weird ""name""" INTEGER)'))
        _insert_typed(conn, "t", 'weird "name"', [1, 2, 3])
        verdict = probe_sqlite_integer_column(
            conn=conn, table="t", column='weird "name"'
        )
        assert verdict is DataType.INT

    def test_table_with_special_chars(self, sqlite_engine) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        with sqlite_engine.connect() as conn:
            conn.execute(sa.text('CREATE TABLE "weird table" (v INTEGER)'))
            _insert_typed(conn, "weird table", "v", [1, 2, 3])
            verdict = probe_sqlite_integer_column(
                conn=conn, table="weird table", column="v"
            )
            assert verdict is DataType.INT


# ===========================================================================
# Test #10 — LIMIT caps the row scan
# ===========================================================================


class TestProbeLimitCap:
    def test_limit_caps_scan(self, conn) -> None:
        """The probe's row-scan SQL must include LIMIT ``PROBE_SCAN_CAP + 1``
        so saturation can be detected."""
        from slayer.sql import sqlite_introspect

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [1, 2, 3])

        # Wrap conn.execute so we can capture the SQL strings it sees.
        seen_sql: list[str] = []
        original_execute = conn.execute

        def _spy(statement, *args, **kwargs):
            try:
                seen_sql.append(str(statement))
            except Exception:
                pass
            return original_execute(statement, *args, **kwargs)

        conn.execute = _spy  # type: ignore[method-assign]
        try:
            sqlite_introspect.probe_sqlite_integer_column(
                conn=conn, table="t", column="v"
            )
        finally:
            conn.execute = original_execute  # type: ignore[method-assign]

        cap_plus_one = sqlite_introspect.PROBE_SCAN_CAP + 1
        assert any(f"LIMIT {cap_plus_one}" in s for s in seen_sql), seen_sql


# ===========================================================================
# Test #10b, #10c — BLOB handling
# ===========================================================================


class TestProbeBlob:
    def test_blob_only_returns_text(self, conn) -> None:
        """BLOBs can't be safely cast numerically; widen to TEXT (Codex #2)."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [b"\x00\x01", b"\x02\x03"])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT

    def test_blob_plus_integer_returns_text(self, conn) -> None:
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [1, 2, 3, b"\x00\x01"])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT


# ===========================================================================
# Test #10d, #10e — sample saturation
# ===========================================================================


class TestProbeSampleSaturation:
    def test_sample_saturated_all_int_returns_none(self, conn, caplog) -> None:
        """When the sample exhausts at PROBE_SCAN_CAP + 1 and would otherwise
        verdict INT, return None so the caller keeps declared INT but logs."""
        from slayer.sql import sqlite_introspect

        # Tiny override for test speed: monkeypatch the cap to 50.
        original_cap = sqlite_introspect.PROBE_SCAN_CAP
        sqlite_introspect.PROBE_SCAN_CAP = 50
        try:
            conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
            _insert_typed(conn, "t", "v", [i for i in range(60)])
            with caplog.at_level(logging.WARNING):
                verdict = sqlite_introspect.probe_sqlite_integer_column(
                    conn=conn, table="t", column="v"
                )
            assert verdict is None
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert any("saturat" in r.getMessage().lower() for r in warnings)
        finally:
            sqlite_introspect.PROBE_SCAN_CAP = original_cap

    def test_sample_saturated_with_real_still_returns_double(
        self, conn
    ) -> None:
        """When the sample saturates but we already saw REAL values in the
        sample, we have enough evidence for DOUBLE — saturation doesn't
        downgrade the verdict back to None."""
        from slayer.sql import sqlite_introspect

        original_cap = sqlite_introspect.PROBE_SCAN_CAP
        sqlite_introspect.PROBE_SCAN_CAP = 50
        try:
            conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
            # First row REAL, rest INT — sample saturates but n_real > 0.
            _insert_typed(conn, "t", "v", [0.5])
            _insert_typed(conn, "t", "v", [i for i in range(60)])
            verdict = sqlite_introspect.probe_sqlite_integer_column(
                conn=conn, table="t", column="v"
            )
            assert verdict is DataType.DOUBLE
        finally:
            sqlite_introspect.PROBE_SCAN_CAP = original_cap


# ===========================================================================
# Test #10f — distinct-text coerce probe saturation
# ===========================================================================


class TestProbeDistinctTextSaturation:
    def test_distinct_text_saturated_returns_text(self, conn) -> None:
        """When the distinct-text coerce probe saturates, we can't prove
        every distinct value coerces — widen to TEXT conservatively.

        Implementation note: SQLite's INTEGER-affinity coerces any
        losslessly-numeric text on INSERT to INTEGER or REAL storage. To
        keep text values stored AS TEXT for this test, use a BLOB-affinity
        column (any unrecognized type name yields BLOB affinity, which
        preserves storage classes verbatim).
        """
        from slayer.sql import sqlite_introspect

        original = sqlite_introspect.COERCE_DISTINCT_LIMIT
        sqlite_introspect.COERCE_DISTINCT_LIMIT = 10
        try:
            # BLOB affinity (via no declared type) preserves the incoming
            # Python type as the storage class.
            conn.execute(sa.text('CREATE TABLE t (v)'))
            # 20 distinct numeric text values; cap is 10, saturation hits.
            _insert_typed(
                conn, "t", "v",
                [str(i) for i in range(20)],
            )
            verdict = sqlite_introspect.probe_sqlite_integer_column(
                conn=conn, table="t", column="v"
            )
            assert verdict is DataType.TEXT
        finally:
            sqlite_introspect.COERCE_DISTINCT_LIMIT = original

    def test_many_duplicate_low_distinct_text_returns_double(self, conn) -> None:
        """A column with many duplicate numeric-text values but very few
        DISTINCT values must NOT trigger saturation — distinct-text count
        is the trigger, not row count. Verdict: DOUBLE.

        Same BLOB-affinity trick as the saturated test, so text storage
        is preserved verbatim.
        """
        from slayer.sql import sqlite_introspect

        original = sqlite_introspect.COERCE_DISTINCT_LIMIT
        sqlite_introspect.COERCE_DISTINCT_LIMIT = 10
        try:
            conn.execute(sa.text('CREATE TABLE t (v)'))
            # 100 rows, only 3 distinct values (well under the cap of 10).
            _insert_typed(
                conn, "t", "v",
                ["1"] * 40 + ["2.5"] * 40 + ["7"] * 20,
            )
            verdict = sqlite_introspect.probe_sqlite_integer_column(
                conn=conn, table="t", column="v"
            )
            assert verdict is DataType.DOUBLE
        finally:
            sqlite_introspect.COERCE_DISTINCT_LIMIT = original


class TestProbeCoerceQueryFailure:
    def test_coerce_query_failure_returns_none_and_warns(self, conn, caplog) -> None:
        """If the main probe succeeds (column exists, n_text > 0) but the
        follow-up distinct-text query raises, the probe returns None and
        logs one WARNING — distinct from the main-probe-failure path."""
        from slayer.sql import sqlite_introspect

        # BLOB-affinity column (no declared type) preserves storage class
        # so the text values stay TEXT and trigger the coerce branch.
        conn.execute(sa.text('CREATE TABLE t (v)'))
        _insert_typed(conn, "t", "v", ["1", "2", "3"])

        original = sqlite_introspect.probe_sqlite_integer_column
        # Intercept conn.execute and raise on the SECOND invocation (the
        # main probe is call #1, the distinct-text coerce probe is call #2).
        original_execute = conn.execute
        call_count = {"n": 0}

        def _spy(statement, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise sa.exc.OperationalError("coerce-probe boom", None, Exception("simulated"))
            return original_execute(statement, *args, **kwargs)

        conn.execute = _spy  # type: ignore[method-assign]
        try:
            with caplog.at_level(logging.WARNING):
                verdict = original(conn=conn, table="t", column="v")
        finally:
            conn.execute = original_execute  # type: ignore[method-assign]

        assert verdict is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("probe" in r.getMessage().lower() for r in warnings)


# ===========================================================================
# Test #10g — schema-qualified table (ATTACH DATABASE)
# ===========================================================================


class TestProbeSchemaQualified:
    def test_attached_database_schema(self, tmp_path: Path) -> None:
        """SQLite supports ATTACH DATABASE to bind a second file under a
        non-``main`` schema. The probe should accept ``schema=`` and route to
        the right ATTACHed DB."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        primary = tmp_path / "primary.db"
        attached = tmp_path / "attached.db"

        # Create the attached DB with a table.
        att_conn = sqlite3.connect(str(attached))
        att_conn.execute("CREATE TABLE other_t (v INTEGER)")
        att_conn.executemany("INSERT INTO other_t VALUES (?)", [(1,), (2,), (3,)])
        att_conn.commit()
        att_conn.close()

        engine = sa.create_engine(f"sqlite:///{primary}")
        try:
            with engine.connect() as conn:
                conn.execute(sa.text(f"ATTACH DATABASE '{attached}' AS aux"))
                verdict = probe_sqlite_integer_column(
                    conn=conn, table="other_t", column="v", schema="aux"
                )
                assert verdict is DataType.INT
        finally:
            engine.dispose()


# ===========================================================================
# Test #10i — REAL + non-coercible TEXT → TEXT (Codex finding #1)
# ===========================================================================


class TestProbeRealPlusNonCoercibleText:
    def test_real_plus_non_coercible_text_returns_text(self, conn) -> None:
        """The decision tree must inspect TEXT before returning DOUBLE on
        n_real > 0. A column with REAL plus non-coercible TEXT widens to
        TEXT, never DOUBLE — numeric aggregation over non-numeric strings
        would silently corrupt results."""
        from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

        conn.execute(sa.text('CREATE TABLE t (v INTEGER)'))
        _insert_typed(conn, "t", "v", [0.5, 0.7, "N/A"])
        verdict = probe_sqlite_integer_column(conn=conn, table="t", column="v")
        assert verdict is DataType.TEXT
