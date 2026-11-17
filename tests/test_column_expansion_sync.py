"""Unit tests for the synchronous derived-column expander
(``expand_derived_refs_sync``) used by the DEV-1450 planned-query generator.

(DEV-1485 deleted the async ``expand_derived_refs`` twin along with the legacy
enrichment path it served, so this is now the only expander.) These tests pin its
behavior directly against an in-memory ``name -> SlayerModel`` resolver,
mirroring how the generator drives it via ``bundle.get_referenced_model``.
"""

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import DatasetFieldCycleError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.sql.field_expansion import expand_derived_refs_sync


def _norm(sql: str) -> str:
    return " ".join(sql.split())


def _dataset_a() -> SlayerDataset:
    return SlayerDataset(
        name="A",
        data_source="ds",
        sql_table="A",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="bar", sql="bar", datatype=DataType.DOUBLE),
            DatasetField(name="b_id", sql="b_id", datatype=DataType.INT),
            DatasetField(name="raw_a", sql="raw_a", datatype=DataType.DOUBLE),
            # Local derived chain: c2 references sibling derived c1.
            DatasetField(name="c1", sql="raw_a + 1", datatype=DataType.DOUBLE),
            DatasetField(name="c2", sql="A.c1 * 2", datatype=DataType.DOUBLE),
            # Cross-model derived ref into B's derived column.
            DatasetField(name="ratio", sql="A.bar / B.foo_normalized", datatype=DataType.DOUBLE),
        ],
        relationships=[DatasetRelationship(to="B", join_pairs=[["b_id", "id"]])],
    )


def _dataset_b() -> SlayerDataset:
    return SlayerDataset(
        name="B",
        data_source="ds",
        sql_table="B",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="foo_raw", sql="foo_raw", datatype=DataType.DOUBLE),
            DatasetField(name="foo_normalized", sql="foo_raw / 100.0", datatype=DataType.DOUBLE),
        ],
    )




def test_bare_base_column_returns_name() -> None:
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql=None, dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert out is None


def test_local_derived_chain_inlines_sibling() -> None:
    """``c2 = A.c1 * 2`` with ``c1 = raw_a + 1`` inlines c1 parenthesised and
    qualifies the bare ``raw_a`` to the host alias."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="A.c1 * 2", dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == "(A.raw_a + 1) * 2"


def test_cross_table_derived_ref_inlines_joined_derived() -> None:
    """``A.bar / B.foo_normalized`` inlines B's derived ``foo_normalized``
    (qualified to the single-hop alias ``B``)."""
    a, b = _dataset_a(), _dataset_b()
    out = expand_derived_refs_sync(
        sql="A.bar / B.foo_normalized", dataset=a, alias_path="A",
        datasets_by_name={"A": a, "B": b}, dialect="sqlite",
    )
    assert _norm(out) == "A.bar / (B.foo_raw / 100.0)"


def test_bare_local_ref_qualifies_to_alias() -> None:
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="raw_a + 1", dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == "A.raw_a + 1"


def test_cycle_raises_column_cycle_error() -> None:
    cyclic = SlayerDataset(
        name="C",
        data_source="ds",
        sql_table="C",
        fields=[
            DatasetField(name="x", sql="x", datatype=DataType.DOUBLE),
            DatasetField(name="c1", sql="c2 + 1", datatype=DataType.DOUBLE),
            DatasetField(name="c2", sql="c1 - 1", datatype=DataType.DOUBLE),
        ],
    )
    with pytest.raises(DatasetFieldCycleError):
        expand_derived_refs_sync(
            sql="c1", dataset=cyclic, alias_path="C",
            datasets_by_name={"C": cyclic}, dialect="sqlite",
        )


def test_unknown_alias_left_untouched() -> None:
    """A ``<alias>.<col>`` whose alias is not a join target is left alone
    (likely a CTE / subquery alias the user wired up)."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="cte_x.value + A.bar", dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    # cte_x.value untouched; A.bar stays qualified to the host alias.
    assert "cte_x.value" in _norm(out)
    assert "A.bar" in _norm(out)


def test_dev1752_subquery_inner_ref_not_qualified() -> None:
    """DEV-1752: qualification is gated on root scope. The OUTER ``bar``
    qualifies to the host alias; the inner ``bar`` (a column of the subquery's
    own FROM) is left bare even though ``bar`` is also a model column."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="bar IN (SELECT bar FROM other_tbl)", dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == "A.bar IN (SELECT bar FROM other_tbl)"


def test_dev1752_root_inlines_while_subquery_local_untouched() -> None:
    """DEV-1752 over-gating guard (non-differential): the root ``c1`` still
    inlines to its definition; the inner ``c1`` is untouched. Non-differential
    because the buggy qualification branch never fired for a DERIVED inner
    column — only for trivial-base / non-model columns — so this guards against
    the fix over-gating root inlining, not the bug itself."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="c1 IN (SELECT c1 FROM other_tbl)", dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == "(A.raw_a + 1) IN (SELECT c1 FROM other_tbl)"


def test_dev1752_nonroot_explicit_ref_not_requalified() -> None:
    """DEV-1752 (differential, alias != model name): an inner ref explicitly
    qualified with the model name is left byte-for-byte untouched — it is NOT
    rewritten to the host alias. Proves non-root columns are untouched, not just
    coincidentally unchanged when alias == model name."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="bar IN (SELECT A.bar FROM other_tbl)", dataset=a, alias_path="A_host",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    # Outer bare bar -> host alias; inner A.bar stays A.bar (NOT A_host.bar).
    assert _norm(out) == "A_host.bar IN (SELECT A.bar FROM other_tbl)"


def test_dev1752_set_operation_branches_leave_inner_bare() -> None:
    """DEV-1752: both legs of a UNION are non-root scopes; their inner columns
    stay bare even though ``bar`` is a model column."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="bar IN (SELECT bar FROM t1 UNION SELECT bar FROM t2)",
        dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == "A.bar IN (SELECT bar FROM t1 UNION SELECT bar FROM t2)"


def test_dev1752_cte_body_leaves_inner_bare() -> None:
    """DEV-1752: a CTE (WITH) inside Mode-A is a non-root scope; refs in its
    body and its select stay bare."""
    a = _dataset_a()
    out = expand_derived_refs_sync(
        sql="bar IN (WITH c AS (SELECT bar FROM t) SELECT bar FROM c)",
        dataset=a, alias_path="A",
        datasets_by_name={"A": a}, dialect="sqlite",
    )
    assert _norm(out) == (
        "A.bar IN (WITH c AS (SELECT bar FROM t) SELECT bar FROM c)"
    )
