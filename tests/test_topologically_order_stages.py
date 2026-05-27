"""DEV-1452 Stage B — module-level ``topologically_order_stages`` helper.

Extracted from ``SlayerQueryEngine._topologically_order_queries`` so the
migrated ``_expand_query_backed_model`` / ``_validate_and_populate_cache``
can call the same Kahn topo-sort the runtime-list ``execute`` path uses.

The helper is the SAME logic the engine-method shim delegates to (decision
#1 of the Stage B plan). Tests pin:

* Public surface (``topologically_order_stages`` lives at
  ``slayer.engine.stage_ordering``).
* Engine-method shim still works at
  ``SlayerQueryEngine._topologically_order_queries(queries)``.
* Decision E: inline-nested ``SlayerModel.source_queries`` contribute to
  the sibling dependency edges (recursive ``_extract_sibling_refs``
  extension).
"""
from __future__ import annotations

import pytest

from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery


# ---------------------------------------------------------------------------
# Surface
# ---------------------------------------------------------------------------


def test_module_surface_exists() -> None:
    """``slayer.engine.stage_ordering.topologically_order_stages`` is a
    public callable. Stage B is the first commit to introduce the helper;
    this import is the canary that lets the rest of the migration land.
    """
    from slayer.engine.stage_ordering import topologically_order_stages  # noqa: F401

    assert callable(topologically_order_stages)


def test_engine_shim_delegates() -> None:
    """``SlayerQueryEngine._topologically_order_queries`` keeps the
    classmethod surface that ``execute(query=list[...])`` already calls;
    the body just delegates to the new module-level helper.
    """
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery(
        name="b",
        source_model={
            "source_name": "orders",
            "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
        },
    )
    root = SlayerQuery(source_model="b")
    queries = [b, a, root]

    expected = topologically_order_stages(queries)
    via_shim = SlayerQueryEngine._topologically_order_queries(queries)
    assert [q.name for q in expected] == [q.name for q in via_shim]
    assert expected[-1] is root  # root stays last


# ---------------------------------------------------------------------------
# Basic semantics (mirror engine-method tests already pinned via
# tests/test_query_backed_models.py runtime-list cases)
# ---------------------------------------------------------------------------


def test_reorders_simple_forward_reference() -> None:
    from slayer.engine.stage_ordering import topologically_order_stages

    # b depends on a via joins.target_model; input order is [b, a, root].
    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery(
        name="b",
        source_model={
            "source_name": "orders",
            "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
        },
    )
    root = SlayerQuery(source_model="b")
    ordered = topologically_order_stages([b, a, root])
    assert [q.name for q in ordered] == ["a", "b", None]


def test_cycle_raises() -> None:
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(
        name="a",
        source_model={
            "source_name": "orders",
            "joins": [{"target_model": "b", "join_pairs": [["id", "id"]]}],
        },
    )
    b = SlayerQuery(
        name="b",
        source_model={
            "source_name": "orders",
            "joins": [{"target_model": "a", "join_pairs": [["id", "id"]]}],
        },
    )
    root = SlayerQuery(source_model="a")
    with pytest.raises(ValueError, match=r"[Cc]ycle"):
        topologically_order_stages([a, b, root])


def test_root_referenced_raises() -> None:
    """The final entry is the DAG root / sink and must not be referenced
    by any other stage (a stored convention; surfaces a clear error).
    """
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="root_stage")
    root = SlayerQuery(name="root_stage", source_model="orders")
    with pytest.raises(ValueError, match="root"):
        topologically_order_stages([a, root])


def test_self_reference_raises() -> None:
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="a")
    root = SlayerQuery(source_model="a")
    with pytest.raises(ValueError, match="self"):
        topologically_order_stages([a, root])


def test_duplicate_name_raises() -> None:
    from slayer.engine.stage_ordering import topologically_order_stages

    a1 = SlayerQuery(name="a", source_model="orders")
    a2 = SlayerQuery(name="a", source_model="orders")
    root = SlayerQuery(source_model="a")
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        topologically_order_stages([a1, a2, root])


# ---------------------------------------------------------------------------
# Decision E — inline-nested SlayerModel.source_queries contribute edges
# ---------------------------------------------------------------------------


def test_inline_nested_slayer_model_source_queries_contribute_to_edges() -> None:
    """A stage whose ``source_model`` is an inline ``SlayerModel`` carrying
    its own ``source_queries`` referencing sibling ``A`` by name MUST cause
    the outer topo-sort to place ``A`` before the enclosing stage.
    """
    from slayer.engine.stage_ordering import topologically_order_stages

    inner = SlayerModel(
        name="inline_qb",
        source_queries=[
            SlayerQuery(source_model="a"),  # references sibling "a"
        ],
    )
    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery(name="b", source_model=inner)
    root = SlayerQuery(source_model="b")
    ordered = topologically_order_stages([b, a, root])
    names = [q.name for q in ordered]
    assert names.index("a") < names.index("b"), names


def test_inline_nested_dict_form_contributes_to_edges() -> None:
    """Same as above with the inline model expressed as a dict literal."""
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery.model_validate({
        "name": "b",
        "source_model": {
            # Inline SlayerModel-as-dict: presence of ``source_queries``
            # but NOT ``source_name`` => inline SlayerModel form.
            "name": "inline_qb",
            "source_queries": [
                {"source_model": "a"},
            ],
        },
    })
    root = SlayerQuery(source_model="b")
    ordered = topologically_order_stages([b, a, root])
    names = [q.name for q in ordered]
    assert names.index("a") < names.index("b"), names


def test_typed_modelextension_nested_join_contributes_to_edges() -> None:
    """Typed ``ModelExtension`` with nested ``joins[].target_model``
    referencing a sibling adds an edge.
    """
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery(
        name="b",
        source_model=ModelExtension(
            source_name="orders",
            joins=[ModelJoin(
                target_model="a",
                join_pairs=[["id", "id"]],
            )],
        ),
    )
    root = SlayerQuery(source_model="b")
    ordered = topologically_order_stages([b, a, root])
    names = [q.name for q in ordered]
    assert names.index("a") < names.index("b"), names


def test_dict_modelextension_nested_join_contributes_to_edges() -> None:
    """``ModelExtension`` expressed as a raw dict (``{"source_name": ...,
    "joins": [...]}``) — same edge contribution as the typed shape.
    """
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(name="a", source_model="orders")
    b = SlayerQuery.model_validate({
        "name": "b",
        "source_model": {
            "source_name": "orders",
            "joins": [{
                "target_model": "a",
                "join_pairs": [["id", "id"]],
            }],
        },
    })
    root = SlayerQuery(source_model="b")
    ordered = topologically_order_stages([b, a, root])
    names = [q.name for q in ordered]
    assert names.index("a") < names.index("b"), names


def test_cycle_via_inline_nested_reference_raises() -> None:
    """A reference cycle that runs through an inline-nested stage's own
    ``source_queries`` must be detected.
    """
    from slayer.engine.stage_ordering import topologically_order_stages

    a = SlayerQuery(
        name="a",
        source_model=SlayerModel(
            name="inline_a",
            source_queries=[SlayerQuery(source_model="b")],
        ),
    )
    b = SlayerQuery(name="b", source_model="a")
    root = SlayerQuery(source_model="a")
    with pytest.raises(ValueError, match=r"[Cc]ycle"):
        topologically_order_stages([a, b, root])
