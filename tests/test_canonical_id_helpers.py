"""Unit tests for ``canonical_id_rooted_at`` (DEV-1409).

The helper drives the new ``datasource`` filter on ``search`` — it answers
"does this canonical_id belong to the given datasource?" under the
dotted-namespace rule (same rule the DEV-1405 cascade-delete uses).
"""

from __future__ import annotations

import pytest

from slayer.memories.resolver import canonical_id_rooted_at


class TestCanonicalIdRootedAt:
    def test_datasource_doc_matches_itself(self) -> None:
        assert canonical_id_rooted_at(
            canonical_id="prod", datasource="prod",
        ) is True

    def test_model_under_datasource_matches(self) -> None:
        assert canonical_id_rooted_at(
            canonical_id="prod.orders", datasource="prod",
        ) is True

    def test_leaf_under_datasource_matches(self) -> None:
        assert canonical_id_rooted_at(
            canonical_id="prod.orders.id", datasource="prod",
        ) is True

    def test_unrelated_datasource_doc_does_not_match(self) -> None:
        assert canonical_id_rooted_at(
            canonical_id="staging", datasource="prod",
        ) is False

    def test_unrelated_model_does_not_match(self) -> None:
        assert canonical_id_rooted_at(
            canonical_id="staging.orders", datasource="prod",
        ) is False

    def test_character_prefix_does_not_match(self) -> None:
        """REGRESSION (DEV-1409): ``prod_v2`` is a sibling datasource, not
        a child of ``prod``. The dotted-namespace rule forbids
        character-prefix matches."""
        assert canonical_id_rooted_at(
            canonical_id="prod_v2", datasource="prod",
        ) is False
        assert canonical_id_rooted_at(
            canonical_id="prod_v2.orders", datasource="prod",
        ) is False
        assert canonical_id_rooted_at(
            canonical_id="prod123", datasource="prod",
        ) is False

    def test_memory_id_never_matches_datasource(self) -> None:
        """``memory:<int>`` canonical ids are datasource-agnostic — they
        never match any datasource filter, even one named ``memory``."""
        assert canonical_id_rooted_at(
            canonical_id="memory:1", datasource="prod",
        ) is False
        assert canonical_id_rooted_at(
            canonical_id="memory:1", datasource="memory",
        ) is False
        assert canonical_id_rooted_at(
            canonical_id="memory:42", datasource="memory",
        ) is False

    def test_empty_datasource_does_not_match_anything(self) -> None:
        """Defensive — empty-string datasource is rejected by validators
        but the helper should still behave."""
        assert canonical_id_rooted_at(
            canonical_id="prod", datasource="",
        ) is False
        assert canonical_id_rooted_at(
            canonical_id="prod.orders", datasource="",
        ) is False

    @pytest.mark.parametrize("canonical_id", [
        "prod",
        "prod.orders",
        "prod.orders.id",
        "prod.orders.revenue_sum",
    ])
    def test_every_descendant_shape_matches(self, canonical_id: str) -> None:
        assert canonical_id_rooted_at(
            canonical_id=canonical_id, datasource="prod",
        ) is True
