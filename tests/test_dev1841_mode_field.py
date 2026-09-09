"""DEV-1841 task 1.1 / 1.2 — the ``to_many_handling`` field, ``strict``
retirement, and the SlayerQuery v3→v4 storage migration.

Spec: openspec …/specs/queries/attribution-modes — "Query-level mode
selection", "The strict flag is retired".
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery
from slayer.storage import migrations as mig


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestModeField:
    def test_default_is_broadcast(self) -> None:
        assert _q().to_many_handling == "broadcast"

    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    def test_each_value_accepted(self, mode: str) -> None:
        assert _q(to_many_handling=mode).to_many_handling == mode

    def test_unrecognized_value_fails_naming_the_modes(self) -> None:
        """Scenario: unrecognized mode value fails — with the accepted values."""
        with pytest.raises(ValueError) as ei:
            _q(to_many_handling="partition")
        msg = str(ei.value)
        assert "broadcast" in msg and "associate" in msg and "error" in msg


class TestStrictRetired:
    def test_strict_field_is_gone(self) -> None:
        assert "strict" not in SlayerQuery.model_fields
        assert "to_many_handling" in SlayerQuery.model_fields

    def test_strict_input_rejected_naming_replacement(self) -> None:
        """Scenario: strict input is rejected with the remedy."""
        with pytest.raises(ValueError) as ei:
            _q(strict=True)
        assert "to_many_handling" in str(ei.value)

    def test_strict_false_also_rejected(self) -> None:
        """Even ``strict=False`` is a retired field on input — no silent accept."""
        with pytest.raises(ValueError) as ei:
            _q(strict=False)
        assert "to_many_handling" in str(ei.value)


class TestStorageMigration:
    def test_current_version_bumped_and_step_registered(self) -> None:
        assert mig.CURRENT_VERSIONS["SlayerQuery"] >= 4
        assert ("SlayerQuery", 3) in mig._REGISTRY

    def _migrate(self, **payload) -> dict:
        payload.setdefault("version", 3)
        payload.setdefault("source_model", "orders")
        return mig.migrate(entity="SlayerQuery", data=payload)

    def test_strict_true_becomes_error_mode(self) -> None:
        """Scenario: stored strict queries migrate — ``strict: true`` → error."""
        out = self._migrate(strict=True)
        assert out["to_many_handling"] == "error"
        assert "strict" not in out
        assert out["version"] >= 4

    def test_strict_false_maps_to_default(self) -> None:
        out = self._migrate(strict=False)
        assert out.get("to_many_handling", "broadcast") == "broadcast"
        assert "strict" not in out

    def test_strict_absent_maps_to_default(self) -> None:
        out = self._migrate()
        assert out.get("to_many_handling", "broadcast") == "broadcast"
        assert "strict" not in out

    def test_migrated_v3_payload_validates(self) -> None:
        """A stored v3 payload with ``strict`` loads (migration precedes the
        input-rejection validator), landing in the error mode."""
        loaded = SlayerQuery.model_validate(
            {"version": 3, "source_model": "orders", "strict": True},
        )
        assert loaded.to_many_handling == "error"
