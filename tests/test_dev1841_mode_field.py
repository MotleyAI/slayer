"""The ``to_many_handling`` field, ``strict`` as its deprecated alias, and the SlayerQuery v3→v4 storage migration.

Spec: queries/attribution-modes — "Mode selection and the association default", "The strict flag is a deprecated alias".
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery
from slayer.storage import migrations as mig
from tests._deprecated_input_helpers import deprecations, recorded


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestModeField:
    def test_default_is_omitted(self) -> None:
        assert _q().to_many_handling is None

    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    def test_each_value_accepted(self, mode: str) -> None:
        assert _q(to_many_handling=mode).to_many_handling == mode

    def test_unrecognized_value_fails_naming_the_modes(self) -> None:
        """Scenario: unrecognized mode value fails — with the accepted values."""
        with pytest.raises(ValueError) as ei:
            _q(to_many_handling="partition")
        msg = str(ei.value)
        assert "broadcast" in msg
        assert "associate" in msg
        assert "error" in msg


class TestStrictRetired:
    def test_strict_field_is_gone(self) -> None:
        assert "strict" not in SlayerQuery.model_fields
        assert "to_many_handling" in SlayerQuery.model_fields

    def test_strict_true_converts_to_error_mode(self) -> None:
        with recorded() as caught:
            query = _q(strict=True)
        assert query.to_many_handling == "error"
        assert len(deprecations(caught)) == 1

    def test_strict_false_is_dropped_with_a_deprecation(self) -> None:
        with recorded() as caught:
            query = _q(strict=False)
        assert query.to_many_handling is None
        assert len(deprecations(caught)) == 1

    @pytest.mark.parametrize("version", [4, "4", 3.9])
    def test_current_or_fractional_version_with_strict_converts(self, version) -> None:
        """Fresh input — never a silent float truncation into a stale migratable version."""
        payload = {"version": version, "source_model": "orders", "strict": True}
        with recorded() as caught:
            query = SlayerQuery.model_validate(payload)
        assert query.to_many_handling == "error"
        assert len(deprecations(caught)) == 1

    def test_unparseable_version_with_strict_fails_cleanly(self) -> None:
        """A ``ValueError``, never a ``TypeError`` from a pre-coercion comparison."""
        payload = {"version": "bogus", "source_model": "orders", "strict": True}
        with recorded(), pytest.raises(ValueError):
            SlayerQuery.model_validate(payload)


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

    def test_strict_string_false_maps_to_default(self) -> None:
        """A legacy quoted ``strict: "false"`` stays broadcast (migration precedes
        Pydantic bool coercion, so the raw string must not read as truthy)."""
        for raw in ("false", "False", "0", "no", "off", ""):
            out = self._migrate(strict=raw)
            assert out.get("to_many_handling", "broadcast") == "broadcast", raw

    def test_strict_string_true_becomes_error_mode(self) -> None:
        for raw in ("true", "True", "1", "yes", "on"):
            out = self._migrate(strict=raw)
            assert out["to_many_handling"] == "error", raw

    def test_strict_unrecognized_string_fails_closed(self) -> None:
        """An unrecognized legacy token raises rather than silently broadcasting."""
        for raw in ("error", "treu", "maybe", "broadcast"):
            with pytest.raises(ValueError) as ei:
                self._migrate(strict=raw)
            assert "to_many_handling" in str(ei.value), raw

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
