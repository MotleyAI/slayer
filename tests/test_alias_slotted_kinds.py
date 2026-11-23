"""The alias environment resolves only its stated key kinds by slot; a composite row key renders over slotted
leaves (spec: importers/osi-export)."""

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import FieldKey, FieldSqlKey, TimeTruncKey
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.value_expr import _ALIAS_SLOTTED_KINDS, AliasFacilities, RenderContext, render_value_key

_LEAF = FieldKey(leaf="ordered_at")
_BUCKET = TimeTruncKey(source=_LEAF, granularity=TimeGranularity.MONTH)


def _ctx(aliases: AliasFacilities) -> RenderContext:
    return RenderContext(dialect=SqlDialect(), aliases=aliases, field_type=lambda _key: DataType.DATETIME)


def _leaf_slots(*, slotted_kinds: tuple[type, ...] = _ALIAS_SLOTTED_KINDS) -> AliasFacilities:
    return AliasFacilities(slotted_kinds=slotted_kinds, slot_id_by_key={_LEAF: "s0"},
                           value_by_slot_id={"s0": exp.column("ordered_at")})


def test_default_slotted_kinds_are_the_five() -> None:
    assert AliasFacilities().slotted_kinds == _ALIAS_SLOTTED_KINDS


def test_unslotted_bucket_raises_under_the_default() -> None:
    ctx = _ctx(_leaf_slots())
    with pytest.raises(RenderContextMissingFacilityError):
        render_value_key(key=_BUCKET, ctx=ctx)


def test_bucket_renders_over_slotted_leaves() -> None:
    out = render_value_key(key=_BUCKET, ctx=_ctx(_leaf_slots(slotted_kinds=(FieldKey, FieldSqlKey))))
    assert out.sql(dialect="snowflake") == "DATE_TRUNC('MONTH', CAST(ordered_at AS TIMESTAMP))"


def test_unslotted_leaf_still_raises_with_leaf_only_kinds() -> None:
    ctx = _ctx(AliasFacilities(slotted_kinds=(FieldKey, FieldSqlKey)))
    with pytest.raises(RenderContextMissingFacilityError):
        render_value_key(key=_BUCKET, ctx=ctx)
