"""Re-ingest reconciles FK edges datasource-wide, converges from partial saves, guards the edge namespace."""

from __future__ import annotations

import pytest

from slayer.core.models import Column, SlayerModel

from tests._dev2073_fixtures import (
    BILLING_SHIPPING,
    CHAIN,
    DS,
    TWO_CYCLE,
    Live,
    addition_for,
    live,
)

COUPONS_V1 = """
CREATE TABLE coupon_usages (id INTEGER PRIMARY KEY, code TEXT);
CREATE TABLE transactions (id INTEGER PRIMARY KEY, coupon_usage_id INTEGER REFERENCES coupon_usages(id));
"""
ADD_REVERSE_FK = "ALTER TABLE coupon_usages ADD COLUMN transaction_id INTEGER REFERENCES transactions(id);"

BILLING_V1 = """
CREATE TABLE addresses (id INTEGER PRIMARY KEY, city TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, billing_address_id INTEGER REFERENCES addresses(id));
"""
ADD_SHIPPING_FK = "ALTER TABLE orders ADD COLUMN shipping_address_id INTEGER REFERENCES addresses(id);"


class TestReingestAddsFks:
    async def test_reverse_fk_names_both_edges_and_saves_the_other_model(self) -> None:
        async with live(COUPONS_V1) as lv:
            await lv.ingest()
            lv.run(ADD_REVERSE_FK)

            result = await lv.ingest()

            assert result.errors == []
            edges = await lv.edges()
            assert edges[("coupon_usages", "transactions", (("transaction_id", "id"),))] == "transaction"
            assert edges[("transactions", "coupon_usages", (("coupon_usage_id", "id"),))] == "coupon_usage"
            assert addition_for(result, "coupon_usages").new_joins == ["transactions"]
            named = {n for a in result.additions for n in a.named_joins}
            assert named == {"coupon_usage", "transaction"}
            assert addition_for(result, "transactions").named_joins == ["coupon_usage"]

    async def test_second_fk_to_a_joined_target_merges_with_new_columns(self) -> None:
        async with live(BILLING_V1) as lv:
            await lv.ingest()
            lv.run(ADD_SHIPPING_FK + "ALTER TABLE orders ADD COLUMN note TEXT;")

            result = await lv.ingest()

            assert result.errors == []
            orders = await lv.model("orders")
            assert "note" in {c.name for c in orders.columns}
            assert {j.name for j in orders.joins} == {"billing_address", "shipping_address"}
            addition = addition_for(result, "orders")
            assert "note" in addition.new_columns
            assert addition.new_joins == ["shipping_address"]
            assert set(addition.named_joins) == {"billing_address", "shipping_address"}

    async def test_unchanged_reingest_saves_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async with live(TWO_CYCLE + BILLING_SHIPPING) as lv:
            await lv.ingest()
            before = await lv.edges()
            saves: list[str] = []
            original = lv.storage.save_model

            async def spy(model, *args, **kwargs):
                saves.append(model.name)
                return await original(model, *args, **kwargs)

            monkeypatch.setattr(lv.storage, "save_model", spy)
            result = await lv.ingest()

            assert result.errors == []
            assert saves == []
            assert all(
                not (a.created or a.new_columns or a.new_joins or a.named_joins)
                for a in result.additions
            )
            assert await lv.edges() == before


async def _ingest_v1_then_upgrade(lv: Live) -> None:
    await lv.ingest()
    lv.run(ADD_REVERSE_FK + ADD_SHIPPING_FK)


async def _snapshot(lv: Live) -> dict[str, tuple]:
    return {
        name: (
            sorted(c.name for c in m.columns),
            sorted((j.target_model, j.name, str(j.join_pairs), str(j.cardinality)) for j in m.joins),
        )
        for name, m in (await lv.models()).items()
    }


class TestReingestConvergesFromAPartialSave:
    async def test_rerun_after_any_failed_save_equals_an_uninterrupted_run(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        schema = COUPONS_V1 + BILLING_V1
        async with live(schema) as lv:
            await _ingest_v1_then_upgrade(lv)
            original = lv.storage.save_model
            counted: list[str] = []

            async def count(model, *args, **kwargs):
                counted.append(model.name)
                return await original(model, *args, **kwargs)

            monkeypatch.setattr(lv.storage, "save_model", count)
            await lv.ingest()
            expected = await _snapshot(lv)
        assert len(counted) >= 3

        for failing_index in range(len(counted)):
            async with live(schema) as lv:
                await _ingest_v1_then_upgrade(lv)
                original = lv.storage.save_model
                calls = {"n": 0}

                async def fail_once(model, *args, _original=original, _calls=calls, _k=failing_index, **kwargs):
                    _calls["n"] += 1
                    if _calls["n"] - 1 == _k:
                        raise RuntimeError("injected save failure")
                    return await _original(model, *args, **kwargs)

                monkeypatch.setattr(lv.storage, "save_model", fail_once)
                try:
                    await lv.ingest()
                except RuntimeError:
                    pass
                monkeypatch.setattr(lv.storage, "save_model", original)

                await lv.ingest()

                assert await _snapshot(lv) == expected, failing_index


class TestEdgeNamesAndModelNamesShareANamespace:
    async def test_new_table_named_like_a_stored_edge_is_skipped(self) -> None:
        async with live(CHAIN) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            named = [j.model_copy(update={"name": "returns"}) for j in orders.joins]
            await lv.storage.save_model(orders.model_copy(update={"joins": named}))
            lv.run("CREATE TABLE returns (id INTEGER PRIMARY KEY, reason TEXT);")

            result = await lv.ingest()

            assert await lv.storage.get_model("returns", data_source=DS) is None
            skipped = [s for s in result.skipped if s.table_name == "returns"]
            assert len(skipped) == 1
            assert "returns" in skipped[0].reason
            assert "orders" in skipped[0].reason

    async def test_saving_a_model_named_like_a_stored_edge_is_rejected(self) -> None:
        async with live(CHAIN) as lv:
            await lv.ingest()
            orders = await lv.model("orders")
            named = [j.model_copy(update={"name": "returns"}) for j in orders.joins]
            await lv.storage.save_model(orders.model_copy(update={"joins": named}))

            returns = SlayerModel(
                name="returns", data_source=DS, sql_table="returns",
                columns=[Column(name="id", primary_key=True)],
            )
            with pytest.raises(ValueError, match="returns") as exc:
                await lv.storage.save_model(returns)
            assert "orders" in str(exc.value)
            assert await lv.storage.get_model("returns", data_source=DS) is None
