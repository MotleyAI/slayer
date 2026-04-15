"""Tests for dbt entity registry and join resolution."""


from slayer.dbt.entities import EntityRegistry
from slayer.dbt.models import DbtEntity, DbtSemanticModel


def _make_model(name, entities):
    return DbtSemanticModel(name=name, entities=entities)


class TestEntityRegistry:
    def test_register_primary(self) -> None:
        reg = EntityRegistry()
        reg.build([_make_model("orders", [
            DbtEntity(name="order_id", type="primary", expr="id"),
        ])])
        assert reg.get_primary_model("order_id") == ("orders", "id")

    def test_register_unique(self) -> None:
        reg = EntityRegistry()
        reg.build([_make_model("users", [
            DbtEntity(name="user_id", type="unique", expr="uid"),
        ])])
        assert reg.get_primary_model("user_id") == ("users", "uid")

    def test_foreign_not_registered(self) -> None:
        reg = EntityRegistry()
        reg.build([_make_model("orders", [
            DbtEntity(name="customer_id", type="foreign"),
        ])])
        assert reg.get_primary_model("customer_id") is None

    def test_expr_defaults_to_name(self) -> None:
        reg = EntityRegistry()
        reg.build([_make_model("orders", [
            DbtEntity(name="order_id", type="primary"),
        ])])
        assert reg.get_primary_model("order_id") == ("orders", "order_id")

    def test_primary_entity_shorthand(self) -> None:
        reg = EntityRegistry()
        sm = DbtSemanticModel(name="orders", primary_entity="order_id", entities=[
            DbtEntity(name="order_id", type="primary", expr="id"),
        ])
        reg.build([sm])
        assert reg.get_primary_model("order_id") == ("orders", "id")


class TestJoinResolution:
    def test_foreign_to_primary_join(self) -> None:
        orders = _make_model("orders", [
            DbtEntity(name="order_id", type="primary", expr="id"),
            DbtEntity(name="customer_id", type="foreign", expr="customer_id"),
        ])
        customers = _make_model("customers", [
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        reg = EntityRegistry()
        reg.build([orders, customers])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 1
        assert joins[0].target_model == "customers"
        assert joins[0].join_pairs == [["customer_id", "id"]]

    def test_no_self_joins(self) -> None:
        """A model's own primary entity should not generate a join to itself."""
        orders = _make_model("orders", [
            DbtEntity(name="order_id", type="primary", expr="id"),
            DbtEntity(name="order_id", type="foreign", expr="order_id"),
        ])
        reg = EntityRegistry()
        reg.build([orders])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 0

    def test_no_duplicate_joins(self) -> None:
        """Multiple foreign entities to the same target should produce one join."""
        orders = _make_model("orders", [
            DbtEntity(name="customer_id", type="foreign", expr="cust_id"),
            DbtEntity(name="customer_id", type="foreign", expr="alt_cust_id"),
        ])
        customers = _make_model("customers", [
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        reg = EntityRegistry()
        reg.build([orders, customers])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 1

    def test_multiple_foreign_entities(self) -> None:
        orders = _make_model("orders", [
            DbtEntity(name="order_id", type="primary", expr="id"),
            DbtEntity(name="customer_id", type="foreign"),
            DbtEntity(name="product_id", type="foreign"),
        ])
        customers = _make_model("customers", [
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        products = _make_model("products", [
            DbtEntity(name="product_id", type="primary", expr="id"),
        ])
        reg = EntityRegistry()
        reg.build([orders, customers, products])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 2
        target_names = {j.target_model for j in joins}
        assert target_names == {"customers", "products"}

    def test_unresolvable_foreign_entity(self) -> None:
        """Foreign entity with no matching primary should be silently skipped."""
        orders = _make_model("orders", [
            DbtEntity(name="unknown_id", type="foreign"),
        ])
        reg = EntityRegistry()
        reg.build([orders])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 0
