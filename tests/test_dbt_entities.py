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

    def test_get_primary_model_deterministic_across_input_order(self) -> None:
        """When multiple models share a primary entity, get_primary_model must return
        the same model regardless of input order."""
        model_a = _make_model("alpha", [DbtEntity(name="shared", type="primary", expr="id")])
        model_b = _make_model("beta", [DbtEntity(name="shared", type="primary", expr="id")])

        reg1 = EntityRegistry()
        reg1.build([model_a, model_b])

        reg2 = EntityRegistry()
        reg2.build([model_b, model_a])

        assert reg1.get_primary_model("shared") == reg2.get_primary_model("shared")


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

    def test_truly_duplicate_signature_deduped(self) -> None:
        """Two foreign entities with identical (target, fk_expr, pk_expr) signature
        produce one join — protects against truly redundant YAML entries."""
        orders = _make_model("orders", [
            DbtEntity(name="customer_id", type="foreign", expr="cust_id"),
            DbtEntity(name="customer_id", type="foreign", expr="cust_id"),
        ])
        customers = _make_model("customers", [
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        reg = EntityRegistry()
        reg.build([orders, customers])

        joins = reg.resolve_joins_for_model(orders)
        assert len(joins) == 1

    def test_distinct_fks_to_same_target_kept_separate(self) -> None:
        """Regression for CodeRabbit #4 — distinct FK columns pointing at the same
        target model (e.g., buyer_id / seller_id both → users.id) must each get
        their own ModelJoin so neither relationship silently disappears."""
        transactions = _make_model("transactions", [
            DbtEntity(name="transaction_id", type="primary", expr="id"),
            DbtEntity(name="user_id", type="foreign", expr="buyer_id"),
            DbtEntity(name="user_id", type="foreign", expr="seller_id"),
        ])
        users = _make_model("users", [
            DbtEntity(name="user_id", type="primary", expr="id"),
        ])
        reg = EntityRegistry()
        reg.build([transactions, users])

        joins = reg.resolve_joins_for_model(transactions)
        assert len(joins) == 2
        # Both joins point at users but via different FK columns
        assert all(j.target_model == "users" for j in joins)
        fk_columns = sorted(j.join_pairs[0][0] for j in joins)
        assert fk_columns == ["buyer_id", "seller_id"]
        # Both target the same primary key on users
        assert all(j.join_pairs[0][1] == "id" for j in joins)

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

    def test_peer_joins_two_entities_same_peer_different_columns(self) -> None:
        """Two shared primary entities mapping to the same peer via different columns
        must both produce peer joins, not collapse to one."""
        # model_a has two primary entities that both map to model_b
        model_a = _make_model("model_a", [
            DbtEntity(name="entity_x", type="primary", expr="col_x"),
            DbtEntity(name="entity_y", type="primary", expr="col_y"),
        ])
        model_b = _make_model("model_b", [
            DbtEntity(name="entity_x", type="primary", expr="bx"),
            DbtEntity(name="entity_y", type="primary", expr="by"),
        ])
        reg = EntityRegistry()
        reg.build([model_a, model_b])

        joins = reg.resolve_joins_for_model(model_a)
        # Both peer joins should survive (different columns)
        assert len(joins) == 2, f"Expected 2 peer joins, got {len(joins)}: {[(j.target_model, j.join_pairs) for j in joins]}"
        pairs = sorted([j.join_pairs[0] for j in joins])
        assert pairs == [["col_x", "bx"], ["col_y", "by"]]
