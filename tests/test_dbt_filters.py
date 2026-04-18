"""Tests for dbt Jinja filter conversion."""

import logging

from slayer.dbt.entities import EntityRegistry
from slayer.dbt.filters import convert_dbt_filter
from slayer.dbt.models import DbtDimension, DbtEntity, DbtSemanticModel


def _build_registry(*models):
    reg = EntityRegistry()
    reg.build(list(models))
    return reg


class TestConvertDbtFilter:
    def test_dimension_foreign_entity(self) -> None:
        """Dimension reference through a foreign entity resolves to target_model.dim."""
        orders = DbtSemanticModel(name="orders", entities=[
            DbtEntity(name="order_id", type="primary", expr="id"),
            DbtEntity(name="customer_id", type="foreign"),
        ])
        customers = DbtSemanticModel(name="customers", entities=[
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        reg = _build_registry(orders, customers)

        result = convert_dbt_filter(
            filter_str="{{Dimension('customer_id__status')}} = 'active'",
            source_model_name="orders",
            entity_registry=reg,
            model_entity_names={"order_id": "primary", "customer_id": "foreign"},
        )
        assert result == "customers.status = 'active'"

    def test_dimension_own_primary_entity(self) -> None:
        """Dimension reference through own primary entity resolves to bare dim name."""
        orders = DbtSemanticModel(name="orders", entities=[
            DbtEntity(name="order_id", type="primary", expr="id"),
        ])
        reg = _build_registry(orders)

        result = convert_dbt_filter(
            filter_str="{{Dimension('order_id__status')}} = 'completed'",
            source_model_name="orders",
            entity_registry=reg,
            model_entity_names={"order_id": "primary"},
        )
        assert result == "status = 'completed'"

    def test_time_dimension(self) -> None:
        """TimeDimension reference extracts just the name."""
        reg = _build_registry()
        result = convert_dbt_filter(
            filter_str="{{ TimeDimension('metric_time', 'day') }} >= '2024-01-01'",
            source_model_name="orders",
            entity_registry=reg,
        )
        assert result == "metric_time >= '2024-01-01'"

    def test_time_dimension_no_grain(self) -> None:
        reg = _build_registry()
        result = convert_dbt_filter(
            filter_str="{{ TimeDimension('order_date') }} >= '2024-01-01'",
            source_model_name="orders",
            entity_registry=reg,
        )
        assert result == "order_date >= '2024-01-01'"

    def test_entity_reference(self) -> None:
        """Entity reference resolves to the entity's expr."""
        orders = DbtSemanticModel(name="orders", entities=[
            DbtEntity(name="customer_id", type="foreign", expr="cust_id"),
        ])
        customers = DbtSemanticModel(name="customers", entities=[
            DbtEntity(name="customer_id", type="primary", expr="id"),
        ])
        reg = _build_registry(orders, customers)

        result = convert_dbt_filter(
            filter_str="{{ Entity('customer_id') }} IS NOT NULL",
            source_model_name="orders",
            entity_registry=reg,
        )
        assert result == "id IS NOT NULL"

    def test_plain_string_passthrough(self) -> None:
        """Non-Jinja filter strings pass through unchanged."""
        reg = _build_registry()
        result = convert_dbt_filter(
            filter_str="status = 'active'",
            source_model_name="orders",
            entity_registry=reg,
        )
        assert result == "status = 'active'"

    def test_multiple_replacements(self) -> None:
        """Multiple Jinja references in one filter string."""
        orders = DbtSemanticModel(name="orders", entities=[
            DbtEntity(name="order_id", type="primary", expr="id"),
        ])
        reg = _build_registry(orders)

        result = convert_dbt_filter(
            filter_str="{{Dimension('order_id__status')}} = 'active' AND {{Dimension('order_id__type')}} = 'online'",
            source_model_name="orders",
            entity_registry=reg,
            model_entity_names={"order_id": "primary"},
        )
        assert result == "status = 'active' AND type = 'online'"

    def test_benchmark_loss_payment_filter(self) -> None:
        """Real-world filter from the dbt benchmark."""
        claim_amount = DbtSemanticModel(name="claim_amount", entities=[
            DbtEntity(name="claim_amount", type="primary", expr="claim_amount_identifier"),
        ])
        loss_payment = DbtSemanticModel(name="loss_payment", entities=[
            DbtEntity(name="claim_amount", type="primary", expr="Claim_Amount_Identifier"),
        ])
        reg = _build_registry(claim_amount, loss_payment)

        result = convert_dbt_filter(
            filter_str="{{Dimension('claim_amount__has_loss_payment')}} = 1",
            source_model_name="claim_amount",
            entity_registry=reg,
            model_entity_names={"claim_amount": "primary"},
        )
        # claim_amount is the model's own primary entity, so bare name
        assert result == "has_loss_payment = 1"

    def test_peer_dimension_qualified_when_all_models_provided(self) -> None:
        """Dimension on peer model is qualified as peer_model.dim when all_semantic_models is provided."""
        # claim_amount has primary entity 'claim_amount' but NO 'has_loss_payment' dimension
        claim_amount = DbtSemanticModel(
            name="claim_amount",
            entities=[DbtEntity(name="claim_amount", type="primary", expr="id")],
            dimensions=[DbtDimension(name="amount")],
        )
        # loss_payment shares the same primary entity and HAS 'has_loss_payment'
        loss_payment = DbtSemanticModel(
            name="loss_payment",
            entities=[DbtEntity(name="claim_amount", type="primary", expr="id")],
            dimensions=[DbtDimension(name="has_loss_payment")],
        )
        reg = _build_registry(claim_amount, loss_payment)

        result = convert_dbt_filter(
            filter_str="{{Dimension('claim_amount__has_loss_payment')}} = 1",
            source_model_name="claim_amount",
            entity_registry=reg,
            model_entity_names={"claim_amount": "primary"},
            all_semantic_models={"claim_amount": claim_amount, "loss_payment": loss_payment},
        )
        # Should be qualified with peer model name
        assert result == "loss_payment.has_loss_payment = 1"

    def test_peer_dimension_warns_without_all_models(self, caplog) -> None:
        """Without all_semantic_models, peer lookup silently falls back to bare name."""
        claim_amount = DbtSemanticModel(
            name="claim_amount",
            entities=[DbtEntity(name="claim_amount", type="primary", expr="id")],
            dimensions=[DbtDimension(name="amount")],
        )
        loss_payment = DbtSemanticModel(
            name="loss_payment",
            entities=[DbtEntity(name="claim_amount", type="primary", expr="id")],
            dimensions=[DbtDimension(name="has_loss_payment")],
        )
        reg = _build_registry(claim_amount, loss_payment)

        with caplog.at_level(logging.WARNING, logger="slayer.dbt.filters"):
            result = convert_dbt_filter(
                filter_str="{{Dimension('claim_amount__has_loss_payment')}} = 1",
                source_model_name="claim_amount",
                entity_registry=reg,
                model_entity_names={"claim_amount": "primary"},
                # all_semantic_models NOT provided
            )
        # Falls back to bare name (cannot find peer)
        assert result == "has_loss_payment = 1"
        # Should have logged a warning about missing all_semantic_models
        assert any("all_semantic_models" in r.message for r in caplog.records), (
            f"Expected warning about all_semantic_models, got: {[r.message for r in caplog.records]}"
        )

    def test_peer_dimension_deterministic_across_registry_order(self) -> None:
        """When two peers both have the same dimension, the resolved peer must be
        deterministic regardless of registry build order."""
        source = DbtSemanticModel(
            name="source",
            entities=[DbtEntity(name="shared", type="primary", expr="id")],
            dimensions=[],  # dim NOT on source
        )
        alpha = DbtSemanticModel(
            name="alpha",
            entities=[DbtEntity(name="shared", type="primary", expr="id")],
            dimensions=[DbtDimension(name="the_dim")],
        )
        beta = DbtSemanticModel(
            name="beta",
            entities=[DbtEntity(name="shared", type="primary", expr="id")],
            dimensions=[DbtDimension(name="the_dim")],
        )
        all_models = {"source": source, "alpha": alpha, "beta": beta}

        reg1 = _build_registry(source, alpha, beta)
        result1 = convert_dbt_filter(
            filter_str="{{Dimension('shared__the_dim')}} = 1",
            source_model_name="source",
            entity_registry=reg1,
            model_entity_names={"shared": "primary"},
            all_semantic_models=all_models,
        )

        reg2 = _build_registry(source, beta, alpha)
        result2 = convert_dbt_filter(
            filter_str="{{Dimension('shared__the_dim')}} = 1",
            source_model_name="source",
            entity_registry=reg2,
            model_entity_names={"shared": "primary"},
            all_semantic_models=all_models,
        )

        assert result1 == result2, f"Nondeterministic: {result1!r} vs {result2!r}"
        # Should pick the lexicographically first peer
        assert result1 == "alpha.the_dim = 1"
