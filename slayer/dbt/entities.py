"""Entity registry — resolves dbt entities to SLayer primary keys and joins.

dbt entities define the grain (primary) and join relationships (foreign) between
semantic models. This module scans all models to build an entity→model mapping,
then generates SLayer ModelJoin objects for foreign entity references.
"""

import logging
from typing import Dict, List, Optional, Tuple

from slayer.core.models import ModelJoin
from slayer.dbt.models import DbtSemanticModel

logger = logging.getLogger(__name__)


class EntityRegistry:
    """Maps entity names to their primary/unique owning models."""

    def __init__(self) -> None:
        # {entity_name: (model_name, expr)}
        self._primaries: Dict[str, Tuple[str, str]] = {}

    def build(self, models: List[DbtSemanticModel]) -> None:
        """First pass: register all primary and unique entities."""
        for model in models:
            # Check primary_entity shorthand
            if model.primary_entity:
                expr = model.primary_entity
                # Look for an entity with this name to get the expr
                for e in model.entities:
                    if e.name == model.primary_entity:
                        expr = e.expr or e.name
                        break
                self._register(
                    entity_name=model.primary_entity,
                    model_name=model.name,
                    expr=expr,
                )

            for entity in model.entities:
                if entity.type in ("primary", "unique"):
                    self._register(
                        entity_name=entity.name,
                        model_name=model.name,
                        expr=entity.expr or entity.name,
                    )

    def _register(self, entity_name: str, model_name: str, expr: str) -> None:
        if entity_name in self._primaries:
            existing_model, _ = self._primaries[entity_name]
            if existing_model != model_name:
                # Multiple models claim same primary entity — last one wins
                # (this is valid in dbt for shared-grain models like loss_payment/claim_amount)
                logger.debug(
                    "Entity '%s' claimed by both '%s' and '%s'; keeping '%s'",
                    entity_name, existing_model, model_name, model_name,
                )
        self._primaries[entity_name] = (model_name, expr)

    def get_primary_model(self, entity_name: str) -> Optional[Tuple[str, str]]:
        """Look up which model owns this entity as primary.

        Returns (model_name, expr) or None.
        """
        return self._primaries.get(entity_name)

    def resolve_joins_for_model(self, model: DbtSemanticModel) -> List[ModelJoin]:
        """For each foreign entity in the model, generate a ModelJoin to the primary model.

        Returns a list of ModelJoin objects. Skips entities whose primary model
        is the same as the current model (self-joins are not useful).
        """
        joins: List[ModelJoin] = []
        seen_targets: set = set()

        for entity in model.entities:
            if entity.type != "foreign":
                continue

            primary = self.get_primary_model(entity.name)
            if primary is None:
                logger.warning(
                    "Model '%s': foreign entity '%s' has no matching primary entity",
                    model.name, entity.name,
                )
                continue

            target_model_name, primary_expr = primary
            if target_model_name == model.name:
                continue  # Skip self-joins

            # Avoid duplicate joins to the same target
            if target_model_name in seen_targets:
                continue
            seen_targets.add(target_model_name)

            foreign_expr = entity.expr or entity.name
            joins.append(ModelJoin(
                target_model=target_model_name,
                join_pairs=[[foreign_expr, primary_expr]],
            ))

        return joins

    def resolve_entity_to_model(self, entity_name: str) -> Optional[str]:
        """Given an entity name, return the model that owns it as primary."""
        primary = self.get_primary_model(entity_name)
        if primary is None:
            return None
        return primary[0]

    def get_entity_expr(self, entity_name: str) -> Optional[str]:
        """Get the SQL expression for an entity's primary key column."""
        primary = self.get_primary_model(entity_name)
        if primary is None:
            return None
        return primary[1]
