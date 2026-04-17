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
        # {entity_name: [(model_name, expr), ...]}
        self._primaries: Dict[str, List[Tuple[str, str]]] = {}

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
        if entity_name not in self._primaries:
            self._primaries[entity_name] = []
        # Deduplicate by model_name
        if any(m == model_name for m, _ in self._primaries[entity_name]):
            return
        if self._primaries[entity_name]:
            existing_model, _ = self._primaries[entity_name][0]
            logger.debug(
                "Entity '%s' shared by '%s' and '%s' (peer join will be created)",
                entity_name, existing_model, model_name,
            )
        self._primaries[entity_name].append((model_name, expr))

    def get_primary_model(self, entity_name: str) -> Optional[Tuple[str, str]]:
        """Look up which model owns this entity as primary.

        Returns (model_name, expr) or None.  When multiple models share the
        same primary entity, the first registered one is returned.
        """
        entries = self._primaries.get(entity_name)
        if not entries:
            return None
        return entries[0]

    def resolve_joins_for_model(self, model: DbtSemanticModel) -> List[ModelJoin]:
        """For each foreign entity in the model, generate a ModelJoin to the primary model.

        Returns a list of ModelJoin objects. Skips entities whose primary model
        is the same as the current model (self-joins are not useful).
        """
        joins: List[ModelJoin] = []
        # Dedupe by full join signature (target + FK columns) so distinct FKs
        # to the same target — e.g. buyer_id -> users.id AND seller_id -> users.id —
        # each get their own ModelJoin instead of silently collapsing.
        seen_signatures: set = set()

        for entity in model.entities:
            if entity.type != "foreign":
                continue

            primaries = self._primaries.get(entity.name, [])
            if not primaries:
                logger.warning(
                    "Model '%s': foreign entity '%s' has no matching primary entity",
                    model.name, entity.name,
                )
                continue

            foreign_expr = entity.expr or entity.name
            for target_model_name, primary_expr in primaries:
                if target_model_name == model.name:
                    continue  # Skip self-joins

                signature = (target_model_name, foreign_expr, primary_expr)
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)

                joins.append(ModelJoin(
                    target_model=target_model_name,
                    join_pairs=[[foreign_expr, primary_expr]],
                ))

        # Peer joins: models sharing the same primary/unique entity are joinable
        already_targeted = {j.target_model for j in joins}
        for entity in model.entities:
            if entity.type not in ("primary", "unique"):
                continue
            peers = self._primaries.get(entity.name, [])
            local_expr = entity.expr or entity.name
            for peer_model_name, peer_expr in peers:
                if peer_model_name == model.name:
                    continue
                if peer_model_name in already_targeted:
                    continue
                already_targeted.add(peer_model_name)
                joins.append(ModelJoin(
                    target_model=peer_model_name,
                    join_pairs=[[local_expr, peer_expr]],
                ))

        return joins

    def resolve_entity_to_model(self, entity_name: str) -> Optional[str]:
        """Given an entity name, return the first model that owns it as primary."""
        entry = self.get_primary_model(entity_name)
        if entry is None:
            return None
        return entry[0]

    def get_entity_expr(self, entity_name: str) -> Optional[str]:
        """Get the SQL expression for an entity's primary key column."""
        entry = self.get_primary_model(entity_name)
        if entry is None:
            return None
        return entry[1]
