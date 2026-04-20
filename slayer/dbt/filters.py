"""Convert dbt Jinja filter syntax to SLayer plain filter strings.

dbt filters use Jinja templates like:
    {{ Dimension('claim_amount__has_loss_payment') }} = 1
    {{ TimeDimension('metric_time', 'day') }} >= '2024-01-01'
    {{ Entity('customer_id') }}

SLayer uses plain SQL-like strings:
    loss_payment.has_loss_payment = 1
    metric_time >= '2024-01-01'
"""

import logging
import re
from typing import Dict, Optional

from slayer.dbt.entities import EntityRegistry
from slayer.dbt.models import DbtSemanticModel

logger = logging.getLogger(__name__)

# Regex patterns for dbt Jinja filter references
_DIMENSION_RE = re.compile(
    r"\{\{\s*Dimension\(\s*['\"](\w+)__(\w+)['\"]\s*\)\s*\}\}"
)
_TIME_DIMENSION_RE = re.compile(
    r"\{\{\s*TimeDimension\(\s*['\"](\w+)['\"]"
    r"(?:\s*,\s*['\"](\w+)['\"])?\s*\)\s*\}\}"
)
_ENTITY_RE = re.compile(
    r"\{\{\s*Entity\(\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)


def convert_dbt_filter(
    filter_str: str,
    source_model_name: str,
    entity_registry: EntityRegistry,
    model_entity_names: Optional[Dict[str, str]] = None,
    all_semantic_models: Optional[Dict[str, DbtSemanticModel]] = None,
) -> str:
    """Convert a dbt Jinja filter string to a SLayer filter string.

    Args:
        filter_str: The raw dbt filter, e.g. "{{ Dimension('claim_amount__has_loss_payment') }} = 1"
        source_model_name: Name of the model this filter is defined on.
        entity_registry: Registry mapping entity names to primary models.
        model_entity_names: {entity_name: entity_type} for entities defined on the source model.
            Used to determine if an entity reference is local (primary) or remote (foreign).
        all_semantic_models: {model_name: DbtSemanticModel} for all parsed models.
            Used to verify whether a dimension actually exists on the source model when
            the entity is local.  When the dimension is missing locally, peer models
            sharing the same primary entity are searched and the result is qualified.
    """
    model_entity_names = model_entity_names or {}
    all_semantic_models = all_semantic_models or {}
    result = filter_str

    # Replace {{ Dimension('entity__dim_name') }} references
    def _replace_dimension(match: re.Match) -> str:
        entity_name = match.group(1)
        dim_name = match.group(2)

        # Check if entity is the source model's own primary entity
        entity_type = model_entity_names.get(entity_name)
        if entity_type in ("primary", "unique"):
            # Verify the dimension actually exists on the source model
            source_sm = all_semantic_models.get(source_model_name)
            if source_sm and any(d.name == dim_name for d in source_sm.dimensions):
                return dim_name  # Local dimension — bare name

            # Dimension not on source — search peer models sharing the same entity
            # Sort by model name for deterministic selection when multiple peers match
            peer_models = sorted(
                entity_registry._primaries.get(entity_name, []),
                key=lambda item: item[0],
            )
            for peer_name, _ in peer_models:
                if peer_name == source_model_name:
                    continue
                peer_sm = all_semantic_models.get(peer_name)
                if peer_sm and any(d.name == dim_name for d in peer_sm.dimensions):
                    return f"{peer_name}.{dim_name}"

            # Fallback: bare name (best effort, no model has the dimension)
            if not all_semantic_models:
                logger.warning(
                    "Cannot resolve peer dimension '%s' for entity '%s' on model '%s': "
                    "all_semantic_models not provided — falling back to bare name",
                    dim_name, entity_name, source_model_name,
                )
            return dim_name

        # Foreign entity — resolve to target_model.dim
        target_model = entity_registry.resolve_entity_to_model(entity_name)
        if target_model and target_model != source_model_name:
            return f"{target_model}.{dim_name}"

        # Fallback: use bare name (might be on same model under a different entity name)
        return dim_name

    result = _DIMENSION_RE.sub(_replace_dimension, result)

    # Replace {{ TimeDimension('name', 'granularity') }} → just the name
    def _replace_time_dimension(match: re.Match) -> str:
        return match.group(1)

    result = _TIME_DIMENSION_RE.sub(_replace_time_dimension, result)

    # Replace {{ Entity('name') }} → the entity's expr column
    def _replace_entity(match: re.Match) -> str:
        entity_name = match.group(1)
        expr = entity_registry.get_entity_expr(entity_name)
        return expr or entity_name

    result = _ENTITY_RE.sub(_replace_entity, result)

    return result.strip()
