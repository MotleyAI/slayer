"""v7 → v8 (SlayerModel) and v3 → v4 (SlayerQuery) schema migrations (DEV-1607).

OSI-aligned vocabulary rename applied to persisted dicts:

* ``columns`` → ``fields``
* ``measures`` → ``metrics`` (and each metric's ``formula`` → ``expression``)
* ``joins`` → ``relationships`` (and each relationship's ``target_model`` →
  ``target_dataset``)
* ``SlayerQuery.source_model`` → ``source_dataset``

These run for PERSISTED dicts (which carry a ``version``). Runtime constructions
without a ``version`` are handled by the deprecated-key aliasing on the Pydantic
models themselves (which additionally warns).
"""

from slayer.storage.migrations import migrate, register_migration


def _rename_metric_expressions(metrics: object) -> None:
    if not isinstance(metrics, list):
        return
    for m in metrics:
        if isinstance(m, dict) and "formula" in m and "expression" not in m:
            m["expression"] = m.pop("formula")


def _rename_relationship_targets(relationships: object) -> None:
    if not isinstance(relationships, list):
        return
    for r in relationships:
        if isinstance(r, dict) and "target_model" in r and "target_dataset" not in r:
            r["target_dataset"] = r.pop("target_model")


def _rename_extension_keys(source: object) -> None:
    """Rename collection keys on an inline ``ModelExtension`` dict in-place."""
    if not isinstance(source, dict):
        return
    if "columns" in source and "fields" not in source:
        source["fields"] = source.pop("columns")
    if "measures" in source and "metrics" not in source:
        source["metrics"] = source.pop("measures")
        _rename_metric_expressions(source["metrics"])
    if "joins" in source and "relationships" not in source:
        source["relationships"] = source.pop("joins")
        _rename_relationship_targets(source["relationships"])


@register_migration("SlayerModel", 7)
def _model_v7_to_v8(data: dict) -> dict:
    """Rename ``columns``/``measures``/``joins`` (+ nested keys) and walk
    ``source_queries`` through the SlayerQuery migration chain."""
    if "columns" in data and "fields" not in data:
        data["fields"] = data.pop("columns")
    if "measures" in data and "metrics" not in data:
        data["metrics"] = data.pop("measures")
    if "joins" in data and "relationships" not in data:
        data["relationships"] = data.pop("joins")
    _rename_metric_expressions(data.get("metrics"))
    _rename_relationship_targets(data.get("relationships"))

    raw = data.get("source_queries")
    if isinstance(raw, list):
        data["source_queries"] = [
            migrate("SlayerQuery", q) if isinstance(q, dict) else q for q in raw
        ]
    return data


@register_migration("SlayerQuery", 3)
def _query_v3_to_v4(data: dict) -> dict:
    """Rename ``source_model`` → ``source_dataset`` and ``measures`` →
    ``metrics`` (+ nested expression/extension keys)."""
    if "source_model" in data and "source_dataset" not in data:
        data["source_dataset"] = data.pop("source_model")
    if "measures" in data and "metrics" not in data:
        data["metrics"] = data.pop("measures")
    _rename_metric_expressions(data.get("metrics"))
    _rename_extension_keys(data.get("source_dataset"))
    return data
