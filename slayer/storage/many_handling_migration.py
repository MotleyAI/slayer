"""SlayerModel v20 / SlayerQuery v11: a stored ``to_many_handling: "broadcast"`` was the saved default, so it
loads omitted (the association default). Stored-only."""

from typing import Any

from slayer.storage.migrations import register_migration, stamp_stored

#: The SlayerQuery version whose saves wrote the broadcast default.
_BROADCAST_DEFAULT_VERSION = 11


def _stamp_stage(stage: Any) -> Any:
    """An unversioned stored stage was written at the broadcast-default version."""
    if isinstance(stage, dict) and "version" not in stage:
        return {**stage, "version": _BROADCAST_DEFAULT_VERSION}
    return stage


def stamp_model_stages(data: dict) -> dict:
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [_stamp_stage(q) for q in data["source_queries"]]
    return data


def drop_stored_broadcast(data: dict) -> dict:
    if data.get("to_many_handling") == "broadcast":
        del data["to_many_handling"]
    source = data.get("source_model")
    if isinstance(source, dict) and "source_name" not in source:
        data["source_model"] = stamp_stored(source)
    return data


register_migration(entity="SlayerModel", source_version=20, stored_only=True)(stamp_model_stages)
register_migration(entity="SlayerQuery", source_version=11, stored_only=True)(drop_stored_broadcast)
