"""Ossie content SLayer reads and writes: datatype names and the description an ``ai_context`` renders."""

from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType

#: Ossie's datatype names, each SLayer's value of the same name.
OSSIE_DATATYPES: dict[str, DataType] = {t.value: t for t in DataType if t is not DataType.ARRAY}
TEMPORAL_DATATYPES = frozenset({"Date", "Time", "DateTime", "DateTimeTz"})


class OSIAIContextObject(BaseModel):
    """Structured AI context with instructions, synonyms, and examples."""

    model_config = ConfigDict(extra="allow")

    instructions: Optional[str] = None
    synonyms: Optional[list[str]] = None
    examples: Optional[list[str]] = None


# ai_context is either a plain string or the structured object above.
OSIAIContext = Union[str, OSIAIContextObject]


def ai_context_to_dict(ctx: Optional[OSIAIContext]) -> Optional[dict[str, Any]]:
    """Normalize an ai_context (string or object) into a plain dict, or None."""
    if ctx is None:
        return None
    if isinstance(ctx, str):
        return {"instructions": ctx}
    return ctx.model_dump(exclude_none=True)


def render_description(explicit: Optional[str], ctx: Optional[OSIAIContext]) -> Optional[str]:
    """Explicit description (lead) + ai_context instructions + synonyms, as the importer stores it."""
    parts: list[str] = []
    if explicit:
        parts.append(explicit)
    ctx_dict = ai_context_to_dict(ctx)
    if ctx_dict:
        instructions = ctx_dict.get("instructions")
        if instructions and instructions != explicit:
            parts.append(instructions)
        synonyms = ctx_dict.get("synonyms")
        if synonyms:
            parts.append("Synonyms: " + ", ".join(synonyms))
    return "\n".join(parts) or None


def explicit_description(stored: Optional[str], ctx: Optional[dict[str, Any]]) -> Optional[str]:
    """The explicit description ``render_description`` turned into ``stored`` (``stored`` when none did)."""
    if not stored or not ctx:
        return stored
    context = OSIAIContextObject.model_validate(ctx)
    lines = stored.split("\n")
    for cut in range(len(lines), -1, -1):
        explicit = "\n".join(lines[:cut]) or None
        if render_description(explicit, context) == stored:
            return explicit
    return stored
