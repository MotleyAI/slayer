"""SlayerModel v17 / SlayerQuery v8 / Memory v6: a stored aggregation over a bare column ``distinct`` reads it quoted.

``agg(distinct)`` is a parse error in the SQL grammar, so the rewrite never changes valid text. Stored-only.
"""

import re

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import rewrite_model_texts, rewrite_query_texts, stamp_memory_query

_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
_BARE_DISTINCT = re.compile(r"(?<=\w\()(\s*)(distinct)(?=\s*[,)])", re.IGNORECASE)


def quote_bare_distinct(text: str) -> str:
    """``text`` with each aggregation's bare first argument ``distinct`` written ``"distinct"``."""
    masked = _QUOTED.sub(lambda m: "\0" * len(m.group()), text)
    for match in reversed(list(_BARE_DISTINCT.finditer(masked))):
        start, end = match.span(2)
        text = f'{text[:start]}"{text[start:end]}"{text[end:]}'
    return text


def rewrite_model_distinct(data: dict) -> dict:
    return rewrite_model_texts(data, quote_bare_distinct)


def rewrite_query_distinct(data: dict) -> dict:
    return rewrite_query_texts(data, quote_bare_distinct)


register_migration(entity="SlayerModel", source_version=17, stored_only=True)(rewrite_model_distinct)
register_migration(entity="SlayerQuery", source_version=8, stored_only=True)(rewrite_query_distinct)
register_migration(entity="Memory", source_version=6, stored_only=True)(stamp_memory_query)
