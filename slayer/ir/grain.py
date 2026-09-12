"""Compatibility home of ``Grain``.

The class moved into ``slayer.core.keys`` when ``partition_keys`` was retyped
to carry it (DEV-1871): a key field's type must live at or below the keys.
"""

from slayer.core.keys import Grain as Grain

__all__ = ["Grain"]
