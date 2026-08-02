"""DEV-1706 Stage 2 — minimal, collision-safe alias allocator.

A single ``AliasAllocator`` is created per top-level ``generate_from_planned``
call and threaded to every ``ScopeFrame`` built during that call. It mints:

* ``_val_<n>`` materialisation aliases (Law 2 — projection-boundary columns),
* CTE names,

seeded from every name already in scope (bundle relations, ``__``-path join
aliases, public projection aliases, model names) so a minted name can never
collide with a user column, a path alias, or a reserved public alias. It also
hands out generation-local ``ScopeFrame`` ids.

This is the *minimal* allocator (subsumes DEV-1692's collision-check primitive).
Stage 9 grows this module into the full naming module (``result_key()``,
``flat_name()``, dialect alias mangling). Mirrors the intent of
``slayer.engine.enrichment._allocate_hidden_name`` but is module-level and
shared by the generator.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, PrivateAttr


class AliasAllocator(BaseModel):
    """Per-generation collision-safe name allocator (mutable)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # External names the allocator must avoid (user columns, join aliases,
    # public projection aliases, model names).
    _reserved: set[str] = PrivateAttr(default_factory=set)
    # Names already handed out by this allocator.
    _used: set[str] = PrivateAttr(default_factory=set)
    # Monotonic ``_val_<n>`` cursor (never reset per scope, so sibling scopes
    # in one generation cannot mint the same ``_val_0``).
    _val_seq: int = PrivateAttr(default=0)
    # Monotonic scope-id cursor.
    _scope_seq: int = PrivateAttr(default=0)

    def reserve(self, *names: str) -> None:
        """Mark ``names`` as taken so they are never allocated."""
        self._reserved.update(names)

    def _taken(self, name: str) -> bool:
        return name in self._reserved or name in self._used

    def allocate(self, preferred: str) -> str:
        """Return ``preferred`` if free, else ``preferred_2``, ``preferred_3``, …"""
        candidate = preferred
        suffix = 2
        while self._taken(candidate):
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        self._used.add(candidate)
        return candidate

    def allocate_val(self) -> str:
        """Return the next free ``_val_<n>`` materialisation alias."""
        while True:
            candidate = f"_val_{self._val_seq}"
            self._val_seq += 1
            if not self._taken(candidate):
                self._used.add(candidate)
                return candidate

    def allocate_cte(self, preferred: str) -> str:
        """Return a collision-safe CTE name (same walk as :meth:`allocate`)."""
        return self.allocate(preferred)

    def next_scope_id(self, root_relation: str) -> str:
        """Return a generation-local ``ScopeFrame`` id, ``<root>#<seq>``.

        Ephemeral — used only for in-generation materialisation dedup; it is
        never emitted into SQL, result keys, or persisted state (D-F / Codex L1).
        """
        scope_id = f"{root_relation}#{self._scope_seq}"
        self._scope_seq += 1
        return scope_id
