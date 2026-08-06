"""Render package — the incremental split of ``generator.py``.

``generator.py`` is ~10k lines because every render path grew where it was
first needed. Each consolidation PR moves one coherent responsibility here.

* :mod:`.value_expr` — one ``ValueKey`` → sqlglot-AST renderer, so a given key
  renders identically wherever it appears.
* :mod:`.aggregates` — one registry for aggregation rendering.

Nothing here imports ``generator``; the dependency runs one way.
"""
