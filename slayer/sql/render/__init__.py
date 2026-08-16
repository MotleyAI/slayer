"""Render package — the incremental split of ``generator.py``.

``generator.py`` is ~10k lines because every render path grew where it was
first needed. Each consolidation PR moves one coherent responsibility here.

* :mod:`.value_expr` — one ``ValueKey`` → sqlglot-AST renderer, so a given key
  renders identically wherever it appears.
* :mod:`.aggregates` — one registry for aggregation rendering.
* :mod:`.parse` — one parse of free SQL text into a SLayer-normalised sqlglot
  AST, shared by the generator and the Mode-A door on ``ScopeFrame``.
* :mod:`.joins` — the one grain join-back builder (P-I).
* :mod:`.order_terms` — the one ``OrderEntry`` → ``ORDER BY`` term resolver.
* :mod:`.cte_assembly` — WITH-chain assembly in dependency-declared topological
  order.
* :mod:`.ranked` — the one ranked (``first`` / ``last``) CTE shape.

Nothing here imports ``generator``; the dependency runs one way.
"""
