"""SQLite-specific AST rewrites applied before SQL emission.

DEV-1331: sqlglot's default SQLite generator emits ``exp.JSONExtract`` as
``col -> '$.path'``. In SQLite the ``->`` operator returns the JSON-typed
form (e.g. ``'"Owned"'`` with literal quotes), whereas ``json_extract`` and
``->>`` (``exp.JSONExtractScalar``) return the unquoted scalar. The mismatch
silently breaks ``CASE WHEN`` / equality matches against bare-string
literals — the bug compiles, executes, and produces wrong answers.

The fix walks an sqlglot AST and rewrites every ``exp.JSONExtract`` node to
``exp.Anonymous(this='JSON_EXTRACT', expressions=[col, path])`` so the
emission is the canonical function-call form on every dialect. ``->>``
(``exp.JSONExtractScalar``) is left untouched — it is correct on SQLite and
the right answer on Postgres / MySQL too.
"""

from sqlglot import exp


def rewrite_sqlite_json_extract(node: exp.Expression) -> exp.Expression:
    """Rewrite every ``exp.JSONExtract`` in the tree rooted at ``node`` to the
    function-call form.

    Returns the (possibly new) root node — callers must use the return value
    because ``node`` itself may be a ``JSONExtract`` (e.g. when parsing a
    ``Column.sql`` whose entire expression is ``json_extract(col, path)``),
    in which case ``Expression.replace`` is a no-op (no parent to rewire)
    and a fresh root must be returned. Non-root rewrites happen in place.
    """
    if isinstance(node, exp.JSONExtract):
        return _to_anonymous(node)
    for je in list(node.find_all(exp.JSONExtract)):
        je.replace(_to_anonymous(je))
    return node


def _to_anonymous(je: exp.JSONExtract) -> exp.Anonymous:
    return exp.Anonymous(
        this="JSON_EXTRACT",
        expressions=[je.this.copy(), je.expression.copy()],
    )
