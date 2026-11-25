"""The distinct projection of an untrusted to-one hop reads only the hop's own columns."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.sql.untrusted_projection import mark_untrusted, project_untrusted_hops

SQL = ("SELECT c.name, COUNT(*) FROM orders AS o LEFT JOIN customers AS c ON o.cid = c.id "
       "WHERE EXISTS (SELECT 1 FROM flags AS c WHERE c.flag = 1) GROUP BY c.name")


def test_a_subquerys_own_alias_shadows_the_hop() -> None:
    """``flags AS c`` inside the ``EXISTS`` is not the joined ``customers AS c``: the filter stays outside."""
    statement = sqlglot.parse_one(SQL)
    assert isinstance(statement, exp.Select)
    mark_untrusted(statement.args["joins"][0].this)
    projected = project_untrusted_hops(statement)
    hop = projected.args["joins"][0].this
    assert isinstance(hop, exp.Subquery)
    assert [e.alias_or_name for e in hop.this.expressions] == ["id", "name"]
    assert not list(hop.find_all(exp.Exists))
    assert isinstance(projected.args["where"].this, exp.Exists)
