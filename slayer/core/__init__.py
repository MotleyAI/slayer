"""SLayer core package — domain models and query types.

NamedQuery (in models.py) holds a forward reference to SlayerQuery (in
query.py); resolving it at the bottom of either module would create an
import cycle, so the rebuild is performed here once both modules are loaded.
"""

from slayer.core import models, query

models.NamedQuery.model_rebuild(_types_namespace={"SlayerQuery": query.SlayerQuery})
