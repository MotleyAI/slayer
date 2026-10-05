"""Driver pre-flight: a missing driver module or dialect plugin raises ``MissingDriverError`` with an install hint."""

from __future__ import annotations

import contextlib
import importlib
import inspect
from collections.abc import Callable, Generator
from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy.engine import Dialect
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import NoSuchModuleError

from slayer.core.errors import MissingDriverError
from slayer.sql.dialects.base import DriverFacts, SqlDialect

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig

_DATASOURCE_DOCS = "https://docs.motley.ai/slayer/configuration/datasources/"


def _hint(facts: DriverFacts, *, missing: str, default: bool) -> str:
    if not default:
        return (
            "install the driver named in your connection_string; "
            "SLayer's extras only install its default drivers."
        )
    if facts.install_extra:
        return f"pip install 'motley-slayer[{facts.install_extra}]'"
    return f"install the package providing {missing!r}; see {_DATASOURCE_DOCS} (Additional support)."


def _missing_driver(
    datasource: DatasourceConfig, *, facts: DriverFacts, missing: str, default: bool, exc: Exception,
) -> MissingDriverError:
    return MissingDriverError(
        datasource_name=datasource.name, ds_type=datasource.type, missing=missing,
        hint=_hint(facts, missing=missing, default=default), detail=str(exc),
    )


def _is_default_driver(facts: DriverFacts, *, url: URL, driver: str | None) -> bool:
    """Whether ``url`` selects SLayer's default driver; ``driver=None`` (none named) judges the backend alone."""
    if url.get_backend_name() not in facts.url_backends:
        return False
    scheme_driver = (facts.url_scheme or "").partition("+")[2]
    if driver is None or ("+" not in url.drivername and not scheme_driver):
        return True
    return driver in {facts.sync_driver, facts.async_driver, scheme_driver}


@contextlib.contextmanager
def plugin_errors(datasource: DatasourceConfig, url: str, *, dialect: SqlDialect) -> Generator[None, None, None]:
    """Translate a missing SQLAlchemy dialect plugin for ``url``, or a missing import of it, while the block loads it."""
    try:
        yield
    except (NoSuchModuleError, ImportError) as exc:
        parsed = make_url(url)
        facts = dialect.driver_facts(datasource.type)
        missing = (isinstance(exc, ImportError) and exc.name) or parsed.drivername
        raise _missing_driver(
            datasource, facts=facts, missing=missing, exc=exc,
            default=_is_default_driver(facts, url=parsed, driver=parsed.drivername.partition("+")[2] or None),
        ) from exc


def import_driver(module: str, *, datasource: DatasourceConfig, dialect: SqlDialect) -> ModuleType:
    """Import one of ``dialect``'s own vendor driver modules."""
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise _missing_driver(
            datasource, facts=dialect.driver_facts(datasource.type), missing=exc.name or module,
            default=True, exc=exc,
        ) from exc


def _dbapi_importer(sa_dialect: type[Dialect]) -> Callable[[], object]:
    """``create_engine``'s choice: an own ``import_dbapi`` wins over a legacy ``dbapi()`` classmethod."""
    legacy = getattr(sa_dialect, "dbapi", None)
    if "import_dbapi" not in sa_dialect.__dict__ and inspect.ismethod(legacy):
        return legacy
    return sa_dialect.import_dbapi


def load_driver(
    datasource: DatasourceConfig, url: str, *, dialect: SqlDialect, is_async: bool,
) -> None:
    """Load ``url``'s dialect and DBAPI exactly as engine construction would, failing typed."""
    parsed = make_url(url)
    with plugin_errors(datasource, url, dialect=dialect):
        sa_dialect = parsed.get_dialect(_is_async=is_async)
    try:
        _dbapi_importer(sa_dialect)()
    except ImportError as exc:
        facts = dialect.driver_facts(datasource.type)
        raise _missing_driver(
            datasource, facts=facts, missing=exc.name or sa_dialect.driver, exc=exc,
            default=_is_default_driver(facts, url=parsed, driver=sa_dialect.driver),
        ) from exc
    for module in dialect.deferred_driver_modules(url):
        import_driver(module, datasource=datasource, dialect=dialect)
