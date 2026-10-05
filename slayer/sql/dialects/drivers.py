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
from slayer.sql.dialects.base import SqlDialect

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig

_DATASOURCE_DOCS = "https://docs.motley.ai/slayer/configuration/datasources/"


def _hint(dialect: SqlDialect, *, missing: str, default: bool) -> str:
    if not default:
        return (
            "install the driver named in your connection_string; "
            "SLayer's extras only install its default drivers."
        )
    if dialect.install_extra:
        return f"pip install 'motley-slayer[{dialect.install_extra}]'"
    return f"install the package providing {missing!r}; see {_DATASOURCE_DOCS} (Additional support)."


def _missing_driver(
    datasource: DatasourceConfig, *, dialect: SqlDialect, missing: str, default: bool, exc: Exception,
) -> MissingDriverError:
    return MissingDriverError(
        datasource_name=datasource.name, ds_type=datasource.type, missing=missing,
        hint=_hint(dialect, missing=missing, default=default), detail=str(exc),
    )


def _is_default_driver(
    dialect: SqlDialect, *, ds_type: str | None, url: URL, driver: str | None,
) -> bool:
    """Whether ``url`` selects SLayer's default driver; ``driver=None`` (none named) judges the backend alone."""
    backend, _, scheme_driver = (dialect.url_scheme or ds_type or "").partition("+")
    if url.get_backend_name() != backend:
        return False
    if driver is None or ("+" not in url.drivername and not scheme_driver):
        return True
    return driver in {dialect.sync_driver, dialect.async_driver, scheme_driver}


@contextlib.contextmanager
def plugin_errors(datasource: DatasourceConfig, url: str, *, dialect: SqlDialect) -> Generator[None, None, None]:
    """Translate a missing SQLAlchemy dialect plugin for ``url`` while the block loads it."""
    try:
        yield
    except NoSuchModuleError as exc:
        parsed = make_url(url)
        raise _missing_driver(
            datasource, dialect=dialect, missing=parsed.drivername, exc=exc,
            default=_is_default_driver(
                dialect, ds_type=datasource.type, url=parsed,
                driver=parsed.drivername.partition("+")[2] or None,
            ),
        ) from exc


def import_driver(module: str, *, datasource: DatasourceConfig, dialect: SqlDialect) -> ModuleType:
    """Import one of ``dialect``'s own vendor driver modules."""
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise _missing_driver(
            datasource, dialect=dialect, missing=exc.name or module, default=True, exc=exc,
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
        raise _missing_driver(
            datasource, dialect=dialect, missing=exc.name or sa_dialect.driver, exc=exc,
            default=_is_default_driver(
                dialect, ds_type=datasource.type, url=parsed, driver=sa_dialect.driver,
            ),
        ) from exc
    for module in dialect.deferred_driver_modules(url):
        import_driver(module, datasource=datasource, dialect=dialect)
