"""Abstract storage protocol and factory."""

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

from slayer.core.models import DatasourceConfig, SlayerModel


class StorageBackend(ABC):
    @abstractmethod
    def save_model(self, model: SlayerModel) -> None: ...

    @abstractmethod
    def get_model(self, name: str) -> Optional[SlayerModel]: ...

    @abstractmethod
    def list_models(self) -> List[str]: ...

    @abstractmethod
    def delete_model(self, name: str) -> bool: ...

    @abstractmethod
    def save_datasource(self, datasource: DatasourceConfig) -> None: ...

    @abstractmethod
    def get_datasource(self, name: str) -> Optional[DatasourceConfig]: ...

    @abstractmethod
    def list_datasources(self) -> List[str]: ...

    @abstractmethod
    def delete_datasource(self, name: str) -> bool: ...


# ---------------------------------------------------------------------------
# Storage factory with pluggable registry
# ---------------------------------------------------------------------------

_STORAGE_REGISTRY: Dict[str, Callable[[str], StorageBackend]] = {}


def register_storage(scheme: str, factory: Callable[[str], StorageBackend]) -> None:
    """Register a storage backend factory for a URI scheme.

    Example:
        register_storage("redis", lambda path: RedisStorage(url=path))
    """
    _STORAGE_REGISTRY[scheme.lower().strip()] = factory


def resolve_storage(path: str) -> StorageBackend:
    """Create a StorageBackend from a path or URI.

    Resolution order:
    1. URI scheme (e.g., "sqlite:///data.db", "yaml://./dir") → registered factory
    2. File extension .db/.sqlite/.sqlite3 → SQLiteStorage
    3. Everything else → YAMLStorage (directory)

    Third-party backends can register via register_storage().
    """
    # Check for URI scheme
    if "://" in path:
        scheme, _, remainder = path.partition("://")
        scheme = scheme.lower()
        if scheme in _STORAGE_REGISTRY:
            return _STORAGE_REGISTRY[scheme](remainder)
        # Built-in schemes
        if scheme == "yaml":
            from slayer.storage.yaml_storage import YAMLStorage

            return YAMLStorage(base_dir=remainder)
        if scheme == "sqlite":
            from slayer.storage.sqlite_storage import SQLiteStorage

            # sqlite:///abs/path → remainder="/abs/path" (keep absolute)
            # sqlite://rel/path → remainder="rel/path" (keep relative)
            db_path = remainder if remainder.startswith("/") else remainder.lstrip("/")
            return SQLiteStorage(db_path=db_path)
        raise ValueError(
            f"Unknown storage scheme '{scheme}'. "
            f"Built-in: yaml, sqlite. "
            f"Registered: {', '.join(_STORAGE_REGISTRY) or 'none'}. "
            f"Use register_storage() to add custom backends."
        )

    # Extension-based detection
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        from slayer.storage.sqlite_storage import SQLiteStorage

        return SQLiteStorage(db_path=path)

    # Default: YAML directory
    from slayer.storage.yaml_storage import YAMLStorage

    return YAMLStorage(base_dir=path)
