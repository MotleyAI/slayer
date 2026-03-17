"""Abstract storage protocol."""

from abc import ABC, abstractmethod
from typing import List, Optional

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
