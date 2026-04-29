"""YAML-based storage for models, named queries, and datasources."""

import os
from typing import List, Optional

import yaml
from pydantic import ValidationError

from slayer.core.models import DatasourceConfig, NamedQuery, SlayerModel
from slayer.storage.base import StorageBackend


class YAMLStorage(StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.models_dir = os.path.join(base_dir, "models")
        self.queries_dir = os.path.join(base_dir, "queries")
        self.datasources_dir = os.path.join(base_dir, "datasources")
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.queries_dir, exist_ok=True)
        os.makedirs(self.datasources_dir, exist_ok=True)

    @staticmethod
    def _list_yaml_basenames(directory: str) -> List[str]:
        """Return sorted ``*.yaml`` / ``*.yml`` basenames (sans extension)."""
        result = []
        for filename in sorted(os.listdir(directory)):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                result.append(filename.rsplit(".", 1)[0])
        return result

    async def _persist_model(self, model: SlayerModel) -> None:
        path = os.path.join(self.models_dir, f"{model.name}.yaml")
        data = model.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def get_model(self, name: str) -> Optional[SlayerModel]:
        path = os.path.join(self.models_dir, f"{name}.yaml")
        if not os.path.exists(path):
            return None
        with open(path) as f:
            data = yaml.safe_load(f)
        return SlayerModel.model_validate(data)

    async def list_models(self) -> List[str]:
        return self._list_yaml_basenames(self.models_dir)

    async def delete_model(self, name: str) -> bool:
        path = os.path.join(self.models_dir, f"{name}.yaml")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    async def _persist_query(self, query: NamedQuery) -> None:
        path = os.path.join(self.queries_dir, f"{query.name}.yaml")
        data = query.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def get_query(self, name: str) -> Optional[NamedQuery]:
        path = os.path.join(self.queries_dir, f"{name}.yaml")
        if not os.path.exists(path):
            return None
        with open(path) as f:
            data = yaml.safe_load(f)
        return NamedQuery.model_validate(data)

    async def list_queries(self) -> List[str]:
        return self._list_yaml_basenames(self.queries_dir)

    async def delete_query(self, name: str) -> bool:
        path = os.path.join(self.queries_dir, f"{name}.yaml")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        path = os.path.join(self.datasources_dir, f"{datasource.name}.yaml")
        data = datasource.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            ds = DatasourceConfig.model_validate(data)
            return ds.resolve_env_vars()
        except yaml.YAMLError as exc:
            raise ValueError(
                f"Datasource '{name}': invalid YAML in {path} — {exc}"
            ) from exc
        except ValidationError as exc:
            raise ValueError(
                f"Datasource '{name}': invalid config — {exc}"
            ) from exc

    async def list_datasources(self) -> List[str]:
        return self._list_yaml_basenames(self.datasources_dir)

    async def delete_datasource(self, name: str) -> bool:
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False
