"""YAML-based storage for models and datasources.

v4 (DEV-1330): models live under ``<base_dir>/models/<data_source>/<name>.yaml``
so two datasources sharing a table name don't collide. The datasource priority
list — used to disambiguate bare-name lookups — is stored at
``<base_dir>/priority.yaml``.

On open, ``migrate_yaml_layout`` walks the legacy flat layout and moves each
file into the new subdirectory. See ``slayer/storage/v4_migration.py`` for the
contract details.
"""

import os
from typing import List, Optional, Tuple

import yaml
from pydantic import ValidationError

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage.base import StorageBackend, _validate_path_component
from slayer.storage.v4_migration import migrate_yaml_layout


class YAMLStorage(StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.models_dir = os.path.join(base_dir, "models")
        self.datasources_dir = os.path.join(base_dir, "datasources")
        self._priority_path = os.path.join(base_dir, "priority.yaml")
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.datasources_dir, exist_ok=True)
        # Idempotent — moves any pre-v4 flat files into <data_source>/ subdirs.
        migrate_yaml_layout(base_dir)

    # ---- internal helpers --------------------------------------------------

    def _model_path(self, data_source: str, name: str) -> str:
        return os.path.join(self.models_dir, data_source, f"{name}.yaml")

    # ---- model CRUD --------------------------------------------------------

    async def save_model(self, model: SlayerModel) -> None:
        target_dir = os.path.join(self.models_dir, model.data_source)
        os.makedirs(target_dir, exist_ok=True)
        path = os.path.join(target_dir, f"{model.name}.yaml")
        data = model.model_dump(mode="json", exclude_none=True)
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)

    async def _list_all_model_identities(self) -> List[Tuple[str, str]]:
        result: List[Tuple[str, str]] = []
        if not os.path.isdir(self.models_dir):
            return result
        for ds in sorted(os.listdir(self.models_dir)):
            ds_dir = os.path.join(self.models_dir, ds)
            if not os.path.isdir(ds_dir):
                continue
            for filename in sorted(os.listdir(ds_dir)):
                if filename.endswith((".yaml", ".yml")):
                    result.append((ds, filename.rsplit(".", 1)[0]))
        return result

    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[SlayerModel]:
        _validate_path_component(name, kind="model name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
        if data_source is None:
            identity = await self.resolve_model_identity(name)
            if identity is None:
                return None
            data_source, name = identity
        path = self._model_path(data_source, name)
        if not os.path.exists(path):  # NOSONAR(S6549) — name/data_source are sanitized by _validate_path_component above (rejects '..', path separators, NULs); SlayerModel Pydantic validators sanitize the save path
            return None
        with open(path) as f:
            data = yaml.safe_load(f)
        return SlayerModel.model_validate(data)

    async def delete_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> bool:
        _validate_path_component(name, kind="model name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
        if data_source is None:
            identity = await self.resolve_model_identity(name)
            if identity is None:
                return False
            data_source, name = identity
        path = self._model_path(data_source, name)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    # ---- datasource CRUD ---------------------------------------------------

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
        result = []
        for filename in sorted(os.listdir(self.datasources_dir)):
            if filename.endswith((".yaml", ".yml")):
                result.append(filename.rsplit(".", 1)[0])
        return result

    async def delete_datasource(self, name: str) -> bool:
        path = os.path.join(self.datasources_dir, f"{name}.yaml")
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    # ---- datasource priority -----------------------------------------------

    async def get_datasource_priority(self) -> List[str]:
        if not os.path.exists(self._priority_path):
            return []
        with open(self._priority_path) as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            data = yaml.safe_load(f) or {}
        priority = data.get("priority", [])
        if not isinstance(priority, list):
            return []
        return [str(p) for p in priority]

    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        with open(self._priority_path, "w") as f:  # NOSONAR(S7493) — YAMLStorage uses sync I/O inside async by design (CLAUDE.md, Async Architecture)
            yaml.dump({"priority": list(priority)}, f, sort_keys=False)
