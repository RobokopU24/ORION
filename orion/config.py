import logging
from pathlib import Path
from pydantic import PrivateAttr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# When ORION_STORAGE / ORION_GRAPHS are not set, fall back to this default workspace.
# Not a CWD relative path, we need this reliably reusable, shouldn't matter where they invoke ORION from.
# Not a repo-relative path because that won't work well for pypi users.
# So we settle on the per-user home directory.
DEFAULT_WORKSPACE_DIR = Path.home() / "ORION-workspace"

_logger = logging.getLogger("orion.config")


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent/".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True
    )

    @field_validator("*", mode="before")
    @classmethod
    def strip_trailing_slashes(cls, v, info):
        if isinstance(v, str) and info.field_name.endswith("_URL"):
            return v.rstrip("/")
        return v

    ORION_STORAGE: str | None = None
    ORION_GRAPHS: str | None = None
    ORION_LOGS: str | None = None

    ORION_OUTPUT_URL: str = "https://localhost"
    ORION_TEST_MODE: bool = False

    BL_VERSION: str = "v4.3.7"

    EDGE_NORMALIZATION_URL: str = "https://bl-lookup-sri.renci.org"
    NODE_NORMALIZATION_URL: str = "https://nodenormalization-sri.renci.org"

    # the following were used for the LitCoin project and may be removed in the future
    NAMERES_URL: str = "https://name-resolution-sri.renci.org"
    SAPBERT_URL: str = "https://babel-sapbert.apps.renci.org"
    SHARED_SOURCE_DATA_PATH: str = "/tmp/shared_data"
    LITCOIN_PRED_MAPPING_URL: str = "https://pred-mapping.apps.renci.org"
    BAGEL_URL: str = "https://bagel.apps.renci.org"
    BAGEL_SERVICE_USERNAME: str | None = None
    BAGEL_SERVICE_PASSWORD: str | None = None
    OPENAI_API_KEY: str | None = None
    OPENAI_API_ORGANIZATION: str | None = None

    # Tracks which fallback directories we've already announced so we log the resolved
    # path once per process instead of on every call.
    _announced_fallbacks: set[str] = PrivateAttr(default_factory=set)

    def get_storage_dir(self) -> str:
        return self._resolve_workspace_dir(self.ORION_STORAGE, "storage", "ORION_STORAGE")

    def get_graphs_dir(self) -> str:
        return self._resolve_workspace_dir(self.ORION_GRAPHS, "graphs", "ORION_GRAPHS")

    def _resolve_workspace_dir(self, env_value: str | None, subdir: str, env_name: str) -> str:
        if env_value:
            if not Path(env_value).is_dir():
                raise IOError(f'{env_name} is set to {env_value} but that directory does not exist.')
            return env_value
        path = DEFAULT_WORKSPACE_DIR / subdir
        path.mkdir(parents=True, exist_ok=True)
        if subdir not in self._announced_fallbacks:
            self._announced_fallbacks.add(subdir)
            _logger.info(f'Using default {subdir} directory: {path} (set {env_name} to override).')
        return str(path)


config = Config()
