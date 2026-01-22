# import os
# from pathlib import Path
# from dotenv import dotenv_values

# CONFIG = {
#     **dotenv_values(Path(__file__).parents[1] / '.env'),  # load config variables from .env
#     **os.environ,  # override loaded values with environment variables
# }

from dataclasses import dataclass
from .utils import LoggingUtil
from pathlib import Path
from typing import ClassVar
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = LoggingUtil.init_logging("ORION.Common.config",
                                  line_format='medium')

class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file= Path(__file__).parent.parent/".env",
        env_file_encoding="utf-8",
    ) # Load .env file and OS ENV Variables.

    ORION_STORAGE: Path =  Path.cwd()/"storage/orion_storage"
    ORION_GRAPHS: Path =  Path.cwd()/"storage/orion_graphs"
    ORION_LOGS: Path | None = None
    SHARED_SOURCE_DATA_PATH: Path | None = None

    ORION_OUTPUT_URL: str="https://localhost/"
    ORION_TEST_MODE: bool=False

    ORION_GRAPH_SPEC: str="example-graph-spec.yaml"
    ORION_GRAPH_SPEC_URL: str=""

    BAGEL_SERVICE_USERNAME: str="default_bagel_username"
    BAGEL_SERVICE_PASSWORD: str="default_bagel_password"

    EDGE_NORMALIZATION_ENDPOINT: str="https://bl-lookup-sri.renci.org/"
    NODE_NORMALIZATION_ENDPOINT: str="https://nodenormalization-sri.renci.org/"
    NAMERES_URL: str="https://name-resolution-sri.renci.org/"
    SAPBERT_URL: str="https://babel-sapbert.apps.renci.org/"
    LITCOIN_PRED_MAPPING_URL: str="https://pred-mapping.apps.renci.org/"
    BAGEL_ENDPOINT: str="https://bagel.apps.renci.org/"
    
    # class method to get an instance of the class, with an option to be able to reload
    _instance: ClassVar["Config | None"] = None

    @classmethod
    def get(cls, refresh: bool = False) -> "Config":
        if cls._instance is None or refresh:
            cls._instance = cls()
        return cls._instance

    # Validation function for ORION_LOGS
    @field_validator("ORION_LOGS")
    @classmethod
    def validate_logs_path(cls, v: Path | None) -> Path | None:
        if v is None:
            return None
        if not v.exists():
            raise ValueError(f"ORION_LOGS path does not exist: {v}")
        elif not v.is_dir():
            raise ValueError(f"ORION_LOGS is not a directory: {v}")
        return v

    ## Making sure that either orion graph spec or orion graph spec url are set (not both)
    @model_validator(mode="after")
    def check_graph_spec(self) -> "Config":
        if self.ORION_GRAPH_SPEC and self.ORION_GRAPH_SPEC_URL:
            raise ValueError("Set either ORION_GRAPH_SPEC or ORION_GRAPH_SPEC_URL, not both")
        if not self.ORION_GRAPH_SPEC_URL and not self.ORION_GRAPH_SPEC:
            raise ValueError("Must set either ORION_GRAPH_SPEC or ORION_GRAPH_SPEC_URL")
        return self
    
    ## Make relevant directory, and return the path for orion_storage, and orion_graphs
    def get_path(self, name: str) -> Path:
        if name not in ("ORION_STORAGE", "ORION_GRAPHS"):
            raise ValueError(f"Unknown directory field: {name}")
        
        path = getattr(self, name)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Failed to create {name} directory: {e}")
        return path

class ConfigProxy:
    def __getattr__(self, name: str):
        return getattr(Config.get(), name)
    
    def __getitem__(self, name: str):
        return getattr(Config.get(), name)

    def refresh(self):
        Config.get(refresh=True)

CONFIG = ConfigProxy()