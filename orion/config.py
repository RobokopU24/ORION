from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    ORION_GRAPH_SPEC: str = "example-graph-spec.yaml"
    ORION_GRAPH_SPEC_URL: str = ""

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

config = Config()