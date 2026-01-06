from Common.config import Config
import os
from unittest import mock
from pathlib import Path

@mock.patch.dict(os.environ, {
    "STORAGE_BASE_PATH":"tmp",
    "ORION_STORAGE_DIR_NAME": "custom_orion_storage",
    "ORION_GRAPHS_DIR_NAME": "custom_orion_graphs"
})
def test_config_created_from_env_vars():
    cfg = Config.from_env()

    assert (Path(cfg.orion_graphs_path).exists())
    assert(Path(cfg.shared_source_data_path).exists())