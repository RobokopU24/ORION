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

    # assert cfg.orion_storage_path == "../tmp/ORION_STORAGE"
    # assert cfg.orion_graph_path == "../tmp/ORION_KG"
    # assert cfg.shared_source_data_path == "../../Storage/shared_data"
    assert (Path(cfg.orion_graphs_path).exists())
    assert(Path(cfg.shared_source_data_path).exists())