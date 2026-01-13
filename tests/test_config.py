from Common.config import Config
import os
from unittest import mock
from pathlib import Path

@mock.patch.dict(os.environ, {
    "STORAGE_BASE_PATH":"tmp",
    "ORION_GRAPHS_DIR_NAME": "custom_orion_graphs"
})
def test_config_created_from_env_vars():
    if Path(f"{os.getcwd()}/tmp/").exists():
        print("Removing already existing storage directory")
        import shutil
        shutil.rmtree(f"{os.getcwd()}/tmp/")

    cfg = Config.from_env()

    print(cfg.base_path)
    assert("tmp" in str(cfg.base_path))
    
    print("-----------")
    print(cfg.orion_graphs_path)
    print(Path(cfg.orion_graphs_path).exists())

    assert (not Path(cfg.orion_graphs_path).exists())
    assert (not Path(cfg.shared_source_data_path).exists())

    orion_graphs_path = cfg.getenv("ORION_GRAPHS_DIR_NAME")
    assert (Path(orion_graphs_path).exists())
    assert (cfg.orion_logs_path == '')
    storage_path = cfg.getenv("ORION_STORAGE_DIR_NAME")
    print(storage_path)
    assert(Path(storage_path).exists())

    assert(cfg.getenv("ORION_GRAPH_SPEC") == "example-graph-spec.yaml" )