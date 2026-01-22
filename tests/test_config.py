import os
from unittest import mock
from pathlib import Path
from Common.config import CONFIG

@mock.patch.dict(os.environ, {
    "ORION_GRAPHS": str(Path.cwd()/"tmp/orion_graphs"),
})

def test_config_created_from_env_vars():
    if (Path.cwd()/"tmp/orion_graphs").exists():
        os.rmdir(Path.cwd()/"tmp/orion_graphs")

    CONFIG.refresh()
    assert(CONFIG.ORION_STORAGE == Path.cwd()/"storage/orion_storage")
    assert(CONFIG.ORION_GRAPHS == Path.cwd()/"tmp/orion_graphs")
    assert(CONFIG.SHARED_SOURCE_DATA_PATH == Path("Storage/SHARED_DATA"))
    assert(CONFIG.ORION_TEST_MODE)

    CONFIG.get_path('ORION_GRAPHS')
    assert(CONFIG['ORION_GRAPHS'].exists())
    os.rmdir(Path.cwd()/"tmp/orion_graphs")
