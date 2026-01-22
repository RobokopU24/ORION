import os
from unittest import mock
from pathlib import Path

@mock.patch.dict(os.environ, {
    "ORION_GRAPHS": "/some/path",
})

def test_config_created_from_env_vars():
    from Common.config import CONFIG

    assert(CONFIG.ORION_STORAGE == Path.cwd()/"storage/orion_storage")
    assert(CONFIG.ORION_GRAPHS == Path("/some/path"))
    assert(CONFIG.SHARED_SOURCE_DATA_PATH == Path("Storage/SHARED_DATA"))
    assert(CONFIG.ORION_TEST_MODE)

    CONFIG.get_path('ORION_STORAGE')
    assert(CONFIG['ORION_STORAGE'].exists())
