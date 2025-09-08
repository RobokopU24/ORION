import os
import pytest
from orion.metadata import SourceMetadata

test_storage_dir = os.path.dirname(os.path.abspath(__file__)) + '/storage'

testing_source_id_1 = "TestingService"
testing_source_version_1 = "version_1"

@pytest.fixture
def meta_manager():

    yield SourceMetadata(testing_source_id_1, testing_source_version_1, test_storage_dir)

    # clean up any potential temp metadata files
    metadata_file = os.path.join(test_storage_dir, f'{testing_source_id_1}.meta.json')
    if os.path.isfile(metadata_file):
        os.remove(metadata_file)


def test_metadata_manager(meta_manager):
    # those tests were obsolete... TODO: a lot of testing
    pass



