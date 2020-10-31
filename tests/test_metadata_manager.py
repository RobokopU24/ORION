import os
import pytest
from Common.metadata_manager import MetadataManager

test_storage_dir = os.path.dirname(os.path.abspath(__file__)) + '/storage'

testing_source_id_1 = "TestingService"


@pytest.fixture
def meta_manager():

    yield MetadataManager(testing_source_id_1, test_storage_dir)

    # clean up any potential temp metadata files
    metadata_file = os.path.join(test_storage_dir, f'{testing_source_id_1}.meta.json')
    if os.path.isfile(metadata_file):
        os.remove(metadata_file)
    for i in range(5):
        metadata_file = os.path.join(test_storage_dir, f'{testing_source_id_1}_{i}.meta.json')
        if os.path.isfile(metadata_file):
            os.remove(metadata_file)


def test_metadata_manager(meta_manager):

    # just created, should not have a latest version
    assert meta_manager.get_source_version() is None
    assert meta_manager.get_source_version() != 'version_1'
    meta_manager.update_version('version_1')
    assert meta_manager.get_source_version() == 'version_1'

    # save the file and then parse it again
    meta_manager.save_metadata()
    meta_manager = MetadataManager(testing_source_id_1, test_storage_dir)
    assert meta_manager.get_source_version() == 'version_1'
    meta_manager.archive_metadata()

    meta_manager.update_version('version_2')
    assert meta_manager.get_source_version() == 'version_2'
    meta_manager.archive_metadata()
    meta_manager.update_version('version_3')
    meta_manager.archive_metadata()
    meta_manager.update_version('version_4')

    # save the file and then parse it again
    meta_manager.save_metadata()
    meta_manager = MetadataManager(testing_source_id_1, test_storage_dir)
    previous_version = meta_manager.get_previous_version()
    assert meta_manager.get_source_version() == 'version_4'
    assert previous_version == 3





