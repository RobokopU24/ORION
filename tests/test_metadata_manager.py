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
    assert meta_manager.is_latest_version(None)
    assert meta_manager.is_latest_version('version_1') is False
    meta_manager.update_version('version_1')
    assert meta_manager.is_latest_version('version_1') is True

    # save the file and then parse it again
    meta_manager.save_metadata()
    meta_manager = MetadataManager(testing_source_id_1, test_storage_dir)
    assert meta_manager.is_latest_version('version_1') is True
    meta_manager.archive_metadata()

    meta_manager.update_version('version_2')
    assert meta_manager.is_latest_version('version_2') is True
    meta_manager.archive_metadata()
    meta_manager.update_version('version_3')
    meta_manager.archive_metadata()
    meta_manager.update_version('version_4')

    # save the file and then parse it again
    meta_manager.save_metadata()
    meta_manager = MetadataManager(testing_source_id_1, test_storage_dir)
    previous_versions = meta_manager.get_previous_versions()
    assert meta_manager.is_latest_version('version_4') is True
    assert 1 in previous_versions
    assert 3 in previous_versions





