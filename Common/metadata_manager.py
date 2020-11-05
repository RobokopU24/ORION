
import os
import json
import datetime


class MetadataManager:

    NOT_STARTED = 'not_started'
    WAITING_ON_DEPENDENCY = 'waiting'
    STABLE = 'stable'
    IN_PROGRESS = 'in_progress'
    BROKEN = 'broken'
    FAILED = 'failed'

    def __init__(self, source_id: str, data_storage_dir: str):
        self.metadata = {}
        self.source_id = source_id
        self.storage_directory = data_storage_dir
        self.metadata_file_path = os.path.join(self.storage_directory, f'{source_id}.meta.json')
        self.load_current_metadata()

    def load_current_metadata(self):
        if os.path.isfile(self.metadata_file_path):
            with open(self.metadata_file_path) as meta_json_file:
                self.metadata = json.load(meta_json_file)
        else:
            self.init_metadata()

    def init_metadata(self):
        self.metadata['source_id'] = self.source_id
        self.metadata['source_version'] = None
        self.metadata['load_version'] = 1
        self.metadata['load_date'] = None
        self.metadata['previous_version'] = None
        self.metadata['update_status'] = self.NOT_STARTED
        self.metadata['normalization_status'] = self.WAITING_ON_DEPENDENCY
        self.metadata['annotation_status'] = self.WAITING_ON_DEPENDENCY
        self.metadata['source_specific'] = {}

    def set_update_status(self, update_status: str):
        self.metadata['update_status'] = update_status
        self.save_metadata()

    def get_update_status(self):
        self.load_current_metadata()
        return self.metadata['update_status']

    def set_update_error(self, update_error: str):
        self.metadata['update_error'] = update_error
        self.save_metadata()

    def get_update_error(self):
        self.load_current_metadata()
        return self.metadata['update_status']

    def set_normalization_status(self, normalization_status: str):
        self.metadata['normalization_status'] = normalization_status
        self.save_metadata()

    def get_normalization_status(self):
        self.load_current_metadata()
        return self.metadata['normalization_status']

    def set_annotation_status(self, annotation_status: str):
        self.metadata['annotation_status'] = annotation_status
        self.save_metadata()

    def get_annotation_status(self):
        self.load_current_metadata()
        return self.metadata['annotation_status']

    def get_source_version(self):
        self.load_current_metadata()
        return self.metadata['source_version']

    def get_load_version(self):
        self.load_current_metadata()
        return self.metadata['load_version']

    def update_version(self, new_version: str):
        current_version = self.metadata['source_version']
        if current_version:
            self.metadata['previous_version'] = self.metadata['load_version']
            self.metadata['load_version'] += 1
        self.metadata['source_version'] = new_version
        self.metadata['load_date'] = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
        self.save_metadata()

    def update_metadata(self, metadata: dict):
        self.metadata['source_specific'] = metadata
        self.save_metadata()

    def save_metadata(self):
        with open(self.metadata_file_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file, indent=4)

    def archive_metadata(self):
        archive_path = os.path.join(self.storage_directory, f'{self.source_id}_{self.metadata["load_version"]}.meta.json')
        with open(archive_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file, indent=4)

    def get_previous_version(self):
        return self.metadata['previous_version']

    def __delete_metadata_file(self):
        if os.path.isfile(self.metadata_file_path):
            os.remove(self.metadata_file_path)

    def __delete_all_metadata_files(self):
        for previous_version in self.metadata['previous_versions']:
            archive_path = os.path.join(self.storage_directory,
                                        f'{self.source_id}_{previous_version}.meta.json')
            if os.path.isfile(archive_path):
                os.remove(archive_path)
