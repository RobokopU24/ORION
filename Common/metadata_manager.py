
import os
import json


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
        self.metadata['previous_load_version'] = None
        self.metadata['has_sequence_variants'] = False
        self.reset_state_metadata()

    def reset_state_metadata(self):
        self.metadata['update_status'] = self.NOT_STARTED
        self.metadata['update_time'] = ''
        self.metadata['update_info'] = {}
        self.metadata['update_error'] = ''
        self.metadata['normalization_status'] = self.WAITING_ON_DEPENDENCY
        self.metadata['normalization_time'] = ''
        self.metadata['normalization_info'] = {}
        self.metadata['normalization_error'] = ''
        self.metadata['supplementation_status'] = self.WAITING_ON_DEPENDENCY
        self.metadata['supplementation_time'] = ''
        self.metadata['supplementation_info'] = {}
        self.metadata['supplementation_error'] = ''

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
        return self.metadata['update_error']

    def set_version_update_error(self, update_error: str):
        self.metadata['update_error'] = update_error
        self.save_metadata()

    def get_update_error(self):
        self.load_current_metadata()
        return self.metadata['update_error']

    def set_normalization_status(self, normalization_status: str):
        self.metadata['normalization_status'] = normalization_status
        self.save_metadata()

    def get_normalization_status(self):
        self.load_current_metadata()
        return self.metadata['normalization_status']

    def set_normalization_error(self, normalization_error: str):
        self.metadata['normalization_error'] = normalization_error
        self.save_metadata()

    def get_normalization_error(self):
        self.load_current_metadata()
        return self.metadata['normalization_error']

    def set_supplementation_status(self, supplementation_status: str):
        self.metadata['supplementation_status'] = supplementation_status
        self.save_metadata()

    def get_supplementation_status(self):
        self.load_current_metadata()
        return self.metadata['supplementation_status']

    def set_supplementation_error(self, supplementation_error: str):
        self.metadata['supplementation_error'] = supplementation_error
        self.save_metadata()

    def get_supplementation_error(self):
        self.load_current_metadata()
        return self.metadata['supplementation_error']

    def get_source_version(self):
        self.load_current_metadata()
        return self.metadata['source_version']

    def get_load_version(self):
        self.load_current_metadata()
        return self.metadata['load_version']

    def update_version(self, new_version: str):
        current_version = self.metadata['source_version']
        if current_version:
            self.metadata['previous_load_version'] = self.metadata['load_version']
            self.metadata['load_version'] += 1
        self.metadata['source_version'] = new_version
        self.save_metadata()

    def set_update_info(self, update_info: dict, update_time: str, has_sequence_variants: bool = False):
        self.metadata['update_info'] = update_info
        self.metadata['update_time'] = update_time
        self.metadata['has_sequence_variants'] = has_sequence_variants
        self.save_metadata()

    def has_sequence_variants(self):
        return self.metadata['has_sequence_variants']

    def set_normalization_info(self, normalization_info: dict, normalization_time: str):
        self.metadata['normalization_info'] = normalization_info
        self.metadata['normalization_time'] = normalization_time
        self.save_metadata()

    def set_supplementation_info(self, supplementation_info: dict, supplementation_time: str):
        self.metadata['supplementation_info'] = supplementation_info
        self.metadata['supplementation_time'] = supplementation_time
        self.save_metadata()

    def save_metadata(self):
        with open(self.metadata_file_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file, indent=4)

    def archive_metadata(self):
        last_load_version = self.get_load_version()
        archive_path = os.path.join(self.storage_directory, f'{self.source_id}_{last_load_version}.meta.json')
        with open(archive_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file, indent=4)
        self.reset_state_metadata()

    def get_previous_load_version(self):
        return self.metadata['previous_load_version']
