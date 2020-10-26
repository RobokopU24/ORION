
import os
import json
import datetime

NOT_STARTED = 'not_started'
WAITING_ON_DEPENDENCY = 'waiting'
STABLE = 'stable'
IN_PROGRESS = 'in_progress'
BROKEN = 'broken'
FAILED = 'failed'

class MetadataManager:

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
        self.metadata['previous_versions'] = []
        self.metadata['update_status'] = NOT_STARTED
        self.metadata['normalization_status'] = WAITING_ON_DEPENDENCY
        self.metadata['annotation_status'] = WAITING_ON_DEPENDENCY

    def get_update_status(self):
        return self.metadata['update_status']

    def is_latest_version(self, new_version: str):
        if self.metadata['source_version'] == new_version:
            return True
        else:
            return False

    def update_version(self, new_version: str):
        current_version = self.metadata['source_version']
        if current_version:
            self.metadata['previous_versions'].append(self.metadata['load_version'])
            self.metadata['load_version'] += 1
        self.metadata['source_version'] = new_version

    def save_metadata(self):
        current_datetime = datetime.datetime.now()
        self.metadata['load_date'] = current_datetime.strftime('%m-%d-%y %H:%M:%S')
        with open(self.metadata_file_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file)

    def archive_metadata(self):
        archive_path = os.path.join(self.storage_directory, f'{self.source_id}_{self.metadata["load_version"]}.meta.json')
        with open(archive_path, 'w') as meta_json_file:
            json.dump(self.metadata, meta_json_file)

    def get_previous_versions(self):
        return self.metadata['previous_versions']

    def __delete_metadata_file(self):
        if os.path.isfile(self.metadata_file_path):
            os.remove(self.metadata_file_path)

    def __delete_all_metadata_files(self):
        for previous_version in self.metadata['previous_versions']:
            archive_path = os.path.join(self.storage_directory,
                                        f'{self.source_id}_{previous_version}.meta.json')
            if os.path.isfile(archive_path):
                os.remove(archive_path)














