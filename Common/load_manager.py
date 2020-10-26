
import os

from Common.metadata_manager import MetadataManager, BROKEN, IN_PROGRESS, FAILED, STABLE
from GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader


GWAS_CATALOG = 'GWASCatalog'

all_sources = {GWAS_CATALOG}

source_data_loader_classes = {
    GWAS_CATALOG: GWASCatalogLoader
}


class SourceDataLoadManager:

    def __init__(self, data_storage_dir: str = None):

        if data_storage_dir:
            self.data_storage_dir = data_storage_dir
        elif 'DATA_SERVICES_STORAGE' in os.environ:
            self.data_storage_dir = os.environ["DATA_SERVICES_STORAGE"]
        else:
            raise IOError('SourceDataLoadManager - specify the storage directory with environment variable DATA_SERVICES_STORAGE.')

        self.metadata_managers = {}

    def start(self):

        self.load_previous_metadata()

        sources_to_update = self.check_sources_for_updates()
        if sources_to_update:
            first_source = sources_to_update.pop()
            self.update_source(first_source)
            for other_source in sources_to_update:
                # TODO - send these sources off to be processed on other instances?
                self.update_source(other_source)

        sources_to_normalize = self.check_sources_for_normalization()
        if sources_to_normalize:
            first_source = sources_to_normalize.pop()
            self.normalize_source(first_source)
            for other_source in sources_to_normalize:
                # TODO - send these sources off to be processed on other instances?
                self.normalize_source(other_source)

    def load_previous_metadata(self):
        for source_id in all_sources:
            self.metadata_managers[source_id] = MetadataManager(source_id, self.data_storage_dir)

    def check_sources_for_updates(self):
        sources_to_update = []
        for source_id, loader_class in source_data_loader_classes.items():

            update_status = self.metadata_managers[source_id].get_update_status()

            if ((update_status is not IN_PROGRESS) and
                    (update_status is not BROKEN) and
                    (update_status is not FAILED)):
                loader = loader_class()
                latest_source_version = loader.get_latest_source_version()
                if not self.metadata_managers[source_id].is_latest_version(latest_source_version):
                    sources_to_update.append(source_id)
        return sources_to_update

    def update_source(self, source_id: str):
        source_data_loader = source_data_loader_classes[source_id]()
        load_meta_data = source_data_loader.load()
        self.metadata_managers[source_id].update_metadata(load_meta_data)
