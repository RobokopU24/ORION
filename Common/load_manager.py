
import os
import multiprocessing

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer
from Common.metadata_manager import MetadataManager as Metadata
from GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader


GWAS_CATALOG = 'GWASCatalog'

all_sources = {GWAS_CATALOG}

source_data_loader_classes = {
    GWAS_CATALOG: GWASCatalogLoader
}


class SourceDataLoadManager:

    logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self, data_storage_dir: str = None):

        if data_storage_dir:
            self.data_storage_dir = data_storage_dir
        elif 'DATA_SERVICES_STORAGE' in os.environ:
            self.data_storage_dir = os.environ["DATA_SERVICES_STORAGE"]
        else:
            raise IOError('SourceDataLoadManager - specify the storage directory with environment variable DATA_SERVICES_STORAGE.')

        self.metadata = {}

    def start(self):

        self.load_previous_metadata()

        pool_size = multiprocessing.cpu_count() - 1

        self.logger.info(f'Checking for sources to update...')
        sources_to_update = self.check_sources_for_updates()
        self.logger.info(f'Updating {len(sources_to_update)} sources...')
        update_func = self.update_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(update_func, sources_to_update)
        pool.close()

        self.logger.info(f'Checking for sources to normalize...')
        sources_to_normalize = self.check_sources_for_normalization()
        self.logger.info(f'Normalizing {len(sources_to_normalize)} sources...')
        normalize_func = self.normalize_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(normalize_func, sources_to_normalize)
        pool.close()

        self.logger.info(f'Checking for sources to annotate...')
        sources_to_annotate = self.check_sources_for_annotation()
        self.logger.info(f'Annotating {len(sources_to_annotate)} sources...')
        annotate_func = self.annotate_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(annotate_func, sources_to_annotate)
        pool.close()

    def load_previous_metadata(self):
        for source_id in all_sources:
            self.metadata[source_id] = Metadata(source_id, self.data_storage_dir)

    def check_sources_for_updates(self):
        sources_to_update = []
        for source_id, loader_class in source_data_loader_classes.items():
            update_status = self.metadata[source_id].get_update_status()
            if update_status == Metadata.NOT_STARTED:
                sources_to_update.append(source_id)
            elif update_status == Metadata.IN_PROGRESS:
                continue
            elif update_status == Metadata.BROKEN:
                pass
                # TODO - log and report errors
            elif update_status == Metadata.FAILED:
                pass
                # TODO - log and report errors
            else:
                loader = loader_class()
                latest_source_version = loader.get_latest_source_version()
                if latest_source_version != self.metadata[source_id].get_source_version():
                    self.metadata[source_id].archive_metadata()
                    sources_to_update.append(source_id)
        return sources_to_update

    def update_source(self, source_id: str):
        self.logger.info(f"Updating {source_id}")
        # create an instance of the appropriate loader using the source_data_loader_classes lookup map
        source_data_loader = source_data_loader_classes[source_id]()
        # update the version and load information
        self.metadata[source_id].set_update_status(Metadata.IN_PROGRESS)
        latest_source_version = source_data_loader.get_latest_source_version()
        self.metadata[source_id].update_version(latest_source_version)
        # call the loader - retrieve/parse data and write to a kgx file
        out_file_name = f'{source_id}_{self.metadata[source_id].get_load_version()}_source'
        load_meta_data = source_data_loader.load(self.data_storage_dir, out_file_name)

        # update the associated metadata
        self.logger.info(f"Updating {source_id} metadata...")
        self.metadata[source_id].update_metadata(load_meta_data)

        if load_meta_data["success"]:
            self.metadata[source_id].set_update_status(Metadata.STABLE)
        else:
            self.metadata[source_id].set_update_status(Metadata.FAILED)
        self.logger.info(f"Updating {source_id} complete.")

    def check_sources_for_normalization(self):
        sources_to_normalize = []
        for source_id in all_sources:
            normalization_status = self.metadata[source_id].get_normalization_status()
            if normalization_status == Metadata.NOT_STARTED:
                sources_to_normalize.append(source_id)
            elif ((normalization_status == Metadata.WAITING_ON_DEPENDENCY)
                  and (self.metadata[source_id].get_update_status() == Metadata.STABLE)):
                sources_to_normalize.append(source_id)
            elif normalization_status:
                # TODO - log and report errors
                pass
        return sources_to_normalize

    def normalize_source(self, source_id: str):
        normalizer = KGXFileNormalizer()
        # normalize the source data file
        self.metadata[source_id].set_normalization_status(Metadata.FAILED)
        pass

    def check_sources_for_annotation(self):
        sources_to_annotate = []
        for source_id in all_sources:
            annotation_status = self.metadata[source_id].get_annotation_status()
            if annotation_status == Metadata.NOT_STARTED:
                sources_to_annotate.append(source_id)
            elif ((annotation_status == Metadata.WAITING_ON_DEPENDENCY)
                  and (self.metadata[source_id].get_normalization_status() == Metadata.STABLE)):
                sources_to_annotate.append(source_id)
            elif annotation_status:
                # TODO - log and report errors
                pass
        return sources_to_annotate

    def annotate_source(self, source_id: str):
        pass


if __name__ == '__main__':
    load_manager = SourceDataLoadManager()
    load_manager.start()




