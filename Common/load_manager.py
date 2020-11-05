
import os
import multiprocessing

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
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

    def __init__(self, storage_dir: str = None):

        if storage_dir:
            self.storage_dir = storage_dir
        elif 'DATA_SERVICES_STORAGE' in os.environ:
            self.storage_dir = os.environ["DATA_SERVICES_STORAGE"]
        else:
            raise IOError('SourceDataLoadManager - specify the storage directory with environment variable DATA_SERVICES_STORAGE.')

        # dict of data_source_id -> MetadataManager object
        self.metadata = {}
        # dict of data_source_id -> latest source version (to prevent double lookups)
        self.new_version_lookup = {}

    def start(self):

        self.load_previous_metadata()

        # TODO determine multiprocessing pool size by deployment capabilities
        pool_size = 6

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
            self.metadata[source_id] = Metadata(source_id, self.storage_dir)

    def check_sources_for_updates(self):
        sources_to_update = []
        for source_id, loader_class in source_data_loader_classes.items():
            source_metadata = self.metadata[source_id]

            update_status = source_metadata.get_update_status()
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
                if latest_source_version != source_metadata.get_source_version():
                    source_metadata.archive_metadata()
                    sources_to_update.append(source_id)
                    self.new_version_lookup[source_id] = latest_source_version
        return sources_to_update

    def update_source(self, source_id: str):
        self.logger.info(f"Updating source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_update_status(Metadata.IN_PROGRESS)
        try:
            # create an instance of the appropriate loader using the source_data_loader_classes lookup map
            source_data_loader = source_data_loader_classes[source_id]()
            # update the version and load information
            if source_id in self.new_version_lookup:
                latest_source_version = self.new_version_lookup[source_id]
            else:
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = source_data_loader.get_latest_source_version()
            source_metadata.update_version(latest_source_version)
            # call the loader - retrieve/parse data and write to a kgx file
            self.logger.info(f"Loading {source_id}...")
            out_file_name = f'{source_id}_source_{source_metadata.get_load_version()}'
            nodes_output_file_path = os.path.join(self.storage_dir, f'{out_file_name}_nodes.json')
            edges_output_file_path = os.path.join(self.storage_dir, f'{out_file_name}_edges.json')
            load_meta_data = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            self.logger.info(f"Updating {source_id} metadata...")
            source_metadata.set_update_status(Metadata.STABLE)
            source_metadata.update_metadata(load_meta_data)
            self.logger.info(f"Updating {source_id} complete.")

        except SourceDataBrokenError as broken_error:
            source_metadata.set_update_error(broken_error.error_message)
            source_metadata.set_update_status(Metadata.BROKEN)

        except SourceDataFailedError as failed_error:
            source_metadata.set_update_error(failed_error.error_message)
            source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

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




