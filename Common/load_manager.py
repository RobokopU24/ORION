import argparse
import os
import multiprocessing

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer, NormalizationBrokenError, NormalizationFailedError
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
from GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader


GWAS_CATALOG = 'GWASCatalog'

ALL_SOURCES = [GWAS_CATALOG]

SOURCES_WITH_VARIANTS = [GWAS_CATALOG]

source_data_loader_classes = {
    GWAS_CATALOG: GWASCatalogLoader
}


class SourceDataLoadManager:

    logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self, storage_dir: str = None):

        self.storage_dir = storage_dir
        self.init_storage_dir()

        # dict of data_source_id -> MetadataManager object
        self.metadata = {}
        # dict of data_source_id -> latest source version (to prevent double lookups)
        self.new_version_lookup = {}

        self.file_normalizer = None

        self.load_previous_metadata()

    def start(self):

        # TODO determine multiprocessing pool size by deployment capabilities
        pool_size = 6

        self.logger.info(f'Checking for sources to update...')
        sources_to_update = self.check_sources_for_updates()
        self.logger.info(f'Updating {len(sources_to_update)} sources: {repr(sources_to_update)}')
        update_func = self.update_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(update_func, sources_to_update)
        pool.close()

        self.logger.info(f'Checking for sources to normalize...')
        sources_to_normalize = self.check_sources_for_normalization()
        self.logger.info(f'Normalizing {len(sources_to_normalize)} sources: {repr(sources_to_normalize)}')
        # TODO can we really do this in parallel or will the normalization services barf?
        normalize_func = self.normalize_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(normalize_func, sources_to_normalize)
        pool.close()

        self.logger.info(f'Checking for sources to annotate...')
        sources_to_annotate = self.check_sources_for_annotation()
        self.logger.info(f'Annotating {len(sources_to_annotate)} sources: {repr(sources_to_annotate)}')
        annotate_func = self.annotate_source
        pool = multiprocessing.Pool(pool_size)
        pool.map(annotate_func, sources_to_annotate)
        pool.close()

    def load_previous_metadata(self):
        for source_id in ALL_SOURCES:
            self.metadata[source_id] = Metadata(source_id, self.get_source_dir_path(source_id))

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
            elif update_status == Metadata.FAILED:
                pass
                # TODO do we want to retry these automatically?

            else:
                loader = loader_class()
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = loader.get_latest_source_version()
                if latest_source_version != source_metadata.get_source_version():
                    self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")
                    source_metadata.archive_metadata()
                    sources_to_update.append(source_id)
                    self.new_version_lookup[source_id] = latest_source_version
                else:
                    self.logger.info(f"Source version for {source_id} is up to date ({latest_source_version})")
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
                self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")

            source_metadata.update_version(latest_source_version)
            # call the loader - retrieve/parse data and write to a kgx file
            self.logger.info(f"Loading new version of {source_id} ({latest_source_version})...")
            nodes_output_file_path = self.get_source_node_file_path(source_id, source_metadata)
            edges_output_file_path = self.get_source_edge_file_path(source_id, source_metadata)
            load_meta_data = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            self.logger.info(f"Load finished. Updating {source_id} metadata...")
            source_metadata.set_update_status(Metadata.STABLE)
            source_metadata.set_update_info(load_meta_data)
            self.logger.info(f"Updating {source_id} complete.")

        except SourceDataBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.error(f"SourceDataBrokenError while updating {source_id}: {broken_error.error_message}")
            source_metadata.set_update_error(broken_error.error_message)
            source_metadata.set_update_status(Metadata.BROKEN)

        except SourceDataFailedError as failed_error:
            # TODO report these by email or something automated
            self.logger.info(f"SourceDataBrokenError while updating {source_id}: {broken_error.error_message}")

            source_metadata.set_update_error(failed_error.error_message)
            source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            # TODO report these by email or something automated
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

    def check_sources_for_normalization(self):
        sources_to_normalize = []
        for source_id in ALL_SOURCES:
            source_metadata = self.metadata[source_id]
            normalization_status = source_metadata.get_normalization_status()
            if normalization_status == Metadata.NOT_STARTED:
                sources_to_normalize.append(source_id)
            elif ((normalization_status == Metadata.WAITING_ON_DEPENDENCY)
                  and (source_metadata.get_update_status() == Metadata.STABLE)):
                sources_to_normalize.append(source_id)
            elif normalization_status == Metadata.FAILED:
                # sources_to_normalize.append(source_id)
                # TODO do we want to retry these automatically?
                pass
        return sources_to_normalize

    def normalize_source(self, source_id: str):
        self.logger.debug(f"Normalizing source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_normalization_status(Metadata.IN_PROGRESS)
        try:
            if not self.file_normalizer:
                self.file_normalizer = KGXFileNormalizer()

            has_sequence_variants = True if source_id in SOURCES_WITH_VARIANTS else False

            self.logger.debug(f"Normalizing node file for {source_id}...")
            nodes_source_file_path = self.get_source_node_file_path(source_id, source_metadata)
            nodes_norm_file_path = self.get_normalized_node_file_path(source_id, source_metadata)
            node_norm_failures_file_path = self.get_node_norm_failures_file_path(source_id, source_metadata)
            node_normalization_info = self.file_normalizer.normalize_node_file(nodes_source_file_path,
                                                                               nodes_norm_file_path,
                                                                               node_norm_failures_file_path,
                                                                               has_sequence_variants=has_sequence_variants)

            edges_source_file_path = self.get_source_edge_file_path(source_id, source_metadata)
            edges_norm_file_path = self.get_normalized_edge_file_path(source_id, source_metadata)
            node_edge_failures_file_path = self.get_edge_norm_failures_file_path(source_id, source_metadata)

            edge_normalization_info = self.file_normalizer.normalize_edge_file(edges_source_file_path,
                                                                               edges_norm_file_path,
                                                                               node_edge_failures_file_path,
                                                                               has_sequence_variants=has_sequence_variants)

            normalization_info = {}
            normalization_info.update(node_normalization_info)
            normalization_info.update(edge_normalization_info)
            self.logger.info(f"Normalization info for {source_id}: {normalization_info}")

            # update the associated metadata
            source_metadata.set_normalization_status(Metadata.STABLE)
            source_metadata.set_normalization_info(normalization_info)
            self.logger.info(f"Normalizing source {source_id} complete.")

        except NormalizationBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.info(f"NormalizationBrokenError while normalizing {source_id}: {broken_error.error_message}")
            source_metadata.set_normalization_error(broken_error.error_message)
            source_metadata.set_normalization_status(Metadata.BROKEN)
        except NormalizationFailedError as failed_error:
            # TODO report these by email or something automated
            self.logger.info(f"NormalizationFailedError while normalizing {source_id}: {failed_error.error_message}")
            source_metadata.set_normalization_error(failed_error.error_message)
            source_metadata.set_normalization_status(Metadata.FAILED)
        except Exception as e:
            self.logger.info(f"Error while normalizing {source_id}: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_normalization_error(repr(e))
            source_metadata.set_normalization_status(Metadata.FAILED)
            raise e

    def check_sources_for_annotation(self):
        sources_to_annotate = []
        for source_id in ALL_SOURCES:
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

    def get_versioned_file_name(self, source_id: str, source_metadata: dict):
        return f'{source_id}_{source_metadata.get_load_version()}'

    def get_source_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_nodes.json')

    def get_source_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_edges.json')

    def get_normalized_node_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_nodes.json')

    def get_node_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_node_failures.log')

    def get_normalized_edge_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_edges.json')

    def get_edge_norm_failures_file_path(self, source_id: str, source_metadata: dict):
        versioned_file_name = self.get_versioned_file_name(source_id, source_metadata)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_edge_failures.log')

    def get_source_dir_path(self, source_id: str):
        return os.path.join(self.storage_dir, source_id)

    def init_storage_dir(self):
        if not self.storage_dir:
            if 'DATA_SERVICES_STORAGE' in os.environ:
                self.storage_dir = os.environ["DATA_SERVICES_STORAGE"]
            else:
                raise IOError('SourceDataLoadManager - specify the storage directory with environment variable DATA_SERVICES_STORAGE.')

        if not os.path.isdir(self.storage_dir):
            raise IOError(f'SourceDataLoadManager - storage directory specified is invalid ({self.storage_dir}).')

        for source_id in ALL_SOURCES:
            source_dir_path = self.get_source_dir_path(source_id)
            if not os.path.isdir(source_dir_path):
                self.logger.info(f"SourceDataLoadManager creating storage dir for {source_id}... {source_dir_path}")
                os.mkdir(source_dir_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('-ds', '--data_source', default='all', help=f'Select a single data source to process from the following: {ALL_SOURCES}')
    parser.add_argument('-dir', '--storage', help='Specify the storage directory. The environment variable DATA_SERVICES_STORAGE is used otherwise.')
    args = parser.parse_args()
    data_source = args.data_source

    load_manager = SourceDataLoadManager()

    if data_source == "all":
        load_manager.start()
    else:
        load_manager.update_source(data_source)
        load_manager.normalize_source(data_source)
        load_manager.annotate_source(data_source)




