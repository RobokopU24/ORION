import json
import os
import argparse
import datetime
import time
from collections import defaultdict

from orion.data_sources import (
    SourceDataLoaderClassFactory,
    RESOURCE_HOGS,
    get_available_data_sources,
    get_data_source_metadata_path,
)
from orion.logging import get_orion_logger
from orion.config import config
from orion.kgx_bundle import KGXBundle
from orion.kgx_file_merger import KGXFileMerger
from orion.kgx_file_normalizer import KGXFileNormalizer
from orion.kgx_metadata import KGXGraphMetadata, KGXKnowledgeSource, KGXKnowledgeGraphSource, generate_kgx_schema_file
from orion.kgxmodel import GraphFileSource, GraphSpec
from orion.kgx_validation import validate_graph
from orion.normalization import NormalizationScheme, NodeNormalizer, EdgeNormalizer, NormalizationFailedError
from orion.metadata import SourceMetadata
from orion.loader_interface import SourceDataBrokenError, SourceDataFailedError
from orion.supplementation import SequenceVariantSupplementation, SupplementationFailedError


SOURCE_DATA_LOADER_CLASSES = SourceDataLoaderClassFactory()

logger = get_orion_logger("orion.ingest_pipeline")


class IngestPipeline:

    def __init__(self,
                 storage_dir: str = None,
                 test_mode: bool = False):

        self.test_mode = test_mode
        if test_mode:
            logger.info(f'IngestPipeline running in test mode... test data sets will be used when possible.')

        # lazy load the storage directory path
        # store the storage_dir parameter to override the Config if provided programmatically or through CLI
        self._storage_dir_override = storage_dir
        self._storage_dir = None

        # dict of source_id -> latest source version (to prevent double lookups)
        self.latest_source_version_lookup = {}
        self.latest_parsing_version_lookup = {}
        # placeholders for lazy loading
        self.latest_edge_normalization_version = None
        self.latest_supplementation_version = None

        # nested dict of source_id -> source_version -> SourceMetadata
        self.source_metadata = defaultdict(dict)

    def run_pipeline(self,
                     source_id: str,
                     source_version: str='latest',
                     parsing_version: str='latest',
                     normalization_scheme: NormalizationScheme = None,
                     supplementation_version: str='latest'):

        logger.info(f"Running pipeline on {source_id}...")
        self.init_source_output_dir(source_id)

        if source_version == 'latest':
            source_version = self.get_latest_source_version(source_id)
        if not self.run_fetch_stage(source_id, source_version):
            logger.error(f"Pipeline for {source_id} aborted during fetch stage.")
            return False

        if parsing_version == 'latest':
            parsing_version = self.get_latest_parsing_version(source_id)
        if not self.run_parsing_stage(source_id, source_version, parsing_version=parsing_version):
            logger.error(f"Pipeline for {source_id} aborted during parsing stage.")
            return False

        if not normalization_scheme:
            logger.debug(f"No normalization scheme provided, using defaults...")
            normalization_scheme = NormalizationScheme()

        if not self.run_normalization_stage(source_id,
                                            source_version,
                                            parsing_version=parsing_version,
                                            normalization_scheme=normalization_scheme):
            logger.error(f"Pipeline for {source_id} aborted during normalization stage.")
            return False

        if supplementation_version == 'latest':
            supplementation_version = SequenceVariantSupplementation.SUPPLEMENTATION_VERSION
        if not self.run_supplementation_stage(source_id,
                                              source_version,
                                              parsing_version=parsing_version,
                                              supplementation_version=supplementation_version,
                                              normalization_scheme=normalization_scheme):
            logger.error(f"Pipeline for {source_id} supplementation stage not successful.")
            return False

        build_version = self.run_qc_and_metadata_stage(source_id,
                                                       source_version,
                                                       parsing_version=parsing_version,
                                                       normalization_scheme=normalization_scheme,
                                                       supplementation_version=supplementation_version)
        if build_version is None:
            logger.warning(f"Pipeline for {source_id} failed quality control...")
            return False

        if not self.finalize_source_build(source_id,
                                          source_version,
                                          parsing_version=parsing_version,
                                          normalization_scheme=normalization_scheme,
                                          supplementation_version=supplementation_version,
                                          build_version=build_version):
            logger.error(f"Pipeline for {source_id} failed to generate a final source build.")
            return False
        return build_version

    def run_fetch_stage(self, source_id: str, source_version: str):
        if not source_version:
            logger.error(f"Error running pipeline for {source_id} - could not determine latest version.")
            return False

        fetch_status = self.get_source_metadata(source_id, source_version).get_fetch_status()
        if fetch_status == SourceMetadata.STABLE:
            return True
        elif fetch_status == SourceMetadata.IN_PROGRESS:
            logger.info(f"Fetch stage for {source_id} is already in progress.")
            return False
        elif fetch_status == SourceMetadata.BROKEN or fetch_status == SourceMetadata.FAILED:
            # TODO consider retry logic here
            logger.info(f"Fetch stage for {source_id} previously: {fetch_status}")
            return False
        else:
            logger.info(f"Fetching source data for {source_id} (version: {source_version})...")
            return self.fetch_source(source_id, source_version=source_version)

    def get_latest_source_version(self, source_id: str, retries: int = 1):
        if source_id in self.latest_source_version_lookup:
            return self.latest_source_version_lookup[source_id]

        loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode)
        logger.info(f"Retrieving latest source version for {source_id}...")
        try:
            latest_source_version = loader.get_latest_source_version()
            logger.info(f"Found latest source version for {source_id}: {latest_source_version}")
            self.latest_source_version_lookup[source_id] = latest_source_version
            return latest_source_version
        except Exception as e:
            error_message = getattr(e, 'error_message', None) or f"{repr(e)}-{str(e)}"
            logger.error(f"Error while checking for latest source version for {source_id}: {error_message}")
            if retries < 4:
                time.sleep(retries * 2)
                return self.get_latest_source_version(source_id, retries=retries + 1)
            return None

    def fetch_source(self, source_id: str, source_version: str='latest', retries: int=1):

        logger.debug(f'Fetching source {source_id}...')
        source_version_path = self.get_source_version_path(source_id, source_version)
        os.makedirs(source_version_path, exist_ok=True)
        source_metadata = self.get_source_metadata(source_id, source_version)
        source_metadata.set_fetch_status(SourceMetadata.IN_PROGRESS)
        try:
            loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode,
                                                           source_data_dir=source_version_path)
            if loader.needs_data_download():
                if source_version != self.get_latest_source_version(source_id):
                    unsupported_error_message = f"Fetching source data {source_id} (version: {source_version}) failed - " \
                                                f"fetching old source versions not supported."
                    logger.error(unsupported_error_message)
                    source_metadata.set_fetch_error(unsupported_error_message)
                    source_metadata.set_fetch_status(SourceMetadata.FAILED)
                    return False

                logger.info(f'Retrieving source data for {source_id} (version: {source_version})..')
                loader.get_data()
            else:
                logger.info(f'Source data was already retrieved for {source_id}..')
            source_metadata.set_fetch_status(SourceMetadata.STABLE)
            return True

        except Exception as e:
            error_message = getattr(e, 'error_message', None) or f"{repr(e)}-{str(e)}"
            logger.info(f"Error while fetching source data for {source_id} (version: {source_version}): "
                        f"{error_message}")
            if retries < 4:
                time.sleep(retries * 2)
                logger.error(f"Retrying fetching for {source_id}.. (retry {retries + 1})")
                return self.fetch_source(source_id=source_id, source_version=source_version, retries=retries + 1)
            source_metadata.set_fetch_error(error_message)
            source_metadata.set_fetch_status(SourceMetadata.FAILED)
            return False

    def run_parsing_stage(self, source_id: str, source_version: str, parsing_version: str):
        source_metadata = self.get_source_metadata(source_id, source_version)
        parsing_status = source_metadata.get_parsing_status(parsing_version)
        if parsing_status == SourceMetadata.STABLE:
            return True
        elif parsing_status == SourceMetadata.IN_PROGRESS:
            logger.info(f"Parsing stage for {source_id} is already in progress.")
            return False
        elif parsing_status == SourceMetadata.BROKEN:
            logger.info(f"Parsing stage for {source_id} previously: {parsing_status}")
            return False
        else:
            # if parsing_status == SourceMetadata.FAILED:
            # TODO consider retry logic here - should we only try a few times? ask user?
            logger.info(f"Parsing source {source_id} (source_version: {source_version}, "
                             f"parsing_version: {parsing_version})...")
            return self.parse_source(source_id, source_version, parsing_version)

    def parse_source(self, source_id: str, source_version: str, parsing_version: str):

        # we currently don't support any parser version but the latest, just bail
        if parsing_version != self.get_latest_parsing_version(source_id):
            logger.error(f'Parser version {parsing_version} unavailable for {source_id}.')
            return False

        logger.info(f'Parsing source {source_id}...')
        current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
        source_metadata = self.get_source_metadata(source_id, source_version)
        source_metadata.update_parsing_metadata(parsing_version,
                                                parsing_source_version=source_version,
                                                parsing_status=SourceMetadata.IN_PROGRESS)
        try:
            # init the parsing output directory
            source_data_dir = self.get_source_version_path(source_id, source_version)
            versioned_parsing_dir = self.get_versioned_parsing_directory(source_id, source_version, parsing_version)
            os.makedirs(versioned_parsing_dir, exist_ok=True)
            # create an instance of the appropriate loader using the source_data_loader_classes lookup map
            source_data_loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode,
                                                                       source_data_dir=source_data_dir)

            # call the loader - retrieve/parse data and write to a kgx file
            nodes_output_file_path = self.get_source_node_file_path(source_id, source_version, parsing_version)
            edges_output_file_path = self.get_source_edge_file_path(source_id, source_version, parsing_version)
            parsing_info = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            has_sequence_variants = source_data_loader.has_sequence_variants
            source_metadata.update_parsing_metadata(parsing_version,
                                                    parsing_source_version=source_version,
                                                    parsing_status=SourceMetadata.STABLE,
                                                    parsing_info=parsing_info,
                                                    parsing_time=current_time,
                                                    has_sequence_variants=has_sequence_variants)
            return True

        except SourceDataBrokenError as broken_error:
            logger.error(f"SourceDataBrokenError while parsing {source_id}: {broken_error.error_message}")
            source_metadata.update_parsing_metadata(parsing_version,
                                                    parsing_status=SourceMetadata.BROKEN,
                                                    parsing_error=broken_error.error_message,
                                                    parsing_time=current_time)
            return False
        except SourceDataFailedError as failed_error:
            logger.error(f"SourceDataFailedError while parsing {source_id}: {failed_error.error_message}")
            source_metadata.update_parsing_metadata(parsing_version,
                                                    parsing_status=SourceMetadata.FAILED,
                                                    parsing_error=failed_error.error_message,
                                                    parsing_time=current_time)
            return False
        except Exception as e:
            logger.error(f"Exception while parsing {source_id}: {repr(e)}-{str(e)}")
            source_metadata.update_parsing_metadata(parsing_version,
                                                    parsing_status=SourceMetadata.FAILED,
                                                    parsing_error=f'{repr(e)}-{str(e)}',
                                                    parsing_time=current_time)
            return False

    def get_latest_parsing_version(self, source_id: str):
        if source_id in self.latest_parsing_version_lookup:
            return self.latest_parsing_version_lookup[source_id]

        parsing_version = SOURCE_DATA_LOADER_CLASSES[source_id].parsing_version
        self.latest_parsing_version_lookup[source_id] = parsing_version
        return parsing_version

    def run_normalization_stage(self,
                                source_id: str,
                                source_version: str,
                                parsing_version: str,
                                normalization_scheme: NormalizationScheme):

        composite_normalization_version = normalization_scheme.get_composite_normalization_version()
        source_metadata = self.get_source_metadata(source_id, source_version)
        normalization_status = source_metadata.get_normalization_status(parsing_version,
                                                                        composite_normalization_version)
        if normalization_status == SourceMetadata.STABLE:
            return True
        elif normalization_status == SourceMetadata.IN_PROGRESS:
            logger.info(f"Normalization stage for {source_id} is already in progress.")
            return False
        elif normalization_status == SourceMetadata.BROKEN:
            logger.info(f"Normalization stage for {source_id} previously: {normalization_status}")
            return False
        else:
            # normalize if needed, or retry on status == SourceMetadata.FAILED
            return self.normalize_source(source_id,
                                         source_version,
                                         parsing_version,
                                         normalization_scheme=normalization_scheme)

    def normalize_source(self,
                         source_id: str,
                         source_version: str,
                         parsing_version: str,
                         normalization_scheme: NormalizationScheme):
        logger.info(f"Normalizing {source_id}...")
        composite_normalization_version = normalization_scheme.get_composite_normalization_version()
        versioned_normalization_dir = self.get_versioned_normalization_directory(source_id,
                                                                                 source_version,
                                                                                 parsing_version,
                                                                                 composite_normalization_version)
        os.makedirs(versioned_normalization_dir, exist_ok=True)
        source_metadata = self.get_source_metadata(source_id, source_version)
        source_metadata.update_normalization_metadata(parsing_version,
                                                      composite_normalization_version,
                                                      normalization_status=SourceMetadata.IN_PROGRESS)
        current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
        try:
            nodes_source_file_path = self.get_source_node_file_path(source_id, source_version, parsing_version)
            nodes_norm_file_path = self.get_normalized_node_file_path(source_id, source_version, parsing_version, composite_normalization_version)
            node_norm_map_file_path = self.get_node_norm_map_file_path(source_id, source_version, parsing_version, composite_normalization_version)
            node_norm_failures_file_path = self.get_node_norm_failures_file_path(source_id, source_version, parsing_version, composite_normalization_version)
            edges_source_file_path = self.get_source_edge_file_path(source_id, source_version, parsing_version)
            edges_norm_file_path = self.get_normalized_edge_file_path(source_id, source_version, parsing_version, composite_normalization_version)
            edge_norm_predicate_map_file_path = self.get_edge_norm_predicate_map_file_path(source_id, source_version, parsing_version, composite_normalization_version)
            has_sequence_variants = source_metadata.has_sequence_variants(parsing_version)
            default_provenance = SOURCE_DATA_LOADER_CLASSES[source_id].provenance_id
            preserve_unconnected_nodes = SOURCE_DATA_LOADER_CLASSES[source_id].preserve_unconnected_nodes
            process_in_memory = False if source_id in RESOURCE_HOGS else True
            file_normalizer = KGXFileNormalizer(nodes_source_file_path,
                                                nodes_norm_file_path,
                                                node_norm_map_file_path,
                                                node_norm_failures_file_path,
                                                edges_source_file_path,
                                                edges_norm_file_path,
                                                edge_norm_predicate_map_file_path,
                                                has_sequence_variants=has_sequence_variants,
                                                normalization_scheme=normalization_scheme,
                                                default_provenance=default_provenance,
                                                process_in_memory=process_in_memory,
                                                preserve_unconnected_nodes=preserve_unconnected_nodes)
            normalization_info = file_normalizer.normalize_kgx_files()

            # update the associated metadata
            source_metadata.update_normalization_metadata(parsing_version,
                                                          composite_normalization_version,
                                                          normalization_time=current_time,
                                                          normalization_status=SourceMetadata.STABLE,
                                                          normalization_info=normalization_info)
            return True
        except NormalizationFailedError as failed_error:
            error_message = f"{source_id} NormalizationFailedError: {failed_error.error_message}"
            if failed_error.actual_error:
                error_message += f" - {failed_error.actual_error}"
            logger.error(error_message)
            source_metadata.update_normalization_metadata(parsing_version,
                                                          composite_normalization_version,
                                                          normalization_status=SourceMetadata.FAILED,
                                                          normalization_error=error_message,
                                                          normalization_time=current_time)
            self.delete_normalization_files(source_id, source_version, parsing_version, composite_normalization_version)
            return False
        except Exception as e:
            logger.error(f"Error while normalizing {source_id}: {repr(e)}")
            source_metadata.update_normalization_metadata(parsing_version,
                                                          composite_normalization_version,
                                                          normalization_status=SourceMetadata.FAILED,
                                                          normalization_error=repr(e),
                                                          normalization_time=current_time)
            self.delete_normalization_files(source_id, source_version, parsing_version, composite_normalization_version)
            return False

    def delete_normalization_files(self,
                                   source_id: str,
                                   source_version: str,
                                   parsing_version: str,
                                   composite_normalization_version: str):
        # Remove any partial output from a failed normalization attempt so the retry starts clean.
        failed_file_paths = [
            self.get_normalized_node_file_path(source_id, source_version, parsing_version, composite_normalization_version),
            self.get_node_norm_map_file_path(source_id, source_version, parsing_version, composite_normalization_version),
            self.get_node_norm_failures_file_path(source_id, source_version, parsing_version, composite_normalization_version),
            self.get_normalized_edge_file_path(source_id, source_version, parsing_version, composite_normalization_version),
            self.get_edge_norm_predicate_map_file_path(source_id, source_version, parsing_version, composite_normalization_version),
        ]
        for failed_file_path in failed_file_paths:
            if os.path.exists(failed_file_path):
                os.remove(failed_file_path)

    def run_supplementation_stage(self,
                                  source_id: str,
                                  source_version: str,
                                  parsing_version: str,
                                  supplementation_version: str,
                                  normalization_scheme: NormalizationScheme):

        if supplementation_version != SequenceVariantSupplementation.SUPPLEMENTATION_VERSION:
            logger.warning(f"Supplementation version {supplementation_version} is not supported.")
            return False

        composite_normalization_version = normalization_scheme.get_composite_normalization_version()
        source_metadata = self.get_source_metadata(source_id, source_version)
        supplementation_status = source_metadata.get_supplementation_status(parsing_version,
                                                                            composite_normalization_version,
                                                                            supplementation_version)
        if supplementation_status == SourceMetadata.STABLE:
            return True
        elif supplementation_status == SourceMetadata.FAILED or supplementation_status == SourceMetadata.BROKEN:
            logger.info(f"Supplementation stage for {source_id} previously failed or was broken.")
            # TODO consider retry logic here
            return False
        elif supplementation_status == SourceMetadata.IN_PROGRESS:
            logger.info(f"Supplementation stage for {source_id} is already in progress.")
            return False
        else:
            return self.supplement_source(source_id,
                                          source_version,
                                          parsing_version,
                                          supplementation_version,
                                          normalization_scheme=normalization_scheme)

    def supplement_source(self,
                          source_id: str,
                          source_version: str,
                          parsing_version: str,
                          supplementation_version: str,
                          normalization_scheme: NormalizationScheme):
        logger.info(f"Supplementing source {source_id}...")
        composite_normalization_version = normalization_scheme.get_composite_normalization_version()
        current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
        versioned_supplementation_dir = self.get_versioned_supplementation_directory(source_id,
                                                                                     source_version,
                                                                                     parsing_version,
                                                                                     composite_normalization_version,
                                                                                     supplementation_version)
        source_metadata = self.get_source_metadata(source_id, source_version)
        try:
            source_metadata.update_supplementation_metadata(parsing_version,
                                                            composite_normalization_version,
                                                            supplementation_version,
                                                            supplementation_status=SourceMetadata.IN_PROGRESS)
            if source_metadata.has_sequence_variants(parsing_version):

                os.makedirs(versioned_supplementation_dir, exist_ok=True)

                nodes_file_path = self.get_normalized_node_file_path(source_id, source_version, parsing_version, composite_normalization_version)
                supplemental_node_file_path = self.get_supplemental_node_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                normalized_supp_node_file_path = self.get_normalized_supp_node_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                supp_node_norm_map_file_path = self.get_supp_node_norm_map_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                supp_node_norm_failures_file_path = self.get_supp_node_norm_failures_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                supplemental_edge_file_path = self.get_supplemental_edge_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                normalized_supp_edge_file_path = self.get_normalized_supplemental_edge_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                supp_edge_norm_predicate_map_file_path = self.get_supp_edge_norm_predicate_map_file_path(source_id, source_version, parsing_version, composite_normalization_version, supplementation_version)
                sv_supp = SequenceVariantSupplementation()
                supplementation_info = sv_supp.find_supplemental_data(nodes_file_path=nodes_file_path,
                                                                      supp_nodes_file_path=supplemental_node_file_path,
                                                                      supp_nodes_norm_file_path=normalized_supp_node_file_path,
                                                                      supp_node_norm_map_file_path=supp_node_norm_map_file_path,
                                                                      supp_node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                                                      supp_edges_file_path=supplemental_edge_file_path,
                                                                      normalized_supp_edge_file_path=normalized_supp_edge_file_path,
                                                                      supp_edge_norm_predicate_map_file_path=supp_edge_norm_predicate_map_file_path,
                                                                      normalization_scheme=normalization_scheme)
            else:
                supplementation_info = None
            source_metadata.update_supplementation_metadata(parsing_version,
                                                            composite_normalization_version,
                                                            supplementation_version,
                                                            supplementation_status=SourceMetadata.STABLE,
                                                            supplementation_time=current_time,
                                                            supplementation_info=supplementation_info)
            return True
        except SupplementationFailedError as failed_error:
            error_message = f"{source_id} SupplementationFailedError: " \
                            f"{failed_error.error_message} - {failed_error.actual_error}"
            logger.error(error_message)
            source_metadata.update_supplementation_metadata(parsing_version,
                                                            composite_normalization_version,
                                                            supplementation_version,
                                                            supplementation_status=SourceMetadata.FAILED,
                                                            supplementation_error=error_message,
                                                            supplementation_time=current_time)
            return False

        except Exception as e:
            logger.error(f"{source_id} Error while supplementing: {repr(e)}")
            source_metadata.update_supplementation_metadata(parsing_version,
                                                            composite_normalization_version,
                                                            supplementation_version,
                                                            supplementation_status=SourceMetadata.FAILED,
                                                            supplementation_error=repr(e),
                                                            supplementation_time=current_time)
            return False

    def run_qc_and_metadata_stage(self,
                                  source_id: str,
                                  source_version: str,
                                  parsing_version: str,
                                  supplementation_version: str,
                                  normalization_scheme: NormalizationScheme):
        # source data QC should go here
        source_metadata = self.get_source_metadata(source_id, source_version)
        loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode)
        source_meta_information = loader.get_source_meta_information()
        normalization_version = normalization_scheme.get_composite_normalization_version()
        build_version = source_metadata.generate_build_metadata(parsing_version=parsing_version,
                                                                supplementation_version=supplementation_version,
                                                                normalization_version=normalization_version,
                                                                source_meta_information=source_meta_information)
        logger.info(f'Build version for {source_id}: {build_version}')

        composite_normalization_version = normalization_scheme.get_composite_normalization_version()
        nodes_filepath = self.get_normalized_node_file_path(source_id, source_version, parsing_version,
                                                            composite_normalization_version)
        edges_filepath = self.get_normalized_edge_file_path(source_id, source_version, parsing_version,
                                                            composite_normalization_version)
        source_version_path = self.get_source_version_path(source_id, source_version)
        qc_output_filename = f'{source_id}_{build_version}.json'
        qc_output_path = os.path.join(source_version_path, qc_output_filename)
        if not os.path.exists(qc_output_path):
            logger.info(f'Running QC and validation...')
            qc_results = validate_graph(nodes_file_path=nodes_filepath,
                                        edges_file_path=edges_filepath,
                                        graph_id=source_id,
                                        build_version=build_version,
                                        logger=logger)
            with open(qc_output_path, 'w') as qc_out:
                qc_out.write(json.dumps(qc_results, indent=4))
            logger.info(f'QC and validation complete, metadata generated: {qc_output_filename}')
        return build_version

    def get_source_metadata(self, source_id: str, source_version):
        if source_id not in self.source_metadata or source_version not in self.source_metadata[source_id]:
            source_metadata = SourceMetadata(source_id,
                                             source_version,
                                             self.get_source_version_path(source_id, source_version))
            self.source_metadata[source_id][source_version] = source_metadata
        else:
            source_metadata = self.source_metadata[source_id][source_version]
        return source_metadata

    def get_versioned_parsing_directory(self, source_id: str, source_version: str, parsing_version: str):
        versioned_parsing_directory = f'parsed_{parsing_version}/'
        return os.path.join(self.get_source_version_path(source_id, source_version),
                            versioned_parsing_directory)

    def get_source_node_file_path(self, source_id: str, source_version: str, parsing_version: str):
        file_name = f'source_nodes.jsonl'
        return os.path.join(self.get_versioned_parsing_directory(source_id, source_version, parsing_version), file_name)

    def get_source_edge_file_path(self, source_id: str, source_version: str, parsing_version: str):
        file_name = f'source_edges.jsonl'
        return os.path.join(self.get_versioned_parsing_directory(source_id, source_version, parsing_version), file_name)

    def get_versioned_normalization_directory(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        versioned_norm_dir = f'normalized_{normalization_version}/'
        return os.path.join(self.get_versioned_parsing_directory(source_id, source_version, parsing_version), versioned_norm_dir)

    def get_normalized_node_file_path(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), f'normalized_nodes.jsonl')

    def get_node_norm_map_file_path(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), f'norm_node_map.json')

    def get_node_norm_failures_file_path(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), f'norm_node_failures.log')

    def get_normalized_edge_file_path(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), f'normalized_edges.jsonl')

    def get_edge_norm_predicate_map_file_path(self, source_id: str, source_version: str, parsing_version: str, normalization_version: str):
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), f'norm_predicate_map.json')

    def get_versioned_supplementation_directory(self,
                                                source_id: str,
                                                source_version: str,
                                                parsing_version: str,
                                                normalization_version: str,
                                                supplementation_version: str):
        versioned_dir = f'supplemental_{supplementation_version}/'
        return os.path.join(self.get_versioned_normalization_directory(source_id,
                                                                       source_version,
                                                                       parsing_version,
                                                                       normalization_version), versioned_dir)

    def get_supplemental_node_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                        normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_nodes.jsonl')

    def get_normalized_supp_node_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                           normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_norm_nodes.jsonl')

    def get_supp_node_norm_map_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                         normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_norm_node_map.json')

    def get_supp_node_norm_failures_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                              normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_norm_nodes_failures.log')

    def get_supplemental_edge_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                        normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_edges.jsonl')

    def get_normalized_supplemental_edge_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                                   normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_norm_edges.jsonl')

    def get_supp_edge_norm_predicate_map_file_path(self, source_id: str, source_version: str, parsing_version: str,
                                                   normalization_version: str, supplementation_version: str):
        return os.path.join(self.get_versioned_supplementation_directory(source_id,
                                                                         source_version,
                                                                         parsing_version,
                                                                         normalization_version,
                                                                         supplementation_version), f'supp_norm_predicate_map.json')

    def get_final_file_paths(self, source_id: str, source_version: str, parsing_version: str,
                             normalization_version: str, supplementation_version: str):
        file_paths = list()
        file_paths.append(
            self.get_normalized_node_file_path(source_id, source_version, parsing_version, normalization_version))
        file_paths.append(
            self.get_normalized_edge_file_path(source_id, source_version, parsing_version, normalization_version))
        if self.get_source_metadata(source_id, source_version).has_supplemental_data(parsing_version,
                                                                                     normalization_version,
                                                                                     supplementation_version):
            file_paths.append(self.get_normalized_supp_node_file_path(source_id,
                                                                      source_version,
                                                                      parsing_version,
                                                                      normalization_version,
                                                                      supplementation_version))
            file_paths.append(
                self.get_normalized_supplemental_edge_file_path(source_id,
                                                                source_version,
                                                                parsing_version,
                                                                normalization_version,
                                                                supplementation_version))
        return file_paths

    # Per-source-version working data lives under a 'data' subdirectory,
    # keeping it separate from the finalized 'builds'.
    # {storage}/{source_id}/data/{source_version}/
    def get_source_version_path(self, source_id: str, source_version: str):
        return os.path.join(self.storage_dir, source_id, 'data', source_version)

    # Source builds are the finalized single-source graph products of the ingest pipeline. A source
    # build is itself a single-source KGX graph, so its directory is a standard KGXBundle layout
    # (nodes.jsonl, edges.jsonl, graph-metadata.json) identical to a graph build's.
    # Layout: {storage}/{source_id}/builds/{build_version}/
    def get_source_builds_directory(self, source_id: str) -> str:
        return os.path.join(self.storage_dir, source_id, 'builds')

    def get_source_build_directory(self, source_id: str, build_version: str) -> str:
        return os.path.join(self.get_source_builds_directory(source_id), build_version)

    def get_source_build_bundle(self, source_id: str, build_version: str) -> KGXBundle:
        return KGXBundle(self.get_source_build_directory(source_id, build_version))

    def has_source_build(self, source_id: str, build_version: str) -> bool:
        """True if a complete source build exists for (source_id, build_version)."""
        bundle = self.get_source_build_bundle(source_id, build_version)
        return bundle.has_nodes_and_edges() and bundle.has_graph_metadata()

    @staticmethod
    def get_source_build_output_url(source_id: str, build_version: str) -> str:
        return f'{config.ORION_OUTPUT_URL}/sources/{source_id}/builds/{build_version}/'

    # Merge within-source nodes & edges and generate KGXGraphMetadata for the output.
    # These are the finalized KGX files published on the graph registry used to build graphs.
    def finalize_source_build(self,
                              source_id: str,
                              source_version: str,
                              parsing_version: str,
                              normalization_scheme: NormalizationScheme,
                              supplementation_version: str,
                              build_version: str) -> bool:

        if self.has_source_build(source_id, build_version):
            # this source build already exists
            return True

        parser_file_paths = self.get_final_file_paths(source_id,
                                                      source_version,
                                                      parsing_version,
                                                      normalization_scheme.get_composite_normalization_version(),
                                                      supplementation_version)
        if not parser_file_paths:
            logger.error(f'Cannot finalize source build for {source_id} build_version {build_version}: '
                         f'parser output files not found.')
            return False

        source_build_bundle = self.get_source_build_bundle(source_id, build_version)
        os.makedirs(source_build_bundle.graph_dir, exist_ok=True)

        graph_spec = GraphSpec(
            graph_id=f'{source_id}_{build_version}',
            graph_name=source_id,
            graph_description='',
            graph_url='',
            sources=[],
        )
        graph_spec.resolved_sources = [GraphFileSource(id=source_id,
                                                       build_version=build_version,
                                                       file_paths=parser_file_paths)]

        logger.info(f'Finalizing source build for {source_id} build_version {build_version}. Merging entities...')
        source_merger = KGXFileMerger(graph_spec=graph_spec,
                                      output_directory=source_build_bundle.graph_dir,
                                      nodes_output_filename=KGXBundle.NODES_FILENAME,
                                      edges_output_filename=KGXBundle.EDGES_FILENAME)
        source_merger.merge()
        merge_metadata = source_merger.get_merge_metadata()
        if 'merge_error' in merge_metadata:
            logger.error(f'Source build merge failed for {source_id}: {merge_metadata["merge_error"]}')
            return False

        node_count = merge_metadata.get('final_node_count')
        edge_count = merge_metadata.get('final_edge_count')
        source_build_url = self.get_source_build_output_url(source_id, build_version)
        build_time = datetime.datetime.now().isoformat(timespec='seconds')
        biolink_version = normalization_scheme.edge_normalization_version

        parser_metadata = self._load_parser_metadata(source_id)
        source_name = parser_metadata.get('name', source_id)
        graph_name = f'A ROBOKOP Knowledge Graph based on {source_name}'

        # Run QC and generate the schema over the finalized (still-uncompressed) KGX files, so
        # every source build carries the same qc-results.json + schema.json a merge graph does.
        # These generators read raw jsonl, so do this before compression.
        nodes_file_path = source_build_bundle.nodes_path
        edges_file_path = source_build_bundle.edges_path
        qc_results = validate_graph(nodes_file_path=nodes_file_path,
                                    edges_file_path=edges_file_path,
                                    graph_id=source_id,
                                    build_version=build_version,
                                    logger=logger)
        with open(source_build_bundle.qc_results_path, 'w') as qc_out:
            json.dump(qc_results, qc_out, indent=2)
        generate_kgx_schema_file(nodes_filepath=nodes_file_path,
                                 edges_filepath=edges_file_path,
                                 output_dir=source_build_bundle.graph_dir,
                                 graph_output_url=source_build_url,
                                 graph_name=graph_name,
                                 biolink_version=biolink_version)

        source_build_bundle.compress_nodes_and_edges()

        # generate KGXGraphMetadata for the final KGX files
        knowledge_source = KGXKnowledgeSource.from_dict(parser_metadata)
        knowledge_source.version = source_version
        kg_source = KGXKnowledgeGraphSource(id=source_build_url,
                                            name=graph_name,
                                            build_version=build_version,
                                            node_count=node_count,
                                            edge_count=edge_count)
        graph_metadata = KGXGraphMetadata(id=source_build_url,
                                          name=graph_name,
                                          url=source_build_url,
                                          version=source_version,
                                          build_version=build_version,
                                          date_created=build_time,
                                          date_modified=build_time,
                                          biolink_version=biolink_version,
                                          babel_version=normalization_scheme.node_normalization_version,
                                          schema={
                                              "@type": "Dataset",
                                              "@id": f"{source_build_url}schema.json",
                                              "name": f"{graph_name} Schema",
                                              "description": "JSON-LD Schema describing the contents of the knowledge graph",
                                              "encodingFormat": "application/ld+json"
                                          },
                                          kg_sources=[kg_source.to_dict()],
                                          knowledge_sources=[knowledge_source],
                                          distribution=[{
                                              "@type": "DataDownload",
                                              "encodingFormat": "biolink:KGX",
                                              "contentUrl": source_build_url,
                                          }])
        with open(source_build_bundle.graph_metadata_path, 'w') as f:
            f.write(graph_metadata.to_json())
        return True

    # Build a GraphFileSource for an existing on-disk source build.
    # Returns None if the source build is incomplete or missing.
    # The merge_strategy is the consuming graph's choice for how to
    # merge this source in, so it's supplied by the caller.
    def load_source_build_file_source(self,
                                      source_id: str,
                                      build_version: str,
                                      merge_strategy: str = None) -> GraphFileSource | None:
        bundle = self.get_source_build_bundle(source_id, build_version)
        if not (bundle.has_nodes_and_edges() and bundle.has_graph_metadata()):
            return None
        return GraphFileSource(
            id=source_id,
            build_version=build_version,
            file_paths=[bundle.nodes_path, bundle.edges_path],
            merge_strategy=merge_strategy,
            kgx_graph_metadata=bundle.load_graph_metadata(),
        )

    @staticmethod
    def _load_parser_metadata(source_id: str) -> dict:
        """Load a data source's parser-supplied source.json metadata (name, description, license, …)."""
        orion_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
        parser_metadata_path = os.path.join(orion_root, get_data_source_metadata_path(source_id))
        with open(parser_metadata_path) as f:
            return json.load(f)

    @property
    def storage_dir(self):
        if self._storage_dir is None:
            self._storage_dir = self._resolve_storage_dir(self._storage_dir_override)
        return self._storage_dir

    @staticmethod
    def _resolve_storage_dir(storage_dir: str = None):
        # if a dir was provided programmatically try to use that
        if storage_dir is not None:
            if os.path.isdir(storage_dir):
                return storage_dir
            else:
                raise IOError(f'Storage directory not valid: {storage_dir}')
        # otherwise resolve from the config
        return config.get_storage_dir()

    def init_source_output_dir(self, source_id: str):
        source_dir_path = os.path.join(self.storage_dir, source_id)
        os.makedirs(source_dir_path, exist_ok=True)


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('data_source',
                        nargs="+",
                        help=f'Select one or more data sources to process from the following: '
                             f'{", ".join(get_available_data_sources())}')
    parser.add_argument('-t', '--test_mode',
                        action='store_true',
                        help='Test mode will process a small sample version of the data.')
    parser.add_argument('-l', '--lenient_normalization',
                        action='store_true',
                        help='Lenient normalization allows nodes that can not be normalized to persist '
                             'in final graph outputs.')
    parser.add_argument('-c', '--conflation',
                        action='store_true',
                        help='Conflation mode will turn on all conflation options during normalization. See https://github.com/NCATSTranslator/Babel/ for more information.')
    args = parser.parse_args()

    loader_test_mode = args.test_mode or config.ORION_TEST_MODE
    strict_normalization = (not args.lenient_normalization)
    conflation_on = args.conflation
    ingest_pipeline = IngestPipeline(test_mode=loader_test_mode)
    for data_source in args.data_source:
        if data_source not in get_available_data_sources():
            print(f'Data source {data_source} is not valid. '
                  f'These are the available data sources: {", ".join(get_available_data_sources())}')
        else:
            cmd_line_normalization_scheme = NormalizationScheme(strict=strict_normalization,
                                                                conflation=conflation_on)
            build_version = ingest_pipeline.run_pipeline(data_source, normalization_scheme=cmd_line_normalization_scheme)
            if build_version:
                print(f'Finished running data pipeline for {data_source} (build_version {build_version}).')


if __name__ == '__main__':
    main()
