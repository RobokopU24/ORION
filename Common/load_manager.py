
import os
import argparse
import yaml
import datetime
import shutil
import json

# from multiprocessing import Pool

from Common.utils import LoggingUtil, NodeNormUtils, EdgeNormUtils, GetDataPullError
from Common.kgx_file_normalizer import KGXFileNormalizer, NormalizationBrokenError, NormalizationFailedError
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
from Common.supplementation import SequenceVariantSupplementation, SupplementationFailedError
from parsers.GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader
from parsers.CTD.src.loadCTD import CTDLoader
from parsers.cord19.src.loadCord19 import Cord19Loader
#from parsers.FooDB.src.loadFDB import FDBLoader
from parsers.GOA.src.loadGOA import GOALoader
from parsers.IntAct.src.loadIA import IALoader
from parsers.PHAROS.src.loadPHAROS import PHAROSLoader
from parsers.UberGraph.src.loadUG import UGLoader
from parsers.ViralProteome.src.loadVP import VPLoader
from parsers.ViralProteome.src.loadUniRef import UniRefSimLoader
from parsers.gtopdb.src.loadGtoPdb import GtoPdbLoader
from parsers.hmdb.src.loadHMDB import HMDBLoader
from parsers.hgnc.src.loadHGNC import HGNCLoader
from parsers.panther.src.loadPanther import PLoader
from parsers.GTEx.src.loadGTEx import GTExLoader
from parsers.drugcentral.src.loaddrugcentral import DrugCentralLoader
from parsers.hetio.src.loadHetio import HetioLoader
from parsers.biolink.src.loadBL import BLLoader

GWAS_CATALOG = 'GWASCatalog'
CTD = 'CTD'
CORD19 = 'Cord19'
FOODB = 'FooDB' # this is on hold, data needs review after latest release of data.
HUMAN_GOA = 'HumanGOA' # this has normalization issues (needs pre-norm to create edges)
INTACT = "IntAct"
PHAROS = 'PHAROS'
UBERGRAPH = 'UberGraph'
UNIREF = "UniRef"
VP = 'ViralProteome'
GTOPDB = 'GtoPdb'
HMDB = 'HMDB'
HGNC = 'HGNC'
PANTHER = 'PANTHER'
GTEX = 'GTEx'
DRUG_CENTRAL = 'DrugCentral'
HETIO = 'Hetio'
BIOLINK = 'Biolink'
UNIREF = 'UniRef'

SOURCE_DATA_LOADER_CLASSES = {
    CTD: CTDLoader,
    CORD19: Cord19Loader,
    INTACT: IALoader,
    GTOPDB: GtoPdbLoader,
    HUMAN_GOA: GOALoader,
    HGNC: HGNCLoader,
    UBERGRAPH: UGLoader,
    VP: VPLoader,
    HMDB: HMDBLoader,
    GWAS_CATALOG: GWASCatalogLoader,
    GTEX: GTExLoader,
    DRUG_CENTRAL: DrugCentralLoader,
    PHAROS: PHAROSLoader,
    HETIO: HetioLoader,
    BIOLINK: BLLoader,
    PANTHER: PLoader,
    UNIREF: UniRefSimLoader

    # items to go
    # chemnorm,
    # cord19-scibite,
    # cord19-scigraph,
    # covid-phenotypes,
    # mychem,
    # ontological-hierarchy,
    # textminingkp,

    # items with issues
    # FOODB: FDBLoader - no longer has curies that will normalize
}

RESOURCE_HOGS = [GTEX, UNIREF]


class SourceDataLoadManager:

    def __init__(self,
                 test_mode: bool = False,
                 source_subset: list = None,
                 fresh_start_mode: bool = False):

        self.logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.test_mode = test_mode
        if test_mode:
            self.logger.info(f'SourceDataLoadManager running in test mode...')

        self.fresh_start_mode = fresh_start_mode
        if fresh_start_mode:
            self.logger.info(f'SourceDataLoadManager running in fresh start mode... previous state is ignored.')

        # locate and verify the main data directory
        self.data_dir = self.init_data_dir()

        # load the sources spec which specifies which data sources to download and parse
        self.sources_without_strict_normalization = []
        self.load_sources_spec()
        self.logger.info(f'Sources spec loaded. Source list: {self.source_list}')

        # if there is a subset specified with the command line override the full source list
        if source_subset:
            self.source_list = source_subset
            self.logger.info(f'Active sources: {source_subset}')
        else:
            self.logger.info(f'Active sources: All')

        invalid_sources = [source for source in self.source_list if source not in SOURCE_DATA_LOADER_CLASSES.keys()]
        if invalid_sources:
            self.logger.error(f'Sources ({invalid_sources}) are not valid - no loader class set up. Ignoring them.')
            self.source_list = [source for source in self.source_list if source not in invalid_sources]

        # set up the individual subdirectories for each data source
        self.init_source_dirs()

        # dict of data_source_id -> MetadataManager object
        self.metadata = {}

        # dict of data_source_id -> latest source version (to prevent double lookups)
        self.new_version_lookup = {}

        # load any existing metadata found in storage data dir
        self.load_previous_metadata()

        # placeholders for lazy instantiation
        self.normalization_version = None
        self.supplementation_version = None

    def start(self):
        # sources_to_run_in_parallel = [source_id for source_id in self.source_list if source_id not in RESOURCE_HOGS]
        #
        # with Pool() as p:
        #    p.map(self.run_pipeline, sources_to_run_in_parallel)
        #
        # sources_to_run_sequentially = [source_id for source_id in self.source_list if source_id in RESOURCE_HOGS]

        sources_to_run_sequentially = self.source_list
        for source_id in sources_to_run_sequentially:
            self.run_pipeline(source_id)

    def run_pipeline(self, source_id: str):
        self.logger.debug(f"Checking for work to do on source {source_id}...")
        if self.update_needed(source_id):
            self.logger.debug(f"Updating source {source_id}...")
            self.update_source(source_id)
            self.logger.debug(f"Updating source {source_id} complete...")
        if self.normalization_needed(source_id):
            self.logger.debug(f"Normalizing source {source_id}...")
            self.normalize_source(source_id)
            self.logger.debug(f"Normalizing source {source_id} complete...")
        if self.supplementation_needed(source_id):
            self.logger.debug(f"Supplementing source {source_id}...")
            self.supplement_source(source_id)
            self.logger.debug(f"Supplementing source {source_id} complete...")

    def load_previous_metadata(self):
        for source_id in self.source_list:
            self.metadata[source_id] = Metadata(source_id, self.get_source_dir_path(source_id))
            if self.fresh_start_mode:
                self.metadata[source_id].reset_state_metadata()

    def update_needed(self, source_id: str):
        source_metadata = self.metadata[source_id]
        update_status = source_metadata.get_update_status()
        if update_status == Metadata.NOT_STARTED:
            return True
        elif update_status == Metadata.IN_PROGRESS:
            return False
        elif update_status == Metadata.BROKEN or update_status == Metadata.FAILED:
            return False
        else:
            try:
                loader = SOURCE_DATA_LOADER_CLASSES[source_id]()
                self.logger.debug(f"Retrieving latest source version for {source_id}...")
                latest_source_version = loader.get_latest_source_version()
                last_version = source_metadata.get_source_version()
                if latest_source_version != last_version:
                    self.logger.info(f"Found new source version for {source_id}: {latest_source_version}. "
                                     f"(current version: {last_version}) Archiving previous version..")
                    self.archive_previous_load(source_metadata)
                    source_metadata.reset_state_metadata()
                    source_metadata.increment_load_version()
                    # loader.clean_up()
                    self.new_version_lookup[source_id] = latest_source_version
                    return True
                else:
                    self.logger.debug(f"Source version for {source_id} is up to date ({latest_source_version})")
                    return False
            except SourceDataFailedError as failed_error:
                # TODO report these by email or something automated
                self.logger.info(
                    f"Error while checking for updated version for {source_id}: {failed_error.error_message}")
                source_metadata.set_version_update_error(failed_error.error_message)
                source_metadata.set_version_update_status(Metadata.FAILED)
                return False

    def update_source(self, source_id: str, retries: int = 0):
        source_metadata = self.metadata[source_id]
        source_metadata.set_update_status(Metadata.IN_PROGRESS)
        self.logger.debug(f"Updating source data for {source_id}...")
        try:
            # create an instance of the appropriate loader using the source_data_loader_classes lookup map
            source_data_loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode)

            # update the version
            if source_id in self.new_version_lookup:
                latest_source_version = self.new_version_lookup[source_id]
            else:
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = source_data_loader.get_latest_source_version()
                self.new_version_lookup[source_id] = latest_source_version
                self.logger.info(f"Found source version for {source_id}: {latest_source_version}")
            source_metadata.set_source_version(latest_source_version)

            # call the loader - retrieve/parse data and write to a kgx file
            self.logger.info(f"Loading {source_id} ({latest_source_version})...")
            nodes_output_file_path = self.get_source_node_file_path(source_id)
            edges_output_file_path = self.get_source_edge_file_path(source_id)
            update_metadata = source_data_loader.load(nodes_output_file_path, edges_output_file_path)

            # update the associated metadata
            self.logger.info(f"Load finished. Updating {source_id} metadata...")
            has_sequence_variants = source_data_loader.has_sequence_variants()
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_update_info(update_metadata,
                                            update_time=current_time,
                                            has_sequence_variants=has_sequence_variants)
            source_metadata.set_update_status(Metadata.STABLE)
            source_metadata.set_normalization_status(Metadata.WAITING_ON_DEPENDENCY)
            source_metadata.set_supplementation_status(Metadata.WAITING_ON_DEPENDENCY)
            self.logger.info(f"Updating {source_id} complete.")

        except SourceDataBrokenError as broken_error:
            # TODO report these by email or something automated
            self.logger.error(f"SourceDataBrokenError while updating {source_id}: {broken_error.error_message}")
            source_metadata.set_update_error(broken_error.error_message)
            source_metadata.set_update_status(Metadata.BROKEN)

        except SourceDataFailedError as failed_error:
            self.logger.error(f"SourceDataFailedError while updating {source_id}: {failed_error.error_message}")
            source_metadata.set_update_error(f'{failed_error.error_message}')
            source_metadata.set_update_status(Metadata.FAILED)

        except GetDataPullError as data_pull_error:
            self.logger.error(f"GetDataPullError while updating {source_id}: {data_pull_error.error_message}")
            # allow two retries for this type of error
            if retries < 2:
                self.logger.error(f"Retrying update for {source_id}.. (retry {retries + 1})")
                self.update_source(source_id, retries + 1)
            else:
                # after that set FAILED state and bail
                source_metadata.set_update_error(f'{data_pull_error.error_message}')
                source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            # TODO report these by email or something automated
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

    def normalization_needed(self, source_id: str):
        # we only proceed with normalization if the latest source data update is stable
        source_metadata = self.metadata[source_id]
        if source_metadata.get_update_status() == Metadata.STABLE:
            normalization_status = source_metadata.get_normalization_status()
            if normalization_status == Metadata.NOT_STARTED or \
                    normalization_status == Metadata.WAITING_ON_DEPENDENCY:
                return True
            elif normalization_status == Metadata.STABLE:
                if self.normalization_version is None:
                    self.normalization_version = self.get_current_normalization_version()
                current_source_norm_version = source_metadata.get_normalization_version()
                if self.normalization_version != current_source_norm_version:
                    self.logger.info(f'Normalization version ({current_source_norm_version})'
                                     f' is out of date. New version: {self.normalization_version}.')
                    return True
                return False
            elif normalization_status == Metadata.FAILED or \
                    normalization_status == Metadata.BROKEN:
                return False
            elif normalization_status == Metadata.IN_PROGRESS:
                return False
            else:
                self.logger.warning(f'Normalization Status {normalization_status} not recognized..')
                return False

    def get_current_normalization_version(self):
        node_normalizer = NodeNormUtils()
        node_norm_version = node_normalizer.get_current_node_norm_version()
        edge_normalizer = EdgeNormUtils()
        edge_norm_version = edge_normalizer.get_current_edge_norm_version()
        current_normalization_version = f'{node_norm_version}__{edge_norm_version}'
        return current_normalization_version

    def normalize_source(self, source_id: str):
        self.logger.debug(f"Normalizing source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_normalization_status(Metadata.IN_PROGRESS)
        try:
            strict_normalization = False if source_id in self.sources_without_strict_normalization else True

            has_sequence_variants = source_metadata.has_sequence_variants()

            nodes_source_file_path = self.get_source_node_file_path(source_id)
            nodes_norm_file_path = self.get_normalized_node_file_path(source_id)
            node_norm_map_file_path = self.get_node_norm_map_file_path(source_id)
            node_norm_failures_file_path = self.get_node_norm_failures_file_path(source_id)
            edges_source_file_path = self.get_source_edge_file_path(source_id)
            edges_norm_file_path = self.get_normalized_edge_file_path(source_id)
            edge_norm_predicate_map_file_path = self.get_edge_norm_predicate_map_file_path(source_id)
            file_normalizer = KGXFileNormalizer(nodes_source_file_path,
                                                nodes_norm_file_path,
                                                node_norm_map_file_path,
                                                node_norm_failures_file_path,
                                                edges_source_file_path,
                                                edges_norm_file_path,
                                                edge_norm_predicate_map_file_path,
                                                has_sequence_variants=has_sequence_variants,
                                                strict_normalization=strict_normalization)
            normalization_info = file_normalizer.normalize_kgx_files()

            # update the associated metadata
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_normalization_info(normalization_info, normalization_time=current_time)
            if not self.normalization_version:
                self.normalization_version = self.get_current_normalization_version()
            source_metadata.set_normalization_version(self.normalization_version)
            source_metadata.set_normalization_status(Metadata.STABLE)
            source_metadata.set_supplementation_status(Metadata.WAITING_ON_DEPENDENCY)

        except NormalizationBrokenError as broken_error:
            error_message = f"{source_id} NormalizationBrokenError: {broken_error.error_message} - {broken_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.BROKEN)
        except NormalizationFailedError as failed_error:
            error_message = f"{source_id} NormalizationFailedError: {failed_error.error_message} - {failed_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"Error while normalizing {source_id}: {repr(e)}")
            source_metadata.set_normalization_error(repr(e))
            source_metadata.set_normalization_status(Metadata.FAILED)
            raise e

    def supplementation_needed(self, source_id: str):
        if self.metadata[source_id].get_normalization_status() == Metadata.STABLE:
            supplementation_status = self.metadata[source_id].get_supplementation_status()
            if supplementation_status == Metadata.NOT_STARTED or \
                    supplementation_status == Metadata.WAITING_ON_DEPENDENCY:
                return True
            elif supplementation_status == Metadata.FAILED or supplementation_status == Metadata.BROKEN:
                return False
            elif supplementation_status == Metadata.STABLE:
                return False
            elif supplementation_status == Metadata.IN_PROGRESS:
                return False
            else:
                self.logger.warning(f'Supplementation Status {supplementation_status} not recognized..')
                return False

    def supplement_source(self, source_id: str):
        self.logger.debug(f"Supplementing source {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_supplementation_status(Metadata.IN_PROGRESS)
        try:
            supplementation_info = {}
            if source_metadata.has_sequence_variants():
                nodes_file_path = self.get_normalized_node_file_path(source_id)
                supplemental_node_file_path = self.get_supplemental_node_file_path(source_id)
                normalized_supp_node_file_path = self.get_normalized_supp_node_file_path(source_id)
                supp_node_norm_map_file_path = self.get_supp_node_norm_map_file_path(source_id)
                supp_node_norm_failures_file_path = self.get_supp_node_norm_failures_file_path(source_id)
                supplemental_edge_file_path = self.get_supplemental_edge_file_path(source_id)
                normalized_supp_edge_file_path = self.get_normalized_supplemental_edge_file_path(source_id)
                supp_edge_norm_predicate_map_file_path = self.get_supp_edge_norm_predicate_map_file_path(source_id)
                sv_supp = SequenceVariantSupplementation()
                supplementation_info = sv_supp.find_supplemental_data(nodes_file_path=nodes_file_path,
                                                                      supp_nodes_file_path=supplemental_node_file_path,
                                                                      supp_nodes_norm_file_path=normalized_supp_node_file_path,
                                                                      supp_node_norm_map_file_path=supp_node_norm_map_file_path,
                                                                      supp_node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                                                      supp_edges_file_path=supplemental_edge_file_path,
                                                                      normalized_supp_edge_file_path=normalized_supp_edge_file_path,
                                                                      supp_edge_norm_predicate_map_file_path=supp_edge_norm_predicate_map_file_path)
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_supplementation_info(supplementation_info, supplementation_time=current_time)
            source_metadata.set_supplementation_status(Metadata.STABLE)
        except SupplementationFailedError as failed_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} SupplementationFailedError: " \
                            f"{failed_error.error_message} - {failed_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_supplementation_error(error_message)
            source_metadata.set_supplementation_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"{source_id} Error while supplementing: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_supplementation_error(repr(e))
            source_metadata.set_supplementation_status(Metadata.FAILED)
            raise e

    def get_versioned_file_name(self, source_id: str, load_version: str = 'latest'):
        if load_version == 'latest':
            latest_version = self.metadata[source_id].get_load_version()
            return f'{source_id}_{latest_version}'
        else:
            return f'{source_id}_{load_version}'

    def get_source_node_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_nodes.jsonl')

    def get_source_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_source_edges.jsonl')

    def get_normalized_node_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_nodes.jsonl')

    def get_node_norm_map_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_node_map.json')

    def get_node_norm_failures_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_node_failures.log')

    def get_normalized_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_edges.jsonl')

    def get_edge_norm_predicate_map_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_predicate_map.json')

    def get_supplemental_node_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_nodes.jsonl')

    def get_normalized_supp_node_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_norm_nodes.jsonl')

    def get_supp_node_norm_map_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_norm_node_map.json')

    def get_supp_node_norm_failures_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_norm_nodes_failures.log')

    def get_supplemental_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_edges.jsonl')

    def get_normalized_supplemental_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_norm_edges.jsonl')

    def get_supp_edge_norm_predicate_map_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_norm_predicate_map.json')

    def get_source_dir_path(self, source_id: str):
        return os.path.join(self.data_dir, source_id)

    def archive_previous_load(self, source_metadata: Metadata):
        source_id = source_metadata.source_id
        versioned_file_name = self.get_versioned_file_name(source_id)
        archive_metadata_path = os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_archive.meta.json')
        with open(archive_metadata_path, 'w') as meta_json_file:
            json.dump(source_metadata.metadata, meta_json_file, indent=4)

        source_storage_dir = self.get_source_dir_path(source_id)
        archive_dir = os.path.join(source_storage_dir, versioned_file_name + "/")
        os.mkdir(archive_dir)
        all_files = os.listdir(source_storage_dir)
        for file in all_files:
            if file.startswith(versioned_file_name + '_') and not os.path.isdir(file):
                shutil.move(os.path.join(source_storage_dir, file), os.path.join(archive_dir, file))

    def init_data_dir(self):
        # use the storage directory specified by the environment variable DATA_SERVICES_STORAGE
        if os.path.isdir(os.environ["DATA_SERVICES_STORAGE"]):
            return os.environ["DATA_SERVICES_STORAGE"]
        else:
            # if it isn't a valid dir back out
            raise IOError(f'Storage directory not valid: {os.environ["DATA_SERVICES_STORAGE"]}')

    def init_source_dirs(self):
        # for each source on the source_list make sure they have subdirectories set up
        for source_id in self.source_list:
            source_dir_path = self.get_source_dir_path(source_id)
            if not os.path.isdir(source_dir_path):
                self.logger.info(f"SourceDataLoadManager creating subdirectory for {source_id}... {source_dir_path}")
                os.mkdir(source_dir_path)

    def load_sources_spec(self):
        # check for a sources spec file name specified by the environment variable
        # the custom sources spec file must be relative to the top level of the DATA_SERVICES_STORAGE directory
        if 'DATA_SERVICES_SOURCES_SPEC' in os.environ and os.environ['DATA_SERVICES_SOURCES_SPEC']:
            spec_file_name = os.environ['DATA_SERVICES_SOURCES_SPEC']
            spec_path = os.path.join(self.data_dir, spec_file_name)
            self.logger.info(f'Sources spec loaded... ({spec_path})')
        else:
            # otherwise use the default one included in the codebase
            spec_path = os.path.dirname(os.path.abspath(__file__)) + '/../default-sources-spec.yml'
            self.logger.info(f'Default sources spec loaded... ({spec_path})')

        with open(spec_path) as spec_file:
            sources_spec = yaml.full_load(spec_file)
            self.source_list = []
            for data_source_spec in sources_spec['data_sources']:
                data_source_id = data_source_spec['id']
                self.source_list.append(data_source_id)
                if 'strict_normalization' in data_source_spec:
                    if not data_source_spec['strict_normalization']:
                        self.sources_without_strict_normalization.append(data_source_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('-ds', '--data_source', default='all', help=f'Select a single data source to process from the following: {SOURCE_DATA_LOADER_CLASSES.keys()}')
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    parser.add_argument('-f', '--fresh_start', action='store_true', help='Fresh start mode will ignore previous states and start fresh.')
    args = parser.parse_args()

    data_source = args.data_source
    if 'DATA_SERVICES_TEST_MODE' in os.environ:
        test_mode_from_env = os.environ['DATA_SERVICES_TEST_MODE']
    else:
        test_mode_from_env = False

    loader_test_mode = args.test_mode or test_mode_from_env

    fresh_start_mode = args.fresh_start

    if data_source == "all":
        load_manager = SourceDataLoadManager(test_mode=loader_test_mode)
        load_manager.start()
    else:
        if data_source not in SOURCE_DATA_LOADER_CLASSES.keys():
            print(f'Data source not valid. Aborting. (Invalid source: {data_source})')
        else:
            load_manager = SourceDataLoadManager(source_subset=[data_source],
                                                 test_mode=loader_test_mode,
                                                 fresh_start_mode=fresh_start_mode)
            load_manager.start()
