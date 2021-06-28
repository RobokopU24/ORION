
import os
import argparse
import yaml
import datetime

from Common.utils import LoggingUtil
from Common.kgx_file_normalizer import KGXFileNormalizer, NormalizationBrokenError, NormalizationFailedError
from Common.metadata_manager import MetadataManager as Metadata
from Common.loader_interface import SourceDataBrokenError, SourceDataFailedError
from Common.supplementation import SequenceVariantSupplementation, SupplementationFailedError
from parsers.GWASCatalog.src.loadGWASCatalog import GWASCatalogLoader
from parsers.CTD.src.loadCTD import CTDLoader
#from parsers.FooDB.src.loadFDB import FDBLoader
from parsers.GOA.src.loadGOA import GOALoader
from parsers.IntAct.src.loadIA import IALoader
#from parsers.PHAROS.src.loadPHAROS import PHAROSLoader
from parsers.UberGraph.src.loadUG import UGLoader
from parsers.ViralProteome.src.loadVP import VPLoader
#from parsers.ViralProteome.src.loadUniRef import UniRefSimLoader
from parsers.gtopdb.src.loadGtoPdb import GtoPdbLoader
from parsers.hmdb.src.loadHMDB import HMDBLoader
from parsers.hgnc.src.loadHGNC import HGNCLoader
from parsers.panther.src.loadPanther import PLoader
from parsers.GTEx.src.loadGTEx import GTExLoader

GWAS_CATALOG = 'GWASCatalog'
CTD = 'CTD'
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

SOURCE_DATA_LOADER_CLASSES = {
    CTD: CTDLoader,
    INTACT: IALoader,
    GTOPDB: GtoPdbLoader,
    HUMAN_GOA: GOALoader,
    HGNC: HGNCLoader,
    UBERGRAPH: UGLoader,
    VP: VPLoader,
    HMDB: HMDBLoader,
    GWAS_CATALOG: GWASCatalogLoader,
    GTEX: GTExLoader,

    # in progress
    PANTHER: PLoader

    # items to go
    # biolink,
    # chembio,
    # chemnorm,
    # cord19-scibite,
    # cord19-scigraph,
    # covid-phenotypes,
    # hetio,
    # kegg,
    # mychem,
    # ontological-hierarchy,
    # textminingkp,

    # items with issues
    # PHAROS: PHAROSLoader - normalization issues in load manager. normalization lists are too large to parse.
    # FOODB: FDBLoader - no longer has curies that will normalize
    # UNIREF: UniRefSimLoader - normalization issues in load manager. normalization lists are too large to parse.
}


class SourceDataLoadManager:

    def __init__(self,
                 test_mode: bool = False,
                 source_subset: list = None):

        self.logger = LoggingUtil.init_logging("Data_services.Common.SourceDataLoadManager",
                                               line_format='medium',
                                               log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.test_mode = test_mode
        if test_mode:
            self.logger.info(f'SourceDataLoadManager running in test mode...')

        # locate and verify the main data directory
        self.data_dir = self.init_data_dir()
        self.workspace_dir = os.path.join(self.data_dir, "resources")

        # load the config which sets up information about the data sources
        self.sources_without_strict_normalization = []
        self.load_config()
        self.logger.info(f'Config loaded. Source list: {self.source_list}')

        # if there is a subset specified with the command line override the master source list
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

    def start(self):
        work_to_do, source_id = self.find_work_to_do()
        while work_to_do:
            work_to_do(source_id)
            work_to_do, source_id = self.find_work_to_do()
        self.logger.info(f'Work complete!')

    def find_work_to_do(self):

        self.logger.info(f'Checking for sources to update...')
        source_id = self.find_a_source_to_update()
        if source_id:
            return self.update_source, source_id

        self.logger.info(f'No more sources to update.. Checking for sources to normalize...')
        source_id = self.find_a_source_to_normalize()
        if source_id:
            return self.normalize_source, source_id

        self.logger.info(f'No more sources to normalize.. Checking for sources to supplement...')
        source_id = self.find_a_source_for_supplementation()
        if source_id:
            return self.supplement_source, source_id

        self.logger.info(f'No more sources to supplement..')
        return None, None

    def load_previous_metadata(self):
        for source_id in self.source_list:
            self.metadata[source_id] = Metadata(source_id, self.get_source_dir_path(source_id))

    def find_a_source_to_update(self):
        for source_id in self.source_list:
            if self.check_if_source_needs_update(source_id):
                return source_id
        return None

    def check_if_source_needs_update(self, source_id):
        source_metadata = self.metadata[source_id]
        update_status = source_metadata.get_update_status()
        if update_status == Metadata.NOT_STARTED:
            return True
        elif update_status == Metadata.IN_PROGRESS:
            return False
        elif update_status == Metadata.BROKEN or update_status == Metadata.FAILED:
            # TODO do we want to retry these automatically?
            return False
        else:
            try:
                loader = SOURCE_DATA_LOADER_CLASSES[source_id]()
                self.logger.info(f"Retrieving source version for {source_id}...")
                latest_source_version = loader.get_latest_source_version()
                if latest_source_version != source_metadata.get_source_version():
                    self.logger.info(f"Found new source version for {source_id}: {latest_source_version}")
                    source_metadata.archive_metadata()
                    self.new_version_lookup[source_id] = latest_source_version
                    return True
                else:
                    self.logger.info(f"Source version for {source_id} is up to date ({latest_source_version})")
                    return False
            except SourceDataFailedError as failed_error:
                # TODO report these by email or something automated
                self.logger.info(
                    f"SourceDataFailedError while checking for updated version for {source_id}: {failed_error.error_message}")
                source_metadata.set_version_update_error(failed_error.error_message)
                source_metadata.set_version_update_status(Metadata.FAILED)
                return False

    def update_source(self, source_id: str):
        source_metadata = self.metadata[source_id]
        source_metadata.set_update_status(Metadata.IN_PROGRESS)
        self.logger.info(f"Updating source data for {source_id}...")
        try:
            # create an instance of the appropriate loader using the source_data_loader_classes lookup map
            source_data_loader = SOURCE_DATA_LOADER_CLASSES[source_id](test_mode=self.test_mode)

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
            # TODO report these by email or something automated
            self.logger.info(f"SourceDataFailedError while updating {source_id}: {failed_error.error_message}")
            source_metadata.set_update_error(f'{failed_error.error_message}')
            source_metadata.set_update_status(Metadata.FAILED)

        except Exception as e:
            # TODO report these by email or something automated
            source_metadata.set_update_error(repr(e))
            source_metadata.set_update_status(Metadata.FAILED)
            raise e

    def find_a_source_to_normalize(self):
        for source_id in self.source_list:
            source_metadata = self.metadata[source_id]
            # we only proceed with normalization if the latest source data update is stable
            if source_metadata.get_update_status() == Metadata.STABLE:
                normalization_status = source_metadata.get_normalization_status()
                # if we haven't attempted normalization for this source data version, queue it up
                if normalization_status == Metadata.NOT_STARTED or \
                        normalization_status == Metadata.WAITING_ON_DEPENDENCY:
                    return source_id
                elif normalization_status == Metadata.FAILED or \
                        normalization_status == Metadata.BROKEN:
                    # TODO do we want to retry these automatically?
                    pass
        return None

    def normalize_source(self, source_id: str):
        self.logger.info(f"Normalizing source data for {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_normalization_status(Metadata.IN_PROGRESS)
        try:
            strict_normalization = False if source_id in self.sources_without_strict_normalization else True

            has_sequence_variants = source_metadata.has_sequence_variants()

            self.logger.info(f"Normalizing KGX files for {source_id}...")
            nodes_source_file_path = self.get_source_node_file_path(source_id)
            nodes_norm_file_path = self.get_normalized_node_file_path(source_id)
            node_norm_failures_file_path = self.get_node_norm_failures_file_path(source_id)
            edges_source_file_path = self.get_source_edge_file_path(source_id)
            edges_norm_file_path = self.get_normalized_edge_file_path(source_id)
            edge_norm_predicate_map_file_path = self.get_edge_norm_predicate_map_file_path(source_id)
            file_normalizer = KGXFileNormalizer(nodes_source_file_path,
                                                nodes_norm_file_path,
                                                node_norm_failures_file_path,
                                                edges_source_file_path,
                                                edges_norm_file_path,
                                                edge_norm_predicate_map_file_path,
                                                has_sequence_variants=has_sequence_variants,
                                                strict_normalization=strict_normalization)

            normalization_info = file_normalizer.normalize_kgx_files()
            # self.logger.info(f"Normalization info for {source_id}: {normalization_info}")

            # update the associated metadata
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_normalization_info(normalization_info, normalization_time=current_time)
            source_metadata.set_normalization_status(Metadata.STABLE)
            source_metadata.set_supplementation_status(Metadata.WAITING_ON_DEPENDENCY)
            self.logger.info(f"Normalizing source {source_id} complete.")

        except NormalizationBrokenError as broken_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} NormalizationBrokenError: {broken_error.error_message} - {broken_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.BROKEN)
        except NormalizationFailedError as failed_error:
            # TODO report these by email or something automated
            error_message = f"{source_id} NormalizationFailedError: {failed_error.error_message} - {failed_error.actual_error}"
            self.logger.error(error_message)
            source_metadata.set_normalization_error(error_message)
            source_metadata.set_normalization_status(Metadata.FAILED)
        except Exception as e:
            self.logger.error(f"Error while normalizing {source_id}: {repr(e)}")
            # TODO report these by email or something automated
            source_metadata.set_normalization_error(repr(e))
            source_metadata.set_normalization_status(Metadata.FAILED)
            raise e

    def find_a_source_for_supplementation(self):
        for source_id in self.source_list:
            if self.metadata[source_id].get_normalization_status() == Metadata.STABLE:
                supplementation_status = self.metadata[source_id].get_supplementation_status()
                if supplementation_status == Metadata.NOT_STARTED or \
                        supplementation_status == Metadata.WAITING_ON_DEPENDENCY:
                    return source_id
                elif supplementation_status == Metadata.FAILED or supplementation_status == Metadata.BROKEN:
                    # TODO do we want to retry these automatically?
                    pass
        return None

    def supplement_source(self, source_id: str):
        self.logger.info(f"Supplementing source {source_id}...")
        source_metadata = self.metadata[source_id]
        source_metadata.set_supplementation_status(Metadata.IN_PROGRESS)
        try:
            supplementation_info = {}
            if source_metadata.has_sequence_variants():
                nodes_file_path = self.get_normalized_node_file_path(source_id)
                supplemental_node_file_path = self.get_supplemental_node_file_path(source_id)
                normalized_supp_node_file_path = self.get_normalized_supp_node_file_path(source_id)
                supp_node_norm_failures_file_path = self.get_supp_node_norm_failures_file_path(source_id)
                supplemental_edge_file_path = self.get_supplemental_edge_file_path(source_id)
                normalized_supp_edge_file_path = self.get_normalized_supplemental_edge_file_path(source_id)
                supp_edge_norm_predicate_map_file_path = self.get_supp_edge_norm_predicate_map_file_path(source_id)
                sv_supp = SequenceVariantSupplementation(workspace_dir=self.workspace_dir)
                supplementation_info = sv_supp.find_supplemental_data(nodes_file_path=nodes_file_path,
                                                                      supp_nodes_file_path=supplemental_node_file_path,
                                                                      normalized_supp_node_file_path=normalized_supp_node_file_path,
                                                                      supp_node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                                                      supp_edges_file_path=supplemental_edge_file_path,
                                                                      normalized_supp_edge_file_path=normalized_supp_edge_file_path,
                                                                      supp_edge_norm_predicate_map_file_path=supp_edge_norm_predicate_map_file_path)
            current_time = datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S')
            source_metadata.set_supplementation_info(supplementation_info, supplementation_time=current_time)
            source_metadata.set_supplementation_status(Metadata.STABLE)
            self.logger.info(f"Supplementing source {source_id} complete.")
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
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_nodes.jsonl')

    def get_supp_node_norm_failures_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_nodes_failures.log')

    def get_supplemental_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_supp_edges.jsonl')

    def get_normalized_supplemental_edge_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_edges.jsonl')

    def get_supp_edge_norm_predicate_map_file_path(self, source_id: str, load_version: str = 'latest'):
        versioned_file_name = self.get_versioned_file_name(source_id, load_version)
        return os.path.join(self.get_source_dir_path(source_id), f'{versioned_file_name}_norm_supp_predicate_map.json')

    def get_source_dir_path(self, source_id: str):
        return os.path.join(self.data_dir, source_id)

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

    def load_config(self):
        # check for a config file name specified by the environment variable
        # the custom config file must be relative to the top level of the data directory
        if 'DATA_SERVICES_CONFIG' in os.environ and os.environ['DATA_SERVICES_CONFIG']:
            config_file_name = os.environ['DATA_SERVICES_CONFIG']
            config_path = os.path.join(self.data_dir, config_file_name)
        else:
            # otherwise use the default one included in the codebase
            config_path = os.path.dirname(os.path.abspath(__file__)) + '/../default-config.yml'

        with open(config_path) as config_file:
            config = yaml.full_load(config_file)
            self.source_list = []
            for data_source_config in config['data_sources']:
                data_source_id = data_source_config['id']
                self.source_list.append(data_source_id)
                if 'strict_normalization' in data_source_config:
                    if not data_source_config['strict_normalization']:
                        self.sources_without_strict_normalization.append(data_source_id)
        self.logger.debug(f'Config loaded... ({config_path})')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files.")
    parser.add_argument('-ds', '--data_source', default='all', help=f'Select a single data source to process from the following: {SOURCE_DATA_LOADER_CLASSES.keys()}')
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    args = parser.parse_args()

    data_source = args.data_source
    if 'DATA_SERVICES_TEST_MODE' in os.environ:
        test_mode_from_env = os.environ['DATA_SERVICES_TEST_MODE']
    else:
        test_mode_from_env = False

    loader_test_mode = args.test_mode or test_mode_from_env

    if data_source == "all":
        load_manager = SourceDataLoadManager(test_mode=loader_test_mode)
        load_manager.start()
    else:
        if data_source not in SOURCE_DATA_LOADER_CLASSES.keys():
            print(f'Data source not valid. Aborting. (Invalid source: {data_source})')
        else:
            load_manager = SourceDataLoadManager(source_subset=[data_source], test_mode=loader_test_mode)
            load_manager.start()
