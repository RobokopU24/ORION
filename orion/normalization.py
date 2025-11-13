import os
import logging
import requests
import time

from requests.adapters import HTTPAdapter, Retry
from dataclasses import dataclass

from robokop_genetics.genetics_normalization import GeneticsNormalizer
from orion.biolink_constants import *
from orion.utils import LoggingUtil

NORMALIZATION_CODE_VERSION = '1.4'

# node property name for node types that did not normalize
CUSTOM_NODE_TYPES = 'custom_node_types'

# predicate to use when normalization fails
FALLBACK_EDGE_PREDICATE = 'biolink:related_to'


@dataclass
class NormalizationScheme:
    node_normalization_version: str = 'latest'
    edge_normalization_version: str = 'latest'
    normalization_code_version: str = NORMALIZATION_CODE_VERSION
    strict: bool = True
    conflation: bool = False

    def get_composite_normalization_version(self):
        composite_normalization_version = f'{self.node_normalization_version}_' \
                                f'{self.edge_normalization_version}_{self.normalization_code_version}'
        if self.conflation:
            composite_normalization_version += '_conflated'
        if self.strict:
            composite_normalization_version += '_strict'
        return composite_normalization_version

    def get_metadata_representation(self):
        return {'node_normalization_version': self.node_normalization_version,
                'edge_normalization_version': self.edge_normalization_version,
                'normalization_code_version': self.normalization_code_version,
                'conflation': self.conflation,
                'strict': self.strict}


class NormalizationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: Exception = None):
        self.error_message = error_message
        self.actual_error = actual_error

NODE_NORMALIZATION_URL = os.environ.get('NODE_NORMALIZATION_ENDPOINT', 'https://nodenormalization-sri.renci.org/')


class NodeNormalizer:
    """
    Class that contains methods relating to node normalization of KGX data.

    the input node list should be KGX compliant and have the following columns that may be
    changed during the normalization:

        id: the id value to be normalized upon
        name: the name of the node
        category: the semantic type(s)
        equivalent_identifiers: the list of synonymous ids
    """

    def __init__(self,
                 log_level=logging.INFO,
                 node_normalization_version: str = 'latest',
                 biolink_version: str = 'latest',
                 strict_normalization: bool = True,
                 conflate_node_types: bool = False,
                 include_taxa: bool = False):
        """
        constructor
        :param log_level - overrides default log level
        :param node_normalization_version - not implemented yet
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("ORION.Common.NodeNormalizer",
                                               level=log_level,
                                               line_format='medium',
                                               log_file_path=os.getenv('ORION_LOGS'))
        # storage for regular nodes that failed to normalize
        self.failed_to_normalize_ids = set()
        # storage for variant nodes that failed to normalize
        self.failed_to_normalize_variant_ids = {}
        # flag that determines whether nodes that failed to normalize should be included or thrown out
        self.strict_normalization = strict_normalization
        self.biolink_version = biolink_version
        self.biolink_compliant_node_types = None
        # whether the normalizer should conflate node types (ie combine genes and proteins)
        self.conflate_node_types = conflate_node_types
        # whether to include taxa in the responses and assign them to nodes
        self.include_taxa = include_taxa
        # storage for variant nodes that split into multiple new nodes in normalization
        self.variant_node_splits = {}
        # normalization map for future look up of all normalized node IDs
        self.node_normalization_lookup = {}
        self.sequence_variant_normalizer = None
        self.variant_node_types = None
        self.requests_session = self.get_normalization_requests_session()

    def hit_node_norm_service(self, curies, retries=0):
        resp: requests.models.Response = \
            self.requests_session.post(f'{NODE_NORMALIZATION_URL}get_normalized_nodes',
                                       json={'curies': curies,
                                             'conflate': self.conflate_node_types,
                                             'drug_chemical_conflate': self.conflate_node_types,
                                             'description': True,
                                             'include_taxa': self.include_taxa})
        if resp.status_code == 200:
            # if successful return the json as an object
            response_json = resp.json()
            if response_json:
                return response_json
            else:
                error_message = f"Node Normalization service {NODE_NORMALIZATION_URL} returned 200 " \
                                f"but with an empty result for (curies: {curies})"
                raise NormalizationFailedError(error_message=error_message)
        else:
            error_message = f'Node norm response code: {resp.status_code} (curies: {curies})'
            self.logger.error(error_message)
            resp.raise_for_status()

    def normalize_node_data(self, node_list: list, batch_size: int = 1000) -> list:
        """
        This method calls the NodeNormalization web service and normalizes a list of nodes.

        :param node_list: A list of unique nodes to normalize
        :param batch_size: the number of curies to be sent to NodeNormalization at once

        :return:
        """

        # look up all valid biolink node types if needed
        # this is used when strict normalization is off to ensure only valid types go into the graph as NODE_TYPES
        if not self.strict_normalization and not self.biolink_compliant_node_types:
            biolink_lookup = EdgeNormalizer(edge_normalization_version=self.biolink_version)
            self.biolink_compliant_node_types = biolink_lookup.get_valid_node_types()

        # make a list of the node ids, we used to deduplicate here, but now we expect the list to be unique ids
        to_normalize: list = [node['id'] for node in node_list]

        # use indexes and slice to grab batch_size sized chunks of ids from the list
        start_index: int = 0
        last_index: int = len(to_normalize)
        chunks_of_ids = []
        while True:
            if start_index < last_index:
                end_index: int = start_index + batch_size

                # force the end index to be no greater than the last index to ensure no overflow
                if end_index >= last_index:
                    end_index = last_index

                # collect a slice of block_size curies from the full list
                chunks_of_ids.append(to_normalize[start_index: end_index])

                # move on down the list
                start_index += batch_size
            else:
                break

        # we should be able to do the following, but it's causing RemoteDisconnected errors with node norm
        #
        # hit the node norm api with the chunks of curies in parallel
        # we could try to optimize the number of max_workers for ThreadPoolExecutor more specifically,
        # by default python attempts to find a reasonable # based on os.cpu_count()
        # with ThreadPoolExecutor() as executor:
        #     executor_results = executor.map(self.hit_node_norm_service, chunks_of_ids)
        #
        # normalization_results = list(executor_results)
        # for normalization_json, ids in zip(normalization_results, chunks_of_ids):
        #     if not normalization_json:
        #        raise NormalizationFailedError(f'!!! Normalization json results missing for ids: {ids}')
        #    else:
        #        merge the normalization results into one dictionary
        #        node_normalization_results.update(**normalization_json)

        # until we can get threading working, hit node norm sequentially
        node_normalization_results: dict = {}
        for chunk in chunks_of_ids:
            results = self.hit_node_norm_service(chunk)
            node_normalization_results.update(**results)

        # reset the node index
        node_idx = 0

        # node ids that failed to normalize
        failed_to_normalize: list = []

        # for each node update the node with normalized information
        # store the normalized IDs in self.node_normalization_lookup for later look up
        while node_idx < len(node_list):

            # get the next node list item by index
            current_node = node_list[node_idx]
            current_node_id = current_node['id']

            # make sure there is a name
            if 'name' not in current_node or not current_node['name']:
                current_node['name'] = current_node['id'].split(':')[-1]

            # remove properties with null values, remove newline characters
            for key in list(current_node.keys()):
                value = current_node[key]
                if value is None:
                    del(current_node[key])
                else:
                    if isinstance(value, str):
                        current_node[key] = value.replace("\n", "")

            # if strict normalization is off, enforce valid node types
            if not self.strict_normalization:

                if NODE_TYPES not in current_node:
                    current_node[NODE_TYPES] = [NAMED_THING]

                # remove all the bad types and make them a property instead
                invalid_node_types = [node_type for node_type in current_node[NODE_TYPES] if
                                      node_type not in self.biolink_compliant_node_types]
                if invalid_node_types:
                    current_node[CUSTOM_NODE_TYPES] = invalid_node_types

                # keep all the valid types
                current_node[NODE_TYPES] = [node_type for node_type in current_node[NODE_TYPES] if
                                            node_type in self.biolink_compliant_node_types]
                # add the NAMED_THING type if it's not there
                if NAMED_THING not in current_node[NODE_TYPES]:
                    current_node[NODE_TYPES].append(NAMED_THING)

                # enforce that the list is really a set
                current_node[NODE_TYPES] = list(set(current_node[NODE_TYPES]))

            # did we get a response from the normalizer
            current_node_normalization = node_normalization_results[current_node_id]
            if current_node_normalization is not None:

                current_node_id_section = current_node_normalization['id']

                # update the node with the normalized info
                normalized_id = current_node_id_section['identifier']
                current_node['id'] = normalized_id
                current_node[NODE_TYPES] = current_node_normalization['type']
                current_node[SYNONYMS] = list(item['identifier'] for item in current_node_normalization[SYNONYMS])

                # set the name as the primary label if it exists
                if 'label' in current_node_id_section:
                    current_node['name'] = current_node_id_section['label']

                # set the node description and/or information content if they are present
                if 'information_content' in current_node_normalization:
                    current_node[INFORMATION_CONTENT] = current_node_normalization[INFORMATION_CONTENT]
                if 'description' in current_node_id_section:
                    current_node[DESCRIPTION] = current_node_id_section['description']
                # we'd like to do this but there's no biolink node property for it
                # if 'taxa' in current_node_id_section:
                #    current_node[something] = current_node_id_section['taxa']

                self.node_normalization_lookup[current_node_id] = [normalized_id]
            else:
                # we didn't find a normalization - add it to the failure list
                failed_to_normalize.append(current_node_id)
                if self.strict_normalization:
                    # if strict normalization is on we set that index to None so that the node is removed
                    node_list[node_idx] = None
                    # store None in the normalization map so we know it didn't normalize
                    self.node_normalization_lookup[current_node_id] = None
                else:
                    # if strict normalization is off keep it and set its previous id in the normalization map
                    self.node_normalization_lookup[current_node_id] = [current_node_id]

            # go to the next node index
            node_idx += 1

        # if something failed to normalize - log it and optionally remove it from the node list
        if len(failed_to_normalize) > 0:
            self.failed_to_normalize_ids.update(failed_to_normalize)

            # if strict remove all nodes that failed normalization
            if self.strict_normalization:
                node_list[:] = [d for d in node_list if d is not None]

        self.logger.debug(f'End of normalize_node_data.')

        # return the failed list to the caller
        return failed_to_normalize

    def normalize_sequence_variants(self, variant_nodes: list):

        if not variant_nodes:
            return

        if not self.sequence_variant_normalizer:
            self.sequence_variant_normalizer = GeneticsNormalizer(use_cache=False)
            self.variant_node_types = self.sequence_variant_normalizer.get_sequence_variant_node_types()
        variant_node_types = self.variant_node_types

        variant_ids = [node['id'] for node in variant_nodes]
        variant_nodes.clear()

        sequence_variant_norms = self.sequence_variant_normalizer.normalize_variants(variant_ids)
        for variant_id, normalization_response in sequence_variant_norms.items():
            for normalization_info in normalization_response:
                # if the normalization info contains an ID it was a success
                if 'id' in normalization_info:
                    normalized_node = {
                        'id': normalization_info['id'],
                        'name': normalization_info['name'],
                        # as long as sequence variant types are all the same we can skip this assignment
                        # 'category': normalized_info["type"],
                        'category': variant_node_types,
                        'equivalent_identifiers': normalization_info['equivalent_identifiers'],
                        'hgvs': normalization_info['hgvs'],
                        'robokop_variant_id': normalization_info['robokop_variant_id']
                    }
                    variant_nodes.append(normalized_node)
                    # assume we don't have a split and store the id for look up
                    self.node_normalization_lookup[variant_id] = [normalization_info["id"]]
                else:
                    # otherwise an error occurred
                    error_for_logs = f'{normalization_info["error_type"]}: {normalization_info["error_message"]}'
                    self.failed_to_normalize_variant_ids[variant_id] = error_for_logs
                    if self.strict_normalization:
                        self.node_normalization_lookup[variant_id] = None
                    else:
                        self.node_normalization_lookup[variant_id] = [variant_id]
                        # TODO for now we dont preserve other properties on variant nodes that didnt normalize
                        # the splitting makes that complicated and doesnt seem worth it until we have a good use case
                        fake_normalized_node = {
                            'id': variant_id,
                            'name': variant_id,
                            'category': variant_node_types,
                            'equivalent_identifiers': [],
                            'hgvs': []
                        }
                        variant_nodes.append(fake_normalized_node)
            if len(normalization_response) > 1:
                # if we have more than one response here assume its a split variant and no errors
                split_ids = [node['id'] for node in normalization_response]
                self.variant_node_splits[variant_id] = split_ids
                # this will overwrite the previous single IDs stored
                self.node_normalization_lookup[variant_id] = split_ids

        return variant_nodes

    def get_current_node_norm_version(self):
        """
        Retrieves the current production version from the node normalization service
        """
        # hit the node norm status endpoint
        node_norm_status_url = f'{NODE_NORMALIZATION_URL}status'
        resp: requests.models.Response = requests.get(node_norm_status_url)
        resp.raise_for_status()
        status: dict = resp.json()
        # extract the version
        node_norm_version = status['babel_version']
        return node_norm_version


    @staticmethod
    def get_normalization_requests_session():
        pool_maxsize = max(os.cpu_count(), 10)
        s = requests.Session()
        retries = Retry(total=8,
                        backoff_factor=1,
                        status_forcelist=[502, 503, 504, 403, 429],
                        allowed_methods=['GET', 'POST', 'HEAD', 'OPTIONS'])
        s.mount('https://', HTTPAdapter(max_retries=retries, pool_maxsize=pool_maxsize))
        s.mount('http://', HTTPAdapter(max_retries=retries, pool_maxsize=pool_maxsize))
        return s


class EdgeNormalizationResult:
    def __init__(self,
                 predicate: str,
                 inverted: bool = False,
                 properties: dict = None):
        self.predicate = predicate
        self.inverted = inverted
        self.properties = properties


class EdgeNormalizer:
    """
    Class that contains methods relating to edge normalization.
    """

    DEFAULT_EDGE_NORM_ENDPOINT = f'https://bl-lookup-sri.renci.org/'

    def __init__(self,
                 edge_normalization_version: str = 'latest',
                 log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("ORION.Common.EdgeNormalizer", level=log_level, line_format='medium', log_file_path=os.getenv('ORION_LOGS'))
        # normalization map for future look up of all normalized predicates
        self.edge_normalization_lookup = {}
        self.cached_edge_norms = {}

        if 'EDGE_NORMALIZATION_ENDPOINT' in os.environ and os.environ['EDGE_NORMALIZATION_ENDPOINT']:
            self.edge_norm_endpoint = os.environ['EDGE_NORMALIZATION_ENDPOINT']
        else:
            self.edge_norm_endpoint = self.DEFAULT_EDGE_NORM_ENDPOINT

        if edge_normalization_version != 'latest':
            if self.check_bl_version_valid(edge_normalization_version):
                self.edge_norm_version = edge_normalization_version
            else:
                raise requests.exceptions.HTTPError(f'Edge norm version {edge_normalization_version} '
                                                    f'is not supported by endpoint {self.edge_norm_endpoint}.')
        else:
            self.edge_norm_version = self.get_current_edge_norm_version()

    def normalize_edge_data(self,
                            edge_list: list,
                            block_size: int = 100) -> list:
        """
        This method calls the EdgeNormalization web service to retrieve information for normalizing edges.

        :param edge_list: A list of edges to normalize - edges are dictionaries with the PREDICATE constant as a key
        :param block_size: the number of predicates to process in a single call
        :return:
        """

        # find the predicates that have not been normalized yet
        predicates_to_normalize = {edge[PREDICATE] for edge in edge_list
                                   if edge[PREDICATE] not in self.edge_normalization_lookup}

        # convert the set to a list so we can iterate through it
        predicates_to_normalize_list = list(predicates_to_normalize)

        # dictionary to accumulate normalization results
        edge_normalizations = {}

        # indexes for iterating through the list in chunks
        start_index: int = 0
        last_index: int = len(predicates_to_normalize_list)
        while True:
            if start_index >= last_index:
                # no more predicates to process, break the loop
                break

            # define the end index of the slice
            end_index: int = start_index + block_size

            # force the end index to be at most the last index to ensure no overflow
            if end_index > last_index:
                end_index = last_index

            # collect a slice of predicates
            predicate_chunk: list = predicates_to_normalize_list[start_index: end_index]

            # hit the edge normalization service
            request_url = f'{self.edge_norm_endpoint}resolve_predicate?version={self.edge_norm_version}&predicate='
            request_url += '&predicate='.join(predicate_chunk)
            self.logger.debug(f'Sending request: {request_url}')
            resp: requests.models.Response = requests.get(request_url)

            # if we get a success status code
            if resp.status_code == 200:
                # merge the response with what we have already
                rvs: dict = resp.json()
                edge_normalizations.update(**rvs)
            elif resp.status_code == 404:
                # this should not happen but if it does fail gracefully and assume no meaningful normalization results
                # (current versions of bl look up should always return at least a default for each predicate)
                pass
            else:
                # this is a real error with the edge normalizer so we bail
                error_message = f'Edge norm response code: {resp.status_code}'
                self.logger.error(error_message)
                resp.raise_for_status()

            # move on down the list
            start_index += block_size

        # storage for items that failed to normalize
        failed_to_normalize: list = list()

        # walk through the unique predicates and process normalized predicates for the lookup map
        for predicate in predicates_to_normalize:
            # did the service return a value with an identifier
            if predicate in edge_normalizations and \
                    (('predicate' in edge_normalizations[predicate]) or \
                    ('identifier' in edge_normalizations[predicate])):
                normalization_info = edge_normalizations[predicate]
                if 'predicate' in normalization_info:
                    normalized_predicate = normalization_info.pop('predicate')
                else:
                    normalized_predicate = normalization_info.pop('identifier')
                normalization_info.pop('label', None)  # just deleting this key, it's not needed anymore
                inverted = True if normalization_info.pop('inverted', False) else False
                self.edge_normalization_lookup[predicate] = EdgeNormalizationResult(predicate=normalized_predicate,
                                                                                    inverted=inverted,
                                                                                    properties=normalization_info)
            else:
                # this should not happen but if it does use the fallback predicate
                self.edge_normalization_lookup[predicate] = EdgeNormalizationResult(predicate=FALLBACK_EDGE_PREDICATE)
                failed_to_normalize.append(predicate)

        # if something failed to normalize output it
        # if failed_to_normalize:
        #    self.logger.error(f'Failed to normalize: {", ".join(failed_to_normalize)}')

        # return the failed list to the caller
        return failed_to_normalize

    def get_current_edge_norm_version(self):
        """
        Retrieves the current production version from the edge normalization service
        """
        versions = self.get_available_versions()
        return versions[0]

    def check_bl_version_valid(self, bl_version: str):
        """
        Checks if the requested version is supported by the API
        """
        if bl_version in self.get_available_versions():
            return True
        else:
            return False

    def get_available_versions(self):
        # call the versions endpoint
        edge_norm_versions_url = f'{self.edge_norm_endpoint}versions'
        resp: requests.models.Response = requests.get(edge_norm_versions_url)

        # did we get a good status code
        if resp.status_code == 200:
            # parse json
            versions = resp.json()
            return versions  # array of versions
        else:
            # this shouldn't happen, raise an exception
            resp.raise_for_status()

    def check_node_type_valid(self, node_type: str):
        if node_type in self.get_valid_node_types():
            return True
        else:
            return False

    def get_valid_node_types(self):
        # call the descendants endpoint with the root node type
        edge_norm_descendants_url = f'{self.edge_norm_endpoint}bl/{NAMED_THING}/descendants?version={self.edge_norm_version}'
        resp: requests.models.Response = requests.get(edge_norm_descendants_url)

        # did we get a good status code
        if resp.status_code == 200:
            # parse json
            descendants = resp.json()
            return descendants  # array of descendants
        else:
            # this shouldn't happen, raise an exception
            resp.raise_for_status()


NAME_RESOLVER_URL = os.getenv('NAMERES_URL', 'https://name-resolution-sri.renci.org')
NAME_RESOLVER_ENDPOINT = f'{NAME_RESOLVER_URL}/lookup'
NAME_RESOLVER_HEADERS = {"accept": "application/json"}
NAME_RESOLVER_API_ERROR = 'api_error'


def call_name_resolution(name: str, biolink_type: str, retries=0, logger=None):
    nameres_payload = {
        "string": name,
        "biolink_type": biolink_type if biolink_type else "",
        "autocomplete": False
    }
    try:
        # logger.info(f'About to call name res..')
        nameres_result = requests.get(NAME_RESOLVER_ENDPOINT,
                                      params=nameres_payload,
                                      headers=NAME_RESOLVER_HEADERS,
                                      timeout=45)
        # logger.info(f'Got result from name res {nameres_result.status_code}')
        if nameres_result.status_code == 200:
            # return the first result if there is one
            nameres_json = nameres_result.json()
            # logger.info(f'Unpacked json..')
            return nameres_json[0] if nameres_json else None
        else:
            error_message = f'Non-200 result from name resolution (url: {NAME_RESOLVER_URL}, ' \
                            f'payload: {nameres_payload}). Status code: {nameres_result.status_code}.'
    except requests.exceptions.ConnectionError as e:
        error_message = f'Connection Error calling name resolution (url: {NAME_RESOLVER_URL}, ' \
                        f'payload: {nameres_payload}). Error: {e}.'
    except requests.exceptions.Timeout as t:
        error_message = f'Calling name resolution timed out (url: {NAME_RESOLVER_URL}, ' \
                        f'payload: {nameres_payload}). Error: {t}.'

    # if we get here something went wrong, log error and retry
    if logger:
        logger.error(error_message)
    else:
        print(error_message)
    if retries < 2:
        time.sleep(5)
        logger.info('Retrying name resolution..')
        return call_name_resolution(name, biolink_type, retries + 1, logger)

    # if retried 2 times already give up and return the last error
    return {NAME_RESOLVER_API_ERROR: error_message}
