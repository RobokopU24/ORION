import shutil
import os
import json
import yaml

import requests.exceptions

from Common.biolink_utils import BiolinkUtils
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import PUBLICATIONS, NEGATED
from Common.utils import GetData, quick_jsonl_file_iterator
from Common.normalization import call_name_resolution, NAME_RESOLVER_API_ERROR
from Common.predicates import call_pred_mapping
from Common.prefixes import PUBMED

from parsers.LitCoin.src.bagel.bagel_service import call_bagel_service
from parsers.LitCoin.src.bagel.bagel import get_orion_bagel_results, extract_best_match, \
    convert_orion_bagel_result_to_bagel_service_format, BAGEL_SUBJECT_SYN_TYPE, BAGEL_OBJECT_SYN_TYPE, get_llm_results


class LITCOIN:
    EDGE_ID = 'id'
    ABSTRACT_ID = 'abstract_id'
    ABSTRACT_SPAN = 'abstract_span'
    ASSERTION_ID = 'assertion_id'
    ASSERTION_SPAN = 'assertion_span'
    OBJECT_NAME = 'object'
    OBJECT_TYPE = 'object_type'
    OBJECT_QUALIFIER = 'object_qualifier'
    PREDICATE = 'predicate'
    PREDICATE_NEGATED = 'negated'
    RELATIONSHIP = 'relationship'
    RELATIONSHIP_QUALIFIER = 'statement_qualifier'
    SUBJECT_NAME = 'subject'
    SUBJECT_TYPE = 'subject_type'
    SUBJECT_QUALIFIER = 'subject_qualifier'


kg_edge_properties = [
    LITCOIN.ABSTRACT_ID,
    LITCOIN.ABSTRACT_SPAN,
    LITCOIN.ASSERTION_ID,
    LITCOIN.ASSERTION_SPAN,
    LITCOIN.SUBJECT_NAME,
    LITCOIN.SUBJECT_TYPE,
    LITCOIN.SUBJECT_QUALIFIER,
    LITCOIN.OBJECT_NAME,
    LITCOIN.OBJECT_TYPE,
    LITCOIN.OBJECT_QUALIFIER,
    LITCOIN.RELATIONSHIP,
    LITCOIN.RELATIONSHIP_QUALIFIER
]

required_edge_properties = [
    LITCOIN.SUBJECT_NAME,
    LITCOIN.SUBJECT_TYPE,
    LITCOIN.OBJECT_NAME,
    LITCOIN.OBJECT_TYPE,
    LITCOIN.RELATIONSHIP
]

ABSTRACT_TITLE_EDGE_PROP = 'abstract_title'
ABSTRACT_TEXT_EDGE_PROP = 'abstract_text'
ABSTRACT_JOURNAL_EDGE_PROP = 'journal'


##############
# Class: LitCoin source loader
#
# Desc: Class that loads/parses the LitCoin data.
##############
class LitCoinLoader(SourceDataLoader):

    source_id: str = 'LitCoin'
    provenance_id: str = 'infores:litcoin'
    parsing_version: str = '4.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.shared_source_data_path = os.getenv('SHARED_SOURCE_DATA_PATH', None)
        self.data_url = 'https://stars.renci.org/var/data_services/litcoin/'
        self.version_file = 'litcoin.yaml'
        self.abstracts_file = 'abstracts_CompAndHeal.json'
        self.llm_output_file = 'litcoin_latest.jsonl'
        self.data_files = [self.llm_output_file, self.abstracts_file]

        self.parsing_metadata = None
        self.bagel_results_lookup = None
        self.bl_utils = BiolinkUtils()

        self.mentions_predicate = "IAO:0000142"

    def get_latest_source_version(self) -> str:
        if not self.shared_source_data_path:
            version_file_url = f"{self.data_url}{self.version_file}"
            r = requests.get(version_file_url)
            version_yaml = yaml.full_load(r.text)
        else:
            with open(os.path.join(self.shared_source_data_path, self.version_file), 'r') as f:
                version_yaml = yaml.full_load(f)

        build_version = str(version_yaml['build'])
        return build_version


    def get_data(self) -> bool:
        for data_file in self.data_files:
            if not self.shared_source_data_path:
                source_data_url = f'{self.data_url}{data_file}'
                data_puller = GetData()
                data_puller.pull_via_http(source_data_url, self.data_path)
            else:
                shutil.copy(os.path.join(self.shared_source_data_path, data_file), self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        self.load_bagel_cache()

        abstracts_file_path = os.path.join(self.data_path, self.abstracts_file)
        abstracts = self.load_abstracts(abstracts_file_path)

        self.parsing_metadata = {
            'bagelization_errors': 0,
            'failed_bagelization': 0,
            'failed_abstract_lookup': 0,
        }

        records = 0
        skipped_records = 0
        failed_abstract_lookup = 0
        missing_abstracts = set()
        predicates_that_could_not_be_mapped = set()
        litcoin_file_path: str = os.path.join(self.data_path, self.llm_output_file)

        for litcoin_edge in quick_jsonl_file_iterator(litcoin_file_path):

            records += 1
            if records == 10 and self.test_mode:
                break

            abstract_id = litcoin_edge[LITCOIN.ABSTRACT_ID]
            pubmed_id = f'{PUBMED}:{abstract_id}'
            self.logger.info(f'processing edge {records}, abstract {abstract_id}')

            try:
                abstract_title = abstracts[abstract_id]['title']
                abstract_text = abstracts[abstract_id]['abstract']
                abstract_journal = abstracts[abstract_id]['journal_name']
            except KeyError:
                missing_abstracts.add(abstract_id)
                skipped_records += 1
                failed_abstract_lookup += 1
                self.logger.info(f'Skipping due to failed abstract lookup.')
                continue

            output_edge_properties = self.parse_llm_edge(litcoin_edge, logger=self.logger)
            if output_edge_properties is None:
                skipped_records += 1
                self.logger.info(f'Skipping due to failed edge property extraction.')
                continue

            try:
                subject_node = self.bagelize_entity(entity_name=litcoin_edge[LITCOIN.SUBJECT_NAME],
                                                    abstract_id=abstract_id,
                                                    abstract_text=abstract_text)
                if subject_node is not None:
                    subject_id = subject_node["id"]
                    subject_name = subject_node["name"]
                    subject_bagel_synonym_type = subject_node["synonym_type"]
                else:
                    self.logger.info('Skipping due to failed subject node bagelization.')
                    skipped_records += 1
                    continue

                object_node = self.bagelize_entity(entity_name=litcoin_edge[LITCOIN.OBJECT_NAME],
                                                   abstract_id=abstract_id,
                                                   abstract_text=abstract_text)
                if object_node is not None:
                    object_id = object_node["id"]
                    object_name = object_node["name"]
                    object_bagel_synonym_type = object_node["synonym_type"]
                else:
                    self.logger.info('Skipping due to failed object node bagelization.')
                    skipped_records += 1
                    continue

                predicate_mapping_resp = call_pred_mapping(litcoin_edge[LITCOIN.SUBJECT_NAME],
                                                           litcoin_edge[LITCOIN.OBJECT_NAME],
                                                           litcoin_edge[LITCOIN.RELATIONSHIP],
                                                           abstract_text,
                                                           logger=self.logger)
                if predicate_mapping_resp is None:
                    self.logger.info('Skipping due to failed predicate mapping.')
                    skipped_records += 1
                    continue

                predicate = predicate_mapping_resp[LITCOIN.PREDICATE]
                predicate_negated = predicate_mapping_resp[LITCOIN.PREDICATE_NEGATED]

                self.output_file_writer.write_node(node_id=subject_id,
                                                   node_name=subject_name)
                self.output_file_writer.write_node(node_id=object_id,
                                                   node_name=object_name)

                output_edge_properties.update({
                    PUBLICATIONS: [pubmed_id],
                    BAGEL_SUBJECT_SYN_TYPE: subject_bagel_synonym_type,
                    BAGEL_OBJECT_SYN_TYPE: object_bagel_synonym_type,
                    ABSTRACT_TITLE_EDGE_PROP: abstract_title,
                    ABSTRACT_TEXT_EDGE_PROP: abstract_text
                })

                output_edge_properties[NEGATED] = predicate_negated

                self.output_file_writer.write_edge(subject_id=subject_id,
                                                   object_id=object_id,
                                                   predicate=predicate,
                                                   edge_properties=output_edge_properties)

                # write the node for the publication and edges from the publication to the entities
                self.output_file_writer.write_node(node_id=pubmed_id,
                                                   node_properties={ABSTRACT_TEXT_EDGE_PROP: abstract_text,
                                                                    ABSTRACT_JOURNAL_EDGE_PROP: abstract_journal})
                self.output_file_writer.write_edge(subject_id=pubmed_id,
                                                   object_id=subject_id,
                                                   predicate=self.mentions_predicate)
                self.output_file_writer.write_edge(subject_id=pubmed_id,
                                                   object_id=object_id,
                                                   predicate=self.mentions_predicate)

            except (KeyboardInterrupt, Exception) as e:
                # save results/cache on an error to avoid duplicate llm calls
                self.save_bagel_cache()
                self.save_llm_results()
                # predicate_mapper.save_cached_predicate_mappings()
                raise e

        # save the predicates mapped with openai embeddings
        # predicate_mapper.save_cached_predicate_mappings()

        # save the bagel results
        self.save_bagel_cache()
        self.save_llm_results()

        self.parsing_metadata.update({
            'records': records,
            'skipped_records': skipped_records,
            'failed_abstract_lookup': failed_abstract_lookup,
            'missing_abstracts': list(missing_abstracts),
            'predicates_that_could_not_be_mapped': list(predicates_that_could_not_be_mapped)
        })
        return self.parsing_metadata

    def bagelize_entity(self, entity_name, abstract_id, abstract_text):
        abstract_id = str(abstract_id)
        if abstract_id in self.bagel_results_lookup and entity_name in self.bagel_results_lookup[abstract_id]["terms"]:
            bagel_results = self.bagel_results_lookup[abstract_id]["terms"][entity_name]
        else:
            if abstract_id not in self.bagel_results_lookup:
                self.bagel_results_lookup[abstract_id] = {"abstract": abstract_text,
                                                          "terms": {}}
            try:
                bagel_results = self.get_bagel_results(text=abstract_text,
                                                       entity=entity_name,
                                                       abstract_id=abstract_id)
                self.bagel_results_lookup[abstract_id]["terms"][entity_name] = bagel_results
            except Exception as e:
                error_message = f'Failed Bagelization: {type(e)}:{e}'
                self.logger.error(error_message)
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                    raise e
                self.parsing_metadata['bagelization_errors'] += 1
                bagel_results = {'error': error_message}
                self.bagel_results_lookup[abstract_id]["terms"][entity_name] = bagel_results

        if 'error' in bagel_results:
            return None

        bagel_node = extract_best_match(bagel_results)
        if not bagel_node:
            self.parsing_metadata['failed_bagelization'] += 1
            self.logger.info(f'Skipping due to bagelization finding no match for: {entity_name} in abstract {abstract_id}')
            return None
        return bagel_node

    def parse_llm_edge(self, llm_json_edge, logger):
        converted_edge = {}
        for field in required_edge_properties:
            if field not in llm_json_edge:
                logger.warning(f'Missing field {field} in response: {llm_json_edge}')
                return None
            if not isinstance(llm_json_edge[field], str):
                logger.warning(f'Non-string field {field} in response: {llm_json_edge}')
                return None
        else:
            for prop in kg_edge_properties:
                converted_edge[f'llm_{prop}'] = llm_json_edge.get(prop, None)
            return converted_edge

    @staticmethod
    def load_abstracts(abstracts_file_path):
        abstracts = {}
        with open(abstracts_file_path) as abstract_file:
            abstract_json = json.load(abstract_file)
            for abstract_id, abstract in abstract_json.items():
                pmid = abstract['pmid']
                abstracts[pmid] = abstract
        return abstracts

    """
    This was the previous way to parse outputs before the files were converted to json upstream of ORION,
    these had multiple edges per line (llm_output)
    def parse_llm_output(llm_output, logger):
        required_fields = [SUBJECT_NAME,
                           SUBJECT_TYPE,
                           OBJECT_NAME,
                           OBJECT_TYPE,
                           RELATIONSHIP]

        # this regex is from Miles at CoVar, it should extract json objects representing all the edges with entities
        matches = re.findall(r'\{([^\}]*)\}', llm_output)
        valid_responses = []
        for match in matches:
            cur_response = '{' + match + '}'
            try:
                cur_response_dict = json.loads(cur_response)
            except json.JSONDecodeError as e:
                logger.error(f'Error decoding JSON: {e}')
                continue
            for field in required_edge_properties:
                if field not in cur_response_dict:
                    logger.warning(f'Missing field {field} in response: {cur_response_dict}')
                    break
                if not isinstance(cur_response_dict[field], str):
                    logger.warning(f'Non-string field {field} in response: {cur_response_dict}')
                    break
            else:  # only return edge/node dictionaries which have all the required fields
                valid_responses.append(cur_response_dict)
        return valid_responses
    """

    def get_bagel_results(self, text, entity, abstract_id):
        orion_bagel_results = get_orion_bagel_results(text=text, term=entity, abstract_id=abstract_id)
        return convert_orion_bagel_result_to_bagel_service_format(orion_bagel_results)

    def get_bagel_cache_path(self):
        cache_path = os.path.join(self.data_path, "bagel_cache.json")
        # cache does not exist in source data path, check whether it exists in the shared source data path
        if not os.path.exists(cache_path) and self.shared_source_data_path:
            shared_cache_path = os.path.join(self.shared_source_data_path, "bagel_cache.json")
            if os.path.exists(shared_cache_path):
                shutil.copyfile(shared_cache_path, cache_path)
        return cache_path

    def load_bagel_cache(self):
        bagel_cache_file_path = self.get_bagel_cache_path()
        if os.path.exists(bagel_cache_file_path):
            with open(bagel_cache_file_path, "r") as bagel_cache_file:
                self.bagel_results_lookup = json.load(bagel_cache_file)
        else:
            self.bagel_results_lookup = {}

    def save_bagel_cache(self):
        bagel_cache_file_path = self.get_bagel_cache_path()
        print(self.bagel_results_lookup)
        with open(bagel_cache_file_path, "w") as bagel_cache_file:
            return json.dump(self.bagel_results_lookup,
                             bagel_cache_file,
                             indent=4,
                             sort_keys=True)

    def get_llm_results_path(self):
        return os.path.join(self.data_path, "llm_results.json")

    def save_llm_results(self):
        llm_results_path = self.get_llm_results_path()
        if os.path.exists(llm_results_path):
            with open(llm_results_path, "r") as llm_results_file:
                previous_llm_results = json.load(llm_results_file)
        else:
            previous_llm_results = []

        new_llm_results = get_llm_results()
        results_to_add = []
        for llm_result in new_llm_results:
            already_there = False
            for old_llm_result in previous_llm_results:
                if llm_result == old_llm_result:
                    already_there = True
                    break
            if not already_there:
                results_to_add.append(llm_result)
        with open(llm_results_path, "w") as llm_results_file:
            json.dump(previous_llm_results + results_to_add, llm_results_file, indent=4)

    def name_resolution_function(self, node_name, preferred_biolink_node_type, retries=0):
        return call_name_resolution(node_name,
                                    preferred_biolink_node_type,
                                    retries, logger=self.logger)

    def standardize_name_resolution_results(self, name_res_json):
        if not name_res_json:
            return {}
        elif NAME_RESOLVER_API_ERROR in name_res_json:
            return name_res_json
        return {
            "curie": name_res_json['curie'],
            "name": name_res_json['label'],
            "types": list(self.bl_utils.find_biolink_leaves(frozenset(name_res_json['types']))),
            "score": name_res_json['score']
        }


class LitCoinBagelServiceLoader(LitCoinLoader):

    source_id: str = 'LitCoinBagelService'
    parsing_version: str = '1.0'

    def get_bagel_results(self, text, entity, abstract_id):
        return call_bagel_service(text=text, entity=entity)


"""
This is broken with the new jsonl format
class LitCoinEntityExtractorLoader(LitCoinLoader):
    source_id: str = 'LitCoinEntityExtractor'
    parsing_version: str = '1.4'

    def parse_data(self) -> dict:
        litcoin_file_path: str = os.path.join(self.data_path, self.llm_output_file)
        all_entities = {}
        with open(litcoin_file_path) as litcoin_file:
            litcoin_json = json.load(litcoin_file)
            for litcoin_object in litcoin_json:
                abstract_id = litcoin_object['abstract_id']
                llm_output = litcoin_object['output']
                for litcoin_edge in self.parse_llm_output(llm_output):

                    subject_name = litcoin_edge[LLM_SUBJECT_NAME]
                    subject_type = litcoin_edge[LLM_SUBJECT_TYPE]
                    # NODE_TYPE_MAPPINGS was replaced with Bagel for now
                    # subject_mapped_type = NODE_TYPE_MAPPINGS.get(self.convert_node_type_to_biolink_format(subject_type),
                    #                                             None)
                    all_entities[f'{subject_name}{subject_type}'] = {'name': subject_name,
                                                                     'llm_type': subject_type,
                                                                     # 'name_res_type': subject_mapped_type,
                                                                     'abstract_id': abstract_id}
                    object_name = litcoin_edge[LLM_OBJECT_NAME]
                    object_type = litcoin_edge[LLM_OBJECT_TYPE]
                    # NODE_TYPE_MAPPINGS was replaced with Bagel for now
                    # object_mapped_type = NODE_TYPE_MAPPINGS.get(self.convert_node_type_to_biolink_format(object_type),
                    #                                            None)
                    all_entities[f'{object_name}{object_type}'] = {'name': object_name,
                                                                   'llm_type': object_type,
                                                                   # 'name_res_type': object_mapped_type,
                                                                   'abstract_id': abstract_id}

        with open(os.path.join(self.data_path, "..",
                               f"parsed_{self.parsing_version}",
                               "name_res_inputs.csv"), "w") as name_res_inputs:
            name_res_inputs.write("query,llm_type,biolink_type,abstract_id\n")
            for entity in all_entities.values():
                name_res_inputs.write(f'"{entity["name"]}","{entity["llm_type"]}",{entity["name_res_type"]},{entity["abstract_id"]}\n')
        self.logger.info(f'{len(all_entities.values())} unique entities extracted')
        return {}
"""

