
import os
import json
import orjson
import re
import requests
import urllib

from Common.biolink_utils import BiolinkUtils
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.biolink_constants import PUBLICATIONS
from Common.utils import GetData, snakify
from Common.normalization import call_name_resolution, NAME_RESOLVER_API_ERROR
from Common.prefixes import PUBMED

from parsers.LitCoin.src.bagel import get_bagel_results, extract_best_match
from parsers.LitCoin.src.predicate_mapping import PredicateMapping


LLM_SUBJECT_NAME = 'subject'
LLM_SUBJECT_TYPE = 'subject_type'
LLM_SUBJECT_QUALIFIER = 'subject_qualifier'
LLM_OBJECT_NAME = 'object'
LLM_OBJECT_TYPE = 'object_type'
LLM_OBJECT_QUALIFIER = 'object_qualifier'
LLM_RELATIONSHIP = 'relationship'
LLM_RELATIONSHIP_QUALIFIER = 'statement_qualifier'

LLM_SUBJECT_NAME_EDGE_PROP = 'llm_subject'
LLM_SUBJECT_TYPE_EDGE_PROP = 'llm_subject_type'
LLM_SUBJECT_QUALIFIER_EDGE_PROP = 'llm_subject_qualifier'
LLM_OBJECT_NAME_EDGE_PROP = 'llm_object'
LLM_OBJECT_TYPE_EDGE_PROP = 'llm_object_type'
LLM_OBJECT_QUALIFIER_EDGE_PROP = 'llm_object_qualifier'
LLM_RELATIONSHIP_EDGE_PROP = 'llm_relationship'
LLM_RELATIONSHIP_QUALIFIER_EDGE_PROP = 'llm_relationship_qualifier'

BAGEL_SUBJECT_SYN_TYPE = 'subject_bagel_syn_type'
BAGEL_OBJECT_SYN_TYPE = 'object_bagel_syn_type'

ABSTRACT_TITLE_EDGE_PROP = 'abstract_title'
ABSTRACT_TEXT_EDGE_PROP = 'abstract_text'

"""
NODE_TYPE_MAPPINGS = {
    "Activity": "Activity",
    "AnatomicalStructure": "AnatomicalEntity",
    "AnatomicalFeature": "AnatomicalEntity",
    "Antibody": "ChemicalEntity",
    "Behavior": "Behavior",
    "BiologicalStructure": "AnatomicalEntity",
    "BiologicalPathway": "Pathway",
    "CellType": "Cell",
    "Chemical": "ChemicalEntity",
    "Chemicals": "ChemicalEntity",
    "Condition": "PhenotypicFeature",
    "Device": "Device",
    "Disease": "Disease",
    "DiseaseSymptom": "DiseaseOrPhenotypicFeature",
    "Drug": "Drug",
    "DrugClass": "Drug",
    "Gene": "Gene",
    "LifestyleFactor": "Behavior",
    "Organ": "AnatomicalEntity",
    "OrganSystem": "AnatomicalEntity",
    "OrganismHuman": "Cohort",
    "OrganismHumanEthnicGroup": "PopulationOfIndividualOrganisms",
    "OrganismPart": "AnatomicalEntity",
    "Organization": "Agent",
    "Phenotype": "PhenotypicFeature",
    "Procedure": "Procedure",
    "Protein": "Protein",
    "Proteins": "Protein",
    "StatisticalMethod": "Activity",
    "Symptom": "PhenotypicFeature",
    "Technique": "Procedure",
    "Therapy": "Procedure",
    "Treatment": "Procedure"
}
"""

##############
# Class: LitCoin source loader
#
# Desc: Class that loads/parses the LitCoin data.
##############
class LitCoinLoader(SourceDataLoader):

    source_id: str = 'LitCoin'
    provenance_id: str = 'infores:robokop-kg'  # TODO - change this to a LitCoin infores when it exists
    parsing_version: str = '2.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://stars.renci.org/var/data_services/litcoin/'
        self.llm_output_file = 'rare_disease_abstracts_fixed._gpt4_20240320.json'
        self.biolink_predicate_vectors_file = 'mapped_predicate_vectors.json'
        self.data_files = [self.llm_output_file, self.biolink_predicate_vectors_file]
        # dicts of name to id lookups organized by node type (node_name_to_id_lookup[node_type] = dict of names -> id)
        # self.node_name_to_id_lookup = defaultdict(dict)  <--- replaced with bagel
        self.bagel_results_lookup = None
        self.name_res_stats = []
        self.bl_utils = BiolinkUtils()

        self.mentions_predicate = "IAO:0000142"

    def get_latest_source_version(self) -> str:
        latest_version = 'rare_disease_1'
        return latest_version

    def get_data(self) -> bool:
        for data_file in self.data_files:
            source_data_url = f'{self.data_url}{data_file}'
            data_puller = GetData()
            data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        # could use cached results for faster dev runs with something like this
        # with open(os.path.join(self.data_path, "litcoin_name_res_results.json"), "w") as name_res_results_file:
        #    self.node_name_to_id_lookup = orjson.load(name_res_results_file)

        predicate_vectors_file_path = os.path.join(self.data_path,
                                                   self.biolink_predicate_vectors_file)
        predicate_map_cache_file_path = os.path.join(self.data_path,
                                                     "mapped_predicates.json")
        predicate_mapper = PredicateMapping(predicate_vectors_file_path=predicate_vectors_file_path,
                                            predicate_map_cache_file_path=predicate_map_cache_file_path,
                                            logger=self.logger,
                                            workspace_dir=self.data_path)
        self.load_bagel_cache()

        records = 0
        skipped_records = 0
        failed_bagelization = 0
        bagelization_errors = 0
        failed_predicate_mapping = 0
        number_of_abstracts = 0
        terms_that_could_not_be_bagelized = set()
        predicates_that_could_not_be_mapped = set()
        litcoin_file_path: str = os.path.join(self.data_path, self.llm_output_file)
        with open(litcoin_file_path) as litcoin_file:
            litcoin_json = json.load(litcoin_file)
            for litcoin_object in litcoin_json:
                number_of_abstracts += 1
                if number_of_abstracts > 2 and self.test_mode:
                    break
                abstract_id = litcoin_object['abstract_id']
                self.logger.info(f'processing abstract {number_of_abstracts}: {abstract_id}')
                pubmed_id = f'{PUBMED}:{abstract_id}'
                abstract_title, abstract_text = self.parse_abstract_text(litcoin_object)
                llm_output = litcoin_object['output']
                for litcoin_edge in self.parse_llm_output(llm_output):
                    try:
                        subject_name = litcoin_edge[LLM_SUBJECT_NAME]
                        if subject_name not in self.bagel_results_lookup:
                            try:
                                bagel_results = get_bagel_results(abstract=abstract_text, term=subject_name)
                                self.bagel_results_lookup[subject_name] = bagel_results
                            except Exception as e:
                                self.logger.error(f'Failed Bagelization: {type(e)}:{e}')
                                skipped_records += 1
                                bagelization_errors += 1
                                continue
                        else:
                            bagel_results = self.bagel_results_lookup[subject_name]
                        bagel_subject_node, subject_bagel_synonym_type = extract_best_match(bagel_results)
                        if not bagel_subject_node:
                            skipped_records += 1
                            failed_bagelization += 1
                            terms_that_could_not_be_bagelized.add(subject_name)
                            continue
                        subject_id = bagel_subject_node['curie']
                        subject_name = bagel_subject_node['label']

                        object_name = litcoin_edge[LLM_OBJECT_NAME]
                        if object_name not in self.bagel_results_lookup:
                            try:
                                bagel_results = get_bagel_results(abstract=abstract_text, term=object_name)
                                self.bagel_results_lookup[object_name] = bagel_results
                            except Exception as e:
                                self.logger.error(f'Failed Bagelization: {type(e)}:{e}')
                                skipped_records += 1
                                bagelization_errors += 1
                                continue
                        else:
                            bagel_results = self.bagel_results_lookup[object_name]
                        bagel_object_node, object_bagel_synonym_type = extract_best_match(bagel_results)
                        if not bagel_object_node:
                            skipped_records += 1
                            failed_bagelization += 1
                            terms_that_could_not_be_bagelized.add(object_name)
                            continue
                        object_id = bagel_object_node['curie']
                        object_name = bagel_object_node['label']

                        # predicate = 'biolink:' + snakify(litcoin_edge[LLM_RELATIONSHIP])
                        predicate = predicate_mapper.get_mapped_predicate(litcoin_edge[LLM_RELATIONSHIP])
                        if not predicate:
                            skipped_records += 1
                            failed_predicate_mapping += 1
                            continue

                        self.output_file_writer.write_node(node_id=subject_id,
                                                           node_name=subject_name)
                        self.output_file_writer.write_node(node_id=object_id,
                                                           node_name=object_name)

                        edge_properties = {
                            PUBLICATIONS: [pubmed_id],
                            LLM_SUBJECT_NAME_EDGE_PROP: litcoin_edge[LLM_SUBJECT_NAME],
                            LLM_SUBJECT_TYPE_EDGE_PROP: litcoin_edge[LLM_SUBJECT_TYPE],
                            LLM_SUBJECT_QUALIFIER_EDGE_PROP: litcoin_edge[LLM_SUBJECT_QUALIFIER]
                            if LLM_SUBJECT_QUALIFIER in litcoin_edge else None,
                            BAGEL_SUBJECT_SYN_TYPE: subject_bagel_synonym_type,
                            LLM_OBJECT_NAME_EDGE_PROP: litcoin_edge[LLM_OBJECT_NAME],
                            LLM_OBJECT_TYPE_EDGE_PROP: litcoin_edge[LLM_OBJECT_TYPE],
                            LLM_OBJECT_QUALIFIER_EDGE_PROP: litcoin_edge[LLM_OBJECT_QUALIFIER]
                            if LLM_OBJECT_QUALIFIER in litcoin_edge else None,
                            BAGEL_OBJECT_SYN_TYPE: object_bagel_synonym_type,
                            LLM_RELATIONSHIP_EDGE_PROP: litcoin_edge[LLM_RELATIONSHIP],
                            LLM_RELATIONSHIP_QUALIFIER_EDGE_PROP: litcoin_edge[LLM_RELATIONSHIP_QUALIFIER]
                            if LLM_RELATIONSHIP_QUALIFIER in litcoin_edge else None,
                            ABSTRACT_TITLE_EDGE_PROP: abstract_title,
                            ABSTRACT_TEXT_EDGE_PROP: abstract_text
                        }

                        self.output_file_writer.write_edge(subject_id=subject_id,
                                                           object_id=object_id,
                                                           predicate=predicate,
                                                           edge_properties=edge_properties)

                        # write the node for the publication and edges from the publication to the entities
                        self.output_file_writer.write_node(node_id=pubmed_id,
                                                           node_properties={ABSTRACT_TEXT_EDGE_PROP: abstract_text})
                        self.output_file_writer.write_edge(subject_id=pubmed_id,
                                                           object_id=subject_id,
                                                           predicate=self.mentions_predicate)
                        self.output_file_writer.write_edge(subject_id=pubmed_id,
                                                           object_id=object_id,
                                                           predicate=self.mentions_predicate)
                        records += 1

                    except Exception as e:
                        # save results/cache on an error to avoid duplicate llm calls
                        self.save_bagel_cache()
                        predicate_mapper.save_cached_predicate_mappings()
                        raise e

        # save the predicates mapped with openai embeddings
        predicate_mapper.save_cached_predicate_mappings()

        # save the bagel results
        self.save_bagel_cache()

        # replaced by bagel
        # self.save_nameres_lookup_results()

        parsing_metadata = {
            'records': records,
            'skipped_records': skipped_records,
            'bagelization_errors': bagelization_errors,
            'failed_bagelization': failed_bagelization,
            'terms_that_could_not_be_bagelized': list(terms_that_could_not_be_bagelized),
            'failed_predicate_mapping': failed_predicate_mapping,
            'predicates_that_could_not_be_mapped': list(predicates_that_could_not_be_mapped)
        }
        return parsing_metadata

    def get_bagel_cache_path(self):
        return os.path.join(self.data_path, "bagel_cache.json")

    def load_bagel_cache(self):
        bagel_cache_file_path = self.get_bagel_cache_path()
        if os.path.exists(bagel_cache_file_path):
            with open(bagel_cache_file_path, "r") as bagel_cache_file:
                self.bagel_results_lookup = json.load(bagel_cache_file)
        else:
            self.bagel_results_lookup = {}

    def save_bagel_cache(self):
        bagel_cache_file_path = self.get_bagel_cache_path()
        with open(bagel_cache_file_path, "w") as bagel_cache_file:
            return json.dump(self.bagel_results_lookup,
                             bagel_cache_file,
                             indent=4,
                             sort_keys=True)
    """
    replaced by bagel
    def process_llm_node(self, node_name: str, node_type: str):

        # check if we did name resolution for this name and type already and return it if so
        if node_name in self.node_name_to_id_lookup[node_type]:
            return self.node_name_to_id_lookup[node_type][node_name]

        # otherwise call the name res service and try to find a match
        # the following node_type string formatting conversion is kind of unnecessary now,
        # it was intended to produce valid biolink types given the node_type from the llm,
        # but that doesn't really work well enough to use, now we use the NODE_TYPE_MAPPINGS mappings,
        # but the keys currently use the post-conversion format so this stays for now
        biolink_node_type = self.convert_node_type_to_biolink_format(node_type)
        preferred_biolink_node_type = NODE_TYPE_MAPPINGS.get(biolink_node_type, None)
        self.logger.info(f'calling name res for {node_name} - {preferred_biolink_node_type}')
        name_resolution_results = self.name_resolution_function(node_name, preferred_biolink_node_type)
        standardized_name_res_result = self.standardize_name_resolution_results(name_resolution_results)
        standardized_name_res_result['queried_type'] = preferred_biolink_node_type
        self.node_name_to_id_lookup[node_type][node_name] = standardized_name_res_result
        return standardized_name_res_result

    def convert_node_type_to_biolink_format(self, node_type):
        try:
            biolink_node_type = re.sub("[()/]", "", node_type)  # remove parentheses and forward slash
            biolink_node_type = "".join([node_type_segment[0].upper() + node_type_segment[1:].lower()
                                 for node_type_segment in biolink_node_type.split()])  # force Pascal case
            return f'{biolink_node_type}'
        except TypeError as e:
            self.logger.error(f'Bad node type provided by llm: {node_type}')
            return ""
    """

    @staticmethod
    def parse_abstract_text(llm_response):
        # this attempts to extract the abstract title and text from the llm output,
        # it's not great because the abstract is not inside the json part of the output
        abstract_title = "Abstract title not provided or could not be parsed."
        abstract_text = "Abstract text not provided or could not be parsed."
        prompt_lines = llm_response["prompt"].split("\n")
        for line in prompt_lines:
            if line.startswith("Title: "):
                abstract_title = line[len("Title: "):]
            if line.startswith("Abstract: "):
                abstract_text = line[len("Abstract: "):]
        return abstract_title, abstract_text

    def parse_llm_output(self, llm_output):

        required_fields = [LLM_SUBJECT_NAME,
                           LLM_SUBJECT_TYPE,
                           LLM_OBJECT_NAME,
                           LLM_OBJECT_TYPE,
                           LLM_RELATIONSHIP]

        # this regex is from Miles at CoVar, it should extract json objects representing all the edges with entities
        matches = re.findall(r'\{([^\}]*)\}', llm_output)
        valid_responses = []
        for match in matches:
            cur_response = '{' + match + '}'
            try:
                cur_response_dict = orjson.loads(cur_response)
            except orjson.JSONDecodeError as e:
                self.logger.error(f'Error decoding JSON: {e}')
                continue
            for field in required_fields:
                if field not in cur_response_dict:
                    self.logger.warning(f'Missing field {field} in response: {cur_response_dict}')
                    break
                if not isinstance(cur_response_dict[field], str):
                    self.logger.warning(f'Non-string field {field} in response: {cur_response_dict}')
                    break
            else:  # only return edge/node dictionaries which have all the required fields
                valid_responses.append(cur_response_dict)
        return valid_responses

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
            "types": list(self.bl_utils.find_biolink_leaves(set(name_res_json['types']))),
            "score": name_res_json['score']
        }

    def save_nameres_lookup_results(self):
        # write out name res results / lookup to file
        with open(os.path.join(self.data_path, "..",
                               f"parsed_{self.parsing_version}",
                               "litcoin_name_res_results.json"), "w") as name_res_results_file:
            json.dump(self.node_name_to_id_lookup,
                      name_res_results_file,
                      indent=4,
                      sort_keys=True)


class LitCoinSapBERTLoader(LitCoinLoader):
    source_id: str = 'LitCoinSapBERT'
    parsing_version: str = '1.7'

    def name_resolution_function(self, node_name, preferred_biolink_node_type, retries=0):
        sapbert_url = os.getenv('SAPBERT_URL', 'https://babel-sapbert.apps.renci.org/')
        sapbert_annotate_endpoint = urllib.parse.urljoin(sapbert_url, '/annotate/')
        sapbert_payload = {
          "text": node_name,
          "model_name": "sapbert",
          "count": 1000,
          "args": {"bl_type": preferred_biolink_node_type}
        }
        sapbert_response = requests.post(sapbert_annotate_endpoint, json=sapbert_payload)
        if sapbert_response.status_code == 200:
            sapbert_json = sapbert_response.json()
            # return the first result if there is one
            if sapbert_json:
                return sapbert_json[0]
        else:
            error_message = f'Non-200 Sapbert result {sapbert_response.status_code} for request {sapbert_payload}.'
            if retries < 3:
                self.logger.error(error_message + f' Retrying (attempt {retries + 1})... ')
                return self.name_resolution_function(node_name, preferred_biolink_node_type, retries + 1)
            else:
                self.logger.error(error_message + f' Giving up...')
        # if no results return None
        return None

    def standardize_name_resolution_results(self, name_res_json):
        if not name_res_json:
            return None
        return {
            "curie": name_res_json['curie'],
            "name": name_res_json['name'],
            "types": name_res_json['category'] if isinstance(name_res_json['category'], list) else [name_res_json['category']],
            "score": name_res_json['score']
        }


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



