import os
import hashlib
import argparse
import requests
import enum
import pandas as pd
import logging
import json
from datetime import datetime
from io import TextIOBase, TextIOWrapper
from csv import reader
from operator import itemgetter
from zipfile import ZipFile
from Common.utils import LoggingUtil, GetData, DatasetDescription, EdgeNormUtils
from pathlib import Path


# data column enumerators
class DataCols(enum.IntEnum):
    ID_interactor_A = 0
    ID_interactor_B = 1
    Alt_ID_interactor_A = 2
    Alt_ID_interactor_B = 3
    Alias_interactor_A = 4
    Alias_interactor_B = 5
    Interaction_detection_method = 6
    Publication_1st_author = 7
    Publication_Identifier = 8
    Taxid_interactor_A = 9
    Taxid_interactor_B = 10
    Interaction_type = 11
    Source_database = 12
    Interaction_identifier = 13
    Confidence_value = 14
    Expansion_method = 15
    Biological_role_interactor_A = 16
    Biological_role_interactor_B = 17
    Experimental_role_interactor_A = 18
    Experimental_role_interactor_B = 19
    Type_interactor_A = 20
    Type_interactor_B = 21
    Xref_interactor_A = 22
    Xref_interactor_B = 23
    Interaction_Xref = 24
    Annotation_interactor_A = 25
    Annotation_interactor_B = 26
    Interaction_annotation = 27
    Host_organism = 28
    Interaction_parameter = 29
    Creation_date = 30
    Update_date = 31
    Checksum_interactor_A = 32
    Checksum_interactor_B = 33
    Interaction_Checksum = 34
    Negative = 35
    Feature_interactor_A = 36
    Feature_interactor_B = 37
    Stoichiometry_interactor_A = 38
    Stoichiometry_interactor_B = 39
    Identification_method_participant_A = 40
    Identification_method_participant_B = 41


##############
# Class: IntAct virus interaction loader
#
# By: Phil Owen
# Date: 6/12/2020
# Desc: Class that loads the Intact Virus interaction data and creates KGX files for importing into a Neo4j graph.
##############
class IALoader:
    # storage for cached node nad edge normalizations
    cached_node_norms: dict = {}
    cached_edge_norms: dict = {}

    # storage for experiment groups to write to file.
    experiment_grp_list: list = []

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.IntAct.IALoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    def load(self, data_file_path: str, out_name: str, output_mode: str = 'json', test_mode: bool = False):
        """
        Loads/parsers the IntAct data file to produce node/edge KGX files for importation into a graph database.

        :param data_file_path: the directory that will contain the intact data file
        :param out_name: The output file name prefix
        :param output_mode: the output mode (tsv or json)
        :param test_mode: sets the usage of using a test data files
        :return: None
        """
        self.logger.info(f'IALoader - Start of IntAct data processing.')

        # assign the data file name
        data_file_name: str = 'intact.zip'

        # get a reference to the data gathering class
        gd = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not test_mode:
            file_count: int = gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/IntAct/current/psimitab/', [data_file_name], data_file_path)
        else:
            file_count: int = 1

        # get the intact archive
        if file_count == 1:
            self. logger.debug(f'{data_file_name} archive retrieved. Parsing IntAct data.')

            with open(os.path.join(data_file_path, f'{out_name}_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
                # write out the node and edge data headers based on the output mode
                if output_mode == 'json':
                    out_node_f.write('{"nodes":[\n')
                    out_edge_f.write('{"edges":[\n')
                else:
                    out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\ttaxon\n')
                    out_edge_f.write(f'id\tsubject\trelation\tedge_label\tpublications\tdetection_method\tobject\tsource_database\n')

                # parse the data
                self.parse_data_file(data_file_path, data_file_name, out_node_f, out_edge_f, output_mode, test_mode)

            # do not remove the file if in debug mode
            # if logger.level != logging.DEBUG and not test_mode:
            #     # remove the data file
            #     os.remove(os.path.join(data_file_path, data_file_name))

            self.logger.debug(f'File parsing complete.')
        else:
            self.logger.error(f'Error: Retrieving IntAct archive failed.')

        # output the normalization errors
        gd.format_normalization_failures(self.get_name(), self.node_norm_failures, self.edge_norm_failures)

        self.logger.info(f'IALoader - Processing complete.')

    def parse_data_file(self, data_file_path: str, data_file_name: str, out_node_f, out_edge_f, output_mode, test_mode: bool = False):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the IntAct zip file
        :param data_file_name: the name of the intact zip file
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :param test_mode: indicates we are in debug mode
        :return:
        """

        infile_path: str = os.path.join(data_file_path, data_file_name)

        with ZipFile(infile_path) as zf:
            # open the taxon file indexes and the uniref data file
            with zf.open('intact.txt', 'r') as fp:
                # create a csv parser
                lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

                # init the interaction counter
                interaction_counter: int = 0

                # reset the experiment group tracker mechanisms
                first = True
                cur_experiment_name = ''
                experiment_grp: list = []

                # while there are lines in the csv file
                for line in lines:
                    # did we get something usable back
                    if line[DataCols.ID_interactor_A.value].startswith('u') and line[DataCols.ID_interactor_B.value].startswith('u'):
                        # increment the interaction counter
                        interaction_counter += 1

                        # get the publication identifier
                        pub_id: str = self.find_target_val(line[DataCols.Publication_Identifier.value], 'pubmed', True)

                        # prime the experiment group tracker if this is the first time in
                        if first:
                            cur_experiment_name = pub_id
                            first = False

                        # we changed to a new experiment
                        if cur_experiment_name != pub_id:
                            # add the experiment to the running list
                            self.experiment_grp_list.extend(experiment_grp)

                            # clear out the experiment group list for the next one
                            experiment_grp.clear()

                            # save the new experiment group name
                            cur_experiment_name = pub_id

                        # define the experiment group mechanism
                        grp: str = f'{pub_id}|{line[DataCols.ID_interactor_A.value]}|{line[DataCols.ID_interactor_B.value]}'  # |{interactor_id}

                        # get the uniprot A ids, alias and taxon
                        uniprot_a: str = 'UniProtKB:' + self.find_target_val(line[DataCols.ID_interactor_A.value], 'uniprotkb')
                        uniprot_alias_a: str = self.find_target_val(line[DataCols.Alias_interactor_A.value], 'uniprotkb', until='(')
                        taxon_a: str = 'NCBITaxon:' + self.find_target_val(line[DataCols.Taxid_interactor_A.value], 'taxid', only_num=True, until='(')
                        taxon_alias_a: str = taxon_a

                        # get the uniprot B ids, alias and taxon
                        uniprot_b: str = 'UniProtKB:' + self.find_target_val(line[DataCols.ID_interactor_B.value], 'uniprotkb')
                        uniprot_alias_b: str = self.find_target_val(line[DataCols.Alias_interactor_B.value], 'uniprotkb', until='(')
                        taxon_b: str = 'NCBITaxon:' + self.find_target_val(line[DataCols.Taxid_interactor_B.value], 'taxid', only_num=True, until='(')
                        taxon_alias_b: str = taxon_b

                        # get the interaction detection method
                        detection_method: str = self.find_detection_method(line[DataCols.Interaction_detection_method.value])

                        # save the items we need in the experiment interaction
                        interaction_line: dict = {'grp': grp, 'pmid': pub_id, 'detection_method': detection_method,
                                                  'u_a': uniprot_a, 'u_b': uniprot_b,
                                                  'u_alias_a': uniprot_alias_a, 'u_alias_b': uniprot_alias_b,
                                                  'u_category_a': '', 'u_category_b': '',
                                                  'u_equivalent_identifiers_a': '', 'u_equivalent_identifiers_b': '',

                                                  't_a': taxon_a, 't_b': taxon_b,
                                                  't_alias_a': taxon_alias_a, 't_alias_b': taxon_alias_b,
                                                  't_category_a': '', 't_category_b': '',
                                                  't_equivalent_identifiers_a': '', 't_equivalent_identifiers_b': '',
                                                  }

                        # save the data to a list for batch processing
                        experiment_grp.append(interaction_line)

                        # output a status indicator
                        if interaction_counter % 250000 == 0:
                            self.logger.debug(f'Completed {interaction_counter} interactions.')

            # save any remainders
            if len(experiment_grp) > 0:
                self.write_out_data(out_node_f, out_edge_f, output_mode, test_mode)
                self.logger.debug(f'Processing completed. {interaction_counter} interactions processed.')

            # if we are in json output mode finish off the file
            if output_mode == 'json':
                out_node_f.write('\n]}')
                out_edge_f.write('\n]}')

            # get the zip archive information
            data_prov: list = zf.infolist()

            # create the dataset KGX node data
            self.get_dataset_provenance(data_file_path, data_prov, 'intact.txt')

    def write_out_data(self, out_node_f: TextIOBase, out_edge_f: TextIOBase, output_mode: str, test_mode: bool = False):
        """
        writes out the data collected from the IntAct file to KGX node and edge files

        :param out_node_f: the node file
        :param out_edge_f: the edge file
        :param output_mode: the output mode (tsv or json)
        :param test_mode: flag to indicate we are going into test mode
        :return:
        """

        self.logger.debug('write_out_data() start.')

        # node normalize the data if we are not in test mode
        if not test_mode:
            self.normalize_node_data(self.experiment_grp_list)

        # write out the edges
        self.write_edge_data(out_edge_f, self.experiment_grp_list, output_mode)

        # init storage for the nodes
        node_list: list = []

        # loop through the group and extract out the node list
        for item in self.experiment_grp_list:
            # for the 2 node types
            for prefix in ['u_', 't_']:
                # for interactors A and B
                for suffix in ['a', 'b']:
                    # if this is a uniprot gene get the taxon number node property
                    if prefix == 'u_':
                        taxon = 'NCBITaxon:' + item['t_' + suffix].split(':')[1]
                    # else a taxon doesnt get a taxon property
                    else:
                        taxon = ''

                    node_list.append({'id': item[prefix + suffix],
                                      'name': item[prefix + 'alias_' + suffix],
                                      'category': item[prefix + 'category_' + suffix],
                                      'equivalent_identifiers': item[prefix + 'equivalent_identifiers_' + suffix],
                                      'taxon': taxon}
                                     )

        # create a set for the nodes
        final_node_set: set = set()

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(node_list, columns=['id', 'name', 'category', 'equivalent_identifiers', 'taxon'])

        # reshape the data frame and remove all node duplicates.
        df = df.drop_duplicates(keep='first')

        # write out each unique node
        for item in df.iterrows():
            # save the name, same for both kinds of output
            name = item[1]["name"].replace('\\', '')

            # depending on the output mode write out the nodes
            if output_mode == 'json':
                # turn these into json
                category = json.dumps(item[1]['category'].split('|'))
                identifiers = json.dumps(item[1]['equivalent_identifiers'].split('|'))
                name = name.replace('"', '\\"')

                # output the node
                final_node_set.add(f'{{"id":"{item[1]["id"]}", "name":"{name}", "category":{category}, "equivalent_identifiers":{identifiers}, "taxon":"{item[1]["taxon"]}"}}')
            else:
                final_node_set.add(f"{item[1]['id']}\t{name}\t{item[1]['category']}\t{item[1]['equivalent_identifiers']}\t{item[1]['taxon']}")

        # write out the node data
        if output_mode == 'json':
            out_node_f.write(',\n'.join(final_node_set))
        else:
            out_node_f.write('\n'.join(final_node_set))

        self.logger.debug("write_out_data() end.")

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized name, category and equivalent identifiers for the node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param node_list: A list with items to normalize
        :return:
        """

        # node list index counter
        node_idx: int = 0

        # de-dupe the list
        node_list = [dict(t) for t in {tuple(d.items()) for d in node_list}]

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify genes that have not been node normed
        tmp_normalize: set = set()

        # iterate through the data and get the keys to normalize
        while node_idx < node_count:
            # for each interactor pair in the record
            for prefix in ['u_', 't_']:
                for suffix in ['a', 'b']:
                    # check to see if this one needs normalization data from the website
                    if not node_list[node_idx][prefix + suffix] in self.cached_node_norms:
                        tmp_normalize.add(node_list[node_idx][prefix + suffix])

            # go to the next interaction
            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 2500

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block indexes {start_index} to {end_index} of {last_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                self.logger.debug(f'Calling node norm service.')

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                self.logger.debug(f'End calling node norm service.')

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**self.cached_node_norms, **rvs}

                    # save the merged list
                    self.cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.debug(f'response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        self.cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx: int = 0

        # for each row in the slice add the new id and name
        # iterate through all the node groups
        while node_idx < node_count:
            # get the node list item
            rv = node_list[node_idx]

            # loop through the A/B interactors and taxa in the interaction
            for prefix in ['u_', 't_']:
                for suffix in ['a', 'b']:
                    try:
                        # did we find a normalized value
                        if self.cached_node_norms[rv[prefix + suffix]] is not None:
                            cached_val = self.cached_node_norms[rv[prefix + suffix]]

                            # find the name and replace it with label
                            if 'label' in cached_val['id'] and cached_val['id']['label'] != '':
                                node_list[node_idx][prefix + 'alias_' + suffix + ''] = cached_val['id']['label']

                            # get the categories
                            if 'type' in cached_val:
                                node_list[node_idx][prefix + 'category_' + suffix] = '|'.join(cached_val['type'])
                            else:
                                # assign the default category
                                if node_list[node_idx][prefix + suffix].startswith('NCBITaxon'):
                                    default_category: str = 'organism_taxon|ontology_class|named_thing'
                                else:
                                    default_category: str = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'

                                node_list[node_idx][prefix + 'category_' + suffix] = default_category

                            # get the equivalent identifiers
                            if 'equivalent_identifiers' in cached_val and len(cached_val['equivalent_identifiers']) > 0:
                                node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = '|'.join(list((item['identifier']) for item in cached_val['equivalent_identifiers']))
                            else:
                                node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]

                            # find the id and replace it with the normalized value
                            node_list[node_idx][prefix + suffix] = cached_val['id']['identifier']
                        else:
                            # put in the defaults
                            if node_list[node_idx][prefix + 'alias_' + suffix] == '':
                                node_list[node_idx][prefix + 'alias_' + suffix] = node_list[node_idx][prefix + suffix]

                            # assign the default category
                            if node_list[node_idx][prefix + suffix].startswith('NCBITaxon'):
                                default_category: str = 'organism_taxon|ontology_class|named_thing'
                            else:
                                default_category: str = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'

                            node_list[node_idx][prefix + 'category_' + suffix] = default_category
                            node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]

                            # self.logger.error(f'Error: Finding node normalization for {node_list[node_idx][prefix + suffix]} failed.')
                            self.node_norm_failures.append(node_list[node_idx][prefix + suffix])
                    except KeyError:
                        # put in the defaults
                        if node_list[node_idx][prefix + 'alias_' + suffix] == '':
                            node_list[node_idx][prefix + 'alias_' + suffix] = node_list[node_idx][prefix + suffix]

                        # assign the default category
                        if node_list[node_idx][prefix + suffix].startswith('NCBITaxon'):
                            default_category: str = 'organism_taxon|ontology_class|named_thing'
                        else:
                            default_category: str = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'

                        node_list[node_idx][prefix + 'category_' + suffix] = default_category
                        node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]

                        # self.logger.error(f'Error: Finding node normalization for {node_list[node_idx][prefix + suffix]} failed.')
                        self.node_norm_failures.append(node_list[node_idx][prefix + suffix])

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list

    def write_edge_data(self, out_edge_f: TextIOBase, experiment_grp: list, output_mode: str):
        """
        writes edges for the experiment group list passed

        :param out_edge_f: the edge file
        :param experiment_grp: single experiment data
        :param output_mode: the output mode (tsv or json)
        :return: nothing
        """

        self.logger.debug(f'Creating edges for {len(experiment_grp)} experiment groups.')

        # init interaction group detection
        cur_interaction_name: str = ''
        first: bool = True
        node_idx: int = 0

        # sort the list of interactions in the experiment group
        sorted_interactions = sorted(experiment_grp, key=itemgetter('grp'))

        # get the number of records in this sorted experiment group
        node_count = len(sorted_interactions)

        # get a reference to the edge normalizer
        en = EdgeNormUtils(self.logger.level)

        # init a edge list
        edge_list: list = []

        # iterate through node groups and create the edge records.
        while node_idx < node_count:
            # logger.debug(f'Working index: {node_idx}.')

            # if its the first time in prime the pump
            if first:
                # save the interaction name
                cur_interaction_name = sorted_interactions[node_idx]['grp']

                # reset the first record flag
                first = False

            # init the list that will contain the group of similar interactions
            grp_list: list = []

            # init the set of distinct detection methods
            detection_method_set: set = set()

            # for each entry member in the group
            while sorted_interactions[node_idx]['grp'] == cur_interaction_name:
                # add the dict to the group
                grp_list.append(sorted_interactions[node_idx])

                # add it to the list of the interaction methods
                detection_method_set.add(sorted_interactions[node_idx]['detection_method'])

                # now that we have it clear it out so de-duplication works
                sorted_interactions[node_idx]['detection_method'] = ''

                # increment the node counter pairing
                node_idx += 1

                # insure we dont overrun the list
                if node_idx >= node_count:
                    break

            # de-duplicate the list of dicts
            grp_list = [dict(dict_tuple) for dict_tuple in {tuple(dict_in_list.items()) for dict_in_list in grp_list}]

            # init the group index counter
            grp_idx: int = 0

            # now that we have a group create the edges
            # a gene to gene pair that has a "directly interacts with" relationship
            while grp_idx < len(grp_list):
                detection_method: str = "|".join(detection_method_set)

                # add the interacting node edges
                edge_list.append({"predicate": "RO:0002436", "subject": f"{grp_list[grp_idx]['u_a']}", "relation": "biolink:directly_interacts_with", "object": f"{grp_list[grp_idx]['u_b']}", "edge_label": "directly_interacts_with", "publications": f"PMID:{grp_list[grp_idx]['pmid']}", "detection_method": detection_method})

                # for each type
                for suffix in ['a', 'b']:
                    # add the taxa edges
                    edge_list.append({"predicate": "RO:0002162", "subject": f"{grp_list[grp_idx]['u_' + suffix]}", "relation": "biolink:in_taxon", "object": f"{grp_list[grp_idx]['t_' + suffix]}", "edge_label": "in_taxon"})

                # goto the next pair
                grp_idx += 1

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the next interaction name
            cur_interaction_name = sorted_interactions[node_idx]['grp']

        # normalize the edges
        en.normalize_edge_data(edge_list, self.cached_edge_norms)

        # init a set for making the edges unique
        edge_set: set = set()

        # loop through the list and create a set of the edge data
        for item in edge_list:
            # create the record ID
            record_id: str = item["subject"] + item["relation"] + item["edge_label"] + item["object"]

            # if this is for the "directly interacts with" relationship
            if item["predicate"] == 'RO:0002436':
                # depending on the mode write out the uniprot A to uniprot B edge
                if output_mode == 'json':
                    # get the detection methods into a list
                    detection_method: list = item['detection_method'].split('|')

                    edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "publications":"{item["publications"]}", "detection_method":{json.dumps(list(detection_method))}, "source_database":"IntAct"}}')
                else:
                    edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["publications"]}\t{item["detection_method"]}\t{item["object"]}\tIntAct')
            else:
                # depending on the output mode write out the uniprot to NCBI taxon edge
                if output_mode == 'json':
                    edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "source_database":"IntAct"}}')
                else:
                    edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t\t\t{item["object"]}\tIntAct')

        # write out the edge data
        if output_mode == 'json':
            out_edge_f.write(',\n'.join(edge_set))
        else:
            out_edge_f.write('\n'.join(edge_set))

        # clear out the used buffers
        edge_list.clear()
        edge_set.clear()

        self.logger.debug(f'Entry member edges created for {node_idx} node(s).')

    @staticmethod
    def find_detection_method(element: str, until: str = '"') -> str:
        # init the return value
        ret_val: str = ''

        # split the element into an array
        vals: list = element.split(':"')

        # get the pubmed id (aka experiment id)
        found_val: str = vals[1]

        # only return the initial number portion of the value
        for c in found_val:
            # is it the end character
            if c != until:
                ret_val += c
            # else do not continue if the end character is found
            else:
                break

        return ret_val

    @staticmethod
    def find_target_val(element: str, target: str, only_num: bool = False, until: str = '') -> str:
        """
        This method gets the value in an element that has IDs separated by '|' and the name/value
        is delimited with ":"

        :param element: The value to parse
        :param target: the name of the value we want to return
        :param only_num: flag to indicate to return the initial number portion of the value
        :param until: save everything in the value until the character is found
        :return: the found value or an empty string
        """

        # init the return value
        ret_val: str = ''

        # split the element into an array
        vals: list = element.split('|')

        # find the pubmed id
        for val in vals:
            # did we find the target value
            if val.startswith(target):
                # get the pubmed id (aka experiment id)
                found_val: str = val.split(':')[1]

                # are we looking for integers only
                if only_num is True:
                    # only return the initial number portion of the value
                    for c in found_val:
                        # is it numeric
                        if c.isnumeric():
                            ret_val += c
                        # else do not continue if non-numeric is found
                        else:
                            break
                elif until != '':
                    # only return the initial number portion of the value
                    for c in found_val:
                        # is it the character indicating end of capture
                        if c != until:
                            ret_val += c
                        # else do not continue if end character is found
                        else:
                            break

                # return it all
                else:
                    ret_val = found_val

                # no need to continue as it was found
                break

        # trim off any trailing "-" suffixes
        ret_val = ret_val.split('-')[0]

        # return the value to the caller
        return ret_val

    @staticmethod
    def get_dataset_provenance(data_path: str, data_prov: list, file_name: str):
        # get the current time
        now: datetime = datetime.now()

        # init the data version
        data_version: datetime = datetime(1980, 1, 1)

        # loop through the data provenance info
        for item in data_prov:
            # did we find the file name we are using
            if item.filename == file_name:
                # convert the version to a date object
                data_version = datetime(*item.date_time[0:6])

                # no need to continue
                break

        # create the dataset descriptor
        ds: dict = {
            'data_set_name': 'IntAct',
            'data_set_title': 'IntAct',
            'data_set_web_site': 'https://www.ebi.ac.uk/intact/',
            'data_set_download_url': 'ftp.ebi.ac.uk/pub/databases/IntAct/current/psimitab/intact.zip',
            'data_set_version': data_version.strftime("%Y%m%d"),
            'data_set_retrieved_on': now.strftime("%Y/%m/%d %H:%M:%S")}

        # create the data description KGX file
        DatasetDescription.create_description(data_path, ds, 'intact')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load IntAct virus interaction data file and create KGX import files.')

    # command line should be like: python loadIA.py -d E:/Data_services/IntAct_data
    ap.add_argument('-i', '--data_dir', required=True, help='The IntAct data file directory')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # IntAct_data_dir = 'E:/Data_services/IntAct'
    IntAct_data_dir = args['data_dir']
    out_mode = args['out_mode']

    # get a reference to the processor
    # logging.DEBUG
    ia = IALoader()

    # load the data files and create KGX output files
    ia.load(IntAct_data_dir, 'intact', out_mode)
