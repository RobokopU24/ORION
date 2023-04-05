import os
import argparse
import enum
import logging
import re

from io import TextIOWrapper
from csv import reader
from operator import itemgetter
from zipfile import ZipFile
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.prefixes import NCBITAXON, UNIPROTKB
from Common.kgxmodel import kgxnode, kgxedge


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
class IALoader(SourceDataLoader):

    source_id: str = 'IntAct'
    provenance_id: str = 'infores:intact'
    description = "The IntAct Molecular Interaction Database provides open access to molecular interactions data derived from literature curation or direct user submission."
    source_data_url = "https://www.ebi.ac.uk/intact/"
    license = "https://www.ebi.ac.uk/about/terms-of-use/"
    attribution = "http://europepmc.org/article/MED/24234451"
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_file: str = 'intact.zip'
        self.source_db: str = 'IntAct Molecular Interaction Database'

        # storage for experiment groups to write to file.
        self.experiment_grp_list: list = []

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        # get the util object
        gd = GetData(self.logger.level)

        # get the file date
        ret_val: str = gd.get_ftp_file_date('ftp.ebi.ac.uk', '/pub/databases/IntAct/current/psimitab/', self.data_file)

        # return to the caller
        return ret_val

    def get_data(self) -> int:
        """
        Gets the intact data.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            file_count: int = gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/IntAct/current/psimitab/', [self.data_file], self.data_path)
        else:
            file_count: int = 1

        # return the file count to the caller
        return file_count

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the IntAct zip file
        :param data_file_name: the name of the intact zip file
        :return: the parsed meta data results
        """
        # get the path to the data file
        infile_path = os.path.join(self.data_path, self.data_file)

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open the zipped data file
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
                    if line[DataCols.ID_interactor_A.value].startswith('u') and \
                            line[DataCols.ID_interactor_B.value].startswith('u'):

                        # increment the counter
                        record_counter += 1

                        # increment the interaction counter
                        interaction_counter += 1

                        # init the publication id
                        pub_id: str = ''

                        # is there a pubmed id
                        if line[DataCols.Publication_Identifier.value].find('pubmed') >= 0:
                            # get the publication identifier
                            pub_id = self.find_target_val(line[DataCols.Publication_Identifier.value], 'pubmed', only_num=True)

                            # did we get a pubmed id
                            if not pub_id == '':
                                # convert it into a curie
                                pub_id = 'PMID:' + pub_id

                        # is there an IMEX id
                        if pub_id == '' and line[DataCols.Publication_Identifier.value].find('imex') >= 0:
                            # get the imex id
                            pub_id = self.find_target_val(line[DataCols.Publication_Identifier.value], 'imex', trim_hyphen=False)

                            # did we get a imex id
                            if not pub_id == '':
                                # imex ids come back in the form IM-####. convert it into a curie
                                pub_id = pub_id.replace('-', ':')

                        # is there a doi id
                        if pub_id == '' and line[DataCols.Publication_Identifier.value].find('doi') >= 0:
                            # try to find the doi curie
                            pub_id = self.find_target_val(line[DataCols.Publication_Identifier.value], 'doi', regex='^10.\d{4,9}/[-._;()/:A-Z0-9]+$', trim_hyphen=False)

                            # did we get a doi id
                            if not pub_id == '':
                                # convert it to a curie
                                pub_id = 'DOI:' + pub_id

                        # alert the user if no pub id found
                        if pub_id == '':
                            self.logger.debug(f"No publication ID found. Source: {line[DataCols.ID_interactor_A.value]}, Object: {line[DataCols.ID_interactor_B.value]}")

                        # prime the experiment group tracker if this is the first time in
                        if first:
                            cur_experiment_name = pub_id
                            first = False

                        # we changed to a new experiment
                        if cur_experiment_name != pub_id:
                            # add the experiment group to the running list
                            self.experiment_grp_list.extend(experiment_grp)

                            # clear out the experiment group list for the next one
                            experiment_grp.clear()

                            # save the new experiment group name
                            cur_experiment_name = pub_id

                        # define the experiment group mechanism
                        grp: str = f'{pub_id}|{line[DataCols.ID_interactor_A.value]}|{line[DataCols.ID_interactor_B.value]}'  # |{interactor_id}

                        # get the uniprot A ids, alias and taxon
                        uniprot_a: str = f'{UNIPROTKB}:' + self.find_target_val(line[DataCols.ID_interactor_A.value], 'uniprotkb')
                        uniprot_alias_a: str = self.find_target_val(line[DataCols.Alias_interactor_A.value], 'uniprotkb', until='(')
                        taxon_a: str = f'{NCBITAXON}:' + self.find_target_val(line[DataCols.Taxid_interactor_A.value], 'taxid', only_num=True, until='(')
                        taxon_alias_a: str = taxon_a

                        # get the uniprot B ids, alias and taxon
                        uniprot_b: str = f'{UNIPROTKB}:' + self.find_target_val(line[DataCols.ID_interactor_B.value], 'uniprotkb')
                        uniprot_alias_b: str = self.find_target_val(line[DataCols.Alias_interactor_B.value], 'uniprotkb', until='(')
                        taxon_b: str = f'{NCBITAXON}:' + self.find_target_val(line[DataCols.Taxid_interactor_B.value], 'taxid', only_num=True, until='(')
                        taxon_alias_b: str = taxon_b

                        # get the interaction detection method
                        detection_method: str = self.find_detection_method(line[DataCols.Interaction_detection_method.value])

                        # get the default categories
                        default_taxon_category: str = 'biolink:OrganismTaxon|biolink:OntologyClass|biolink:NamedThing"'
                        default_gene_category: str = ''  # 'biolink:Gene|biolink:GeneOrGeneProduct|biolink:MacromolecularMachine|biolink:GenomicEntity|biolink:MolecularEntity|biolink:BiologicalEntity|biolink:NamedThing'

                        # save the items we need in the experiment interaction
                        interaction_line: dict = {'grp': grp, 'pub_id': pub_id, 'detection_method': detection_method,
                                                  'u_a': uniprot_a, 'u_b': uniprot_b,
                                                  'u_alias_a': uniprot_alias_a, 'u_alias_b': uniprot_alias_b,
                                                  'u_category_a': default_gene_category, 'u_category_b': default_gene_category,
                                                  'u_equivalent_identifiers_a': '', 'u_equivalent_identifiers_b': '',

                                                  't_a': taxon_a, 't_b': taxon_b,
                                                  't_alias_a': taxon_alias_a, 't_alias_b': taxon_alias_b,
                                                  't_category_a': default_taxon_category, 't_category_b': default_taxon_category,
                                                  't_equivalent_identifiers_a': '', 't_equivalent_identifiers_b': '',
                                                  }

                        # save the data to a list for batch processing
                        experiment_grp.append(interaction_line)

                        # output a status indicator
                        if interaction_counter % 250000 == 0:
                            self.logger.debug(f'Completed {interaction_counter} interactions.')
                    else:
                        # increment the counter
                        skipped_record_counter += 1

        # transform nodes and edges into kgx model lists
        self.get_node_list()
        self.get_edge_list()

        self.logger.debug(f'Processing completed. {interaction_counter} total interactions processed.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

    def get_node_list(self):
        """
        writes out the data collected from the IntAct file to KGX node and edge files

        :return:
        """

        self.logger.debug('get_node_list() start.')

        # loop through the group and extract out the node list
        for item in self.experiment_grp_list:
            # for the 2 node types
            for prefix in ['u_', 't_']:
                # for interactors A and B
                for suffix in ['a', 'b']:
                    # if this is a uniprot gene get the taxon number node property
                    if prefix == 'u_':
                        properties = {'taxon': NCBITAXON + item['t_' + suffix].split(':')[1]}
                    # else a taxon doesnt get a taxon property
                    else:
                        properties = None

                    new_node = kgxnode(item[prefix + suffix],
                                       name=item[prefix + 'alias_' + suffix],
                                       nodeprops=properties)
                    self.final_node_list.append(new_node)

        self.logger.debug("get_node_list() end.")

    def get_edge_list(self):
        """
        gets edges for the experiment group list passed

        :return: nothing
        """

        self.logger.debug(f'Creating edges for {len(self.experiment_grp_list)} experiment groups.')

        # init interaction group detection
        cur_interaction_name: str = ''
        first: bool = True
        node_idx: int = 0

        # sort the list of interactions in the experiment group
        sorted_interactions = sorted(self.experiment_grp_list, key=itemgetter('grp'))

        # get the number of records in this sorted experiment group
        node_count = len(sorted_interactions)

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

                # alert on missing publication id
                if grp_list[grp_idx]['pub_id'] == '':
                    self.logger.debug(f"Publication ID missing for edge. Source: {grp_list[grp_idx]['u_a']}, Object: {grp_list[grp_idx]['u_b']}")

                # add the interacting node edges
                subject_id = f"{grp_list[grp_idx]['u_a']}"
                object_id = f"{grp_list[grp_idx]['u_b']}"
                edge_props = {"publications": f"{grp_list[grp_idx]['pub_id']}", "detection_method": detection_method}
                new_edge = kgxedge(subject_id,
                                   object_id,
                                   predicate="RO:0002436",
                                   primary_knowledge_source=self.provenance_id,
                                   edgeprops=edge_props)
                self.final_edge_list.append(new_edge)

                # for each type
                for suffix in ['a', 'b']:
                    # add the taxa edges
                    subject_id = f"{grp_list[grp_idx]['u_' + suffix]}"
                    object_id = f"{grp_list[grp_idx]['t_' + suffix]}"
                    new_edge = kgxedge(subject_id,
                                       object_id,
                                       predicate="RO:0002162",
                                       primary_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)

                # goto the next pair
                grp_idx += 1

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the next interaction name
            cur_interaction_name = sorted_interactions[node_idx]['grp']

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

    # @staticmethod
    def find_target_val(self, element: str, target: str, only_num: bool = False, until: str = '', regex: str = '', trim_hyphen=True) -> str:
        """
        This method gets the value in an element that has IDs separated by '|' and the name/value
        is delimited with ":"

        :param element: The value to parse
        :param target: the name of the value we want to return
        :param only_num: flag to indicate to return the initial number portion of the value
        :param until: save everything in the value until the character is found
        :param regex: use this regular expression to validate the value
        :param trim_hyphen: do not split the return on a hyphen
        :return: the found value or an empty string
        """

        # init the return value
        ret_val: str = ''

        # split the element into an array
        vals: list = element.split('|')

        # find the target column
        for val in vals:
            # did we find the target value
            if val.startswith(target):
                # get the value (aka experiment id)
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
                            # use a regex if passed in
                elif regex != '':
                    # grab everything after the prefix
                    found_val = val[val.find(':') + 1:]

                    # remove any invalid characters
                    found_val = found_val.replace('"', '')

                    # try to get a match
                    match = re.match(regex, found_val)

                    # if a match was found
                    if match:
                        ret_val = match.string
                    else:
                        self.logger.error(f'regex failure: value: {val}')
                # keep all characters until the stop value is hit
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

        # are we to retain the hyphen or not
        if trim_hyphen:
            # split the string on the hyphen and take the first value
            ret_val = ret_val.split('-')[0]

        # return the value to the caller
        return ret_val


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load IntAct virus interaction data file and create KGX import files.')

    # command line should be like: python loadIA.py -d E:/Data_services/IntAct_data
    ap.add_argument('-i', '--data_dir', required=True, help='The IntAct data file directory')

    # parse the arguments
    args = vars(ap.parse_args())

    # IntAct_data_dir = 'E:/Data_services/IntAct'
    IntAct_data_dir = args['data_dir']
    out_mode = args['out_mode']

    # get a reference to the processor
    # logging.DEBUG
    ia = IALoader(False)

    # load the data files and create KGX output files
    ia.load(IntAct_data_dir, IntAct_data_dir)
