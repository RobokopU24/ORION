import os
import hashlib
import argparse
import requests
import enum
import pandas as pd
from io import TextIOBase, TextIOWrapper
from csv import reader
from datetime import datetime
from operator import itemgetter
from zipfile import ZipFile
from ftplib import FTP


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

    # storage for cached node normalizations
    cached_node_norms: dict = {}

    # storage for experiment groups to write to file.
    experiment_grp_list: list = []

    @staticmethod
    def print_debug_msg(msg: str):
        """
        Adds a timestamp to a print message

        :param msg: the message that gets appended onto a timestamp and output to console
        :return: None
        """

        # get the timestamp
        now: datetime = datetime.now()

        # output the text
        print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')

    def load(self, data_path: str, out_name: str, data_file_name: str):
        """
        Loads/parsers the IntAct data file to produce node/edge KGX files for importation into a graph database.

        :param data_path: the directory that contains the data file
        :param out_name: The output file name prefix
        :param data_file_name: The name of the data archive file
        :return: None
        """
        self.print_debug_msg(f'Start of IntAct data processing. Getting data archive.')

        # get the Intact zip file
        if self.pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/IntAct/current/psimitab/', data_path, data_file_name):
            self.print_debug_msg(f'{data_file_name} archive retrieved. Parsing IntAct data.')

            with open(os.path.join(data_path, f'{out_name}_node_file.csv'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_path, f'{out_name}_edge_file.csv'), 'w', encoding="utf-8") as out_edge_f:
                # write out the node and edge data headers
                out_node_f.write(f'id,name,category,equivalent_identifiers,taxon\n')
                out_edge_f.write(f'id,subject,relation_label,edge_label,publications,detection_method,object\n')

                # parse the data
                self.parse_data_file(os.path.join(data_path, data_file_name), out_node_f, out_edge_f)
            self.print_debug_msg(f'File parsing complete.')
        else:
            self.print_debug_msg(f'Error getting the IntAct archive. Exiting.')

    def parse_data_file(self, infile_path: str, out_node_f, out_edge_f):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX csv files.

        :param infile_path: the name of the intact file to process
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :return:
        """
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

                            # did we reach the write threshold
                            if len(self.experiment_grp_list) > 1000:
                                # write out what we have so far
                                self.write_out_data(out_edge_f, out_node_f)

                                # empty the list for the next batch
                                self.experiment_grp_list.clear()

                            # clear out the experiment group list for the next batch
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

                        # self.print_debug_msg(f'Keeping: {self.find_target_val(line[DATACOLS.Publication_Identifier.value], "pubmed", True)}, {line[0]}, {line[1]}')
                        # output a status indicator
                        if interaction_counter % 10000 == 0:
                            self.print_debug_msg(f'Completed {interaction_counter} interactions.')

                # save any remainders
                if len(experiment_grp) > 0:
                    self.write_out_data(out_edge_f, out_node_f)
                    self.print_debug_msg(f'Processing completed. {interaction_counter} interactions processed.')

    def write_out_data(self, out_edge_f: TextIOBase, out_node_f: TextIOBase):
        """
        writes out the data collected from the IntAct file to KGX node and edge files

        :param out_edge_f: the edge file
        :param out_node_f: the node file
        :return:
        """

        # node normalize the data
        self.experiment_grp_list = self.normalize_node_data(self.experiment_grp_list)

        # write out the edges
        self.write_edge_data(out_edge_f, self.experiment_grp_list)

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
                        taxon = item['t_' + suffix].split(':')[1]
                    # else a taxon doesnt get a taxon property
                    else:
                        taxon = ''

                    node_list.append({'id': item[prefix + suffix],
                                      'name': item[prefix + 'alias_' + suffix].replace('"', ''),
                                      'category': item[prefix + 'category_' + suffix],
                                      'equivalent_identifiers': item[prefix + 'equivalent_identifiers_' + suffix],
                                      'taxon': taxon}
                                     )

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(node_list, columns=['id', 'name', 'category', 'equivalent_identifiers', 'taxon'])

        # reshape the data frame and remove all node duplicates.
        df = df.drop_duplicates(keep='first')

        # write out each unique node
        for item in df.iterrows():
            # write out the node pair
            out_node_f.write(f"{item[1]['id']},\"{item[1]['name']}\",{item[1]['category']},{item[1]['equivalent_identifiers']},{item[1]['taxon']}\n")

        # write out the file buffer
        out_node_f.flush()

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized name, category and equivalent identifiers for the node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param node_list: A list with items to normalize
        :return:
        """

        # node list index counter
        node_idx: int = 0

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
        chunk_size: int = 1000

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

                # self.print_debug_msg(f'Working block indexes {start_index} to {end_index} of {last_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

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
                    # self.print_debug_msg(f'response code: {resp.status_code}')

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

            # loop through the A/B interactors and taxons in the interaction
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
                                node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'

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

                            node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'
                            node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]
                    except KeyError:
                        # put in the defaults
                        if node_list[node_idx][prefix + 'alias_' + suffix] == '':
                            node_list[node_idx][prefix + 'alias_' + suffix] = node_list[node_idx][prefix + suffix]

                        node_list[node_idx][prefix + 'category_' + suffix] = 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing'
                        node_list[node_idx][prefix + 'equivalent_identifiers_' + suffix] = node_list[node_idx][prefix + suffix]
                        self.print_debug_msg(f"Error finding node normalization {node_list[node_idx][prefix + suffix]}")

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list

    @staticmethod
    def write_edge_data(out_edge_f, experiment_grp):
        """
        writes edges for the experiment group list passed

        :param out_edge_f: the edge file
        :param experiment_grp: single experiment data
        :return: nothing
        """

        # self.print_debug_msg(f'Creating edges for {len(node_list)} nodes.')

        # init interaction group detection
        cur_interaction_name: str = ''
        first: bool = True
        node_idx: int = 0

        # sort the list of interactions in the experiment group
        sorted_interactions = sorted(experiment_grp, key=itemgetter('grp'))

        # get the number of records in this sorted experiment group
        node_count = len(sorted_interactions)

        # iterate through node groups and create the edge records.
        while node_idx < node_count:
            # self.print_debug_msg(f'Working index: {node_idx}')

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
                # write out the uniprot A to uniprot B edge
                edge = f',{grp_list[grp_idx]["u_a"]},directly_interacts_with,directly_interacts_with,PMID:{grp_list[grp_idx]["pmid"]},{"|".join(detection_method_set)},{grp_list[grp_idx]["u_b"]}\n'
                out_edge_f.write(hashlib.md5(edge.encode('utf-8')).hexdigest() + edge)

                # write out the uniprot to NCBI taxon edge
                for suffix in ['a', 'b']:
                    edge = f',{grp_list[grp_idx]["u_" + suffix]},in_taxon,in_taxon,,,{grp_list[grp_idx]["t_" + suffix]}\n'
                    out_edge_f.write(hashlib.md5(edge.encode('utf-8')).hexdigest() + edge)

                # goto the next pair
                grp_idx += 1

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the next interaction name
            cur_interaction_name = sorted_interactions[node_idx]['grp']

        # self.print_debug_msg(f'   {node_idx} Entry member edges created.')

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

    def pull_via_ftp(self, ftp_site: str, ftp_dir: str, file_data_path: str, file: str) -> bool:
        """
        gets the requested files from UniProtKB ftp directory

        :param ftp_site: url of the ftp site
        :param ftp_dir: the directory in the site
        :param file_data_path: the destination of the captured file
        :param file: the name of the file to capture
        :return: None
        """

        # init the return value
        ret_val: bool = False

        try:
            # open the FTP connection and go to the directory
            ftp: FTP = FTP(ftp_site)
            ftp.login()
            ftp.cwd(ftp_dir)

            # does the file exist and has data in it
            try:
                size: int = os.path.getsize(os.path.join(file_data_path, file))
            except FileNotFoundError:
                size: int = 0

            # if we have a size we done need to get the file
            if size == 0:
                # open the file
                with open(os.path.join(file_data_path, file), 'wb') as fp:
                    # get the file data into a file
                    ftp.retrbinary(f'RETR {file}', fp.write)
            else:
                self.print_debug_msg(f'Archive retrieval complete.')

            # close the ftp object
            ftp.quit()

            # set the return value
            ret_val = True
        except Exception as e:
            print(f'Pull_via_ftp() failed. Exception: {e}')

        # return pass/fail to the caller
        return ret_val


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load IntAct virus interaction data file and create KGX import files.')

    # command line should be like: python loadIA.py -d /projects/stars/IntAct/virus
    ap.add_argument('-d', '--data_dir', required=True, help='The IntAct data file directory')

    # parse the arguments
    args = vars(ap.parse_args())

    # IntAct_data_dir = ''
    # IntAct_data_dir = ''
    # IntAct_data_dir = 'D:\Work\Robokop\IntAct\Virus'
    IntAct_data_dir = args['data_dir']

    # get a reference to the processor
    ia = IALoader()

    # load the data files and create KGX output files
    ia.load(IntAct_data_dir, 'intact', 'intact.zip')
