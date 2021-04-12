import os
import argparse
import enum
import pandas as pd
import gzip
import logging
import datetime
import requests

from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from io import TextIOWrapper
from csv import reader
from Common.utils import LoggingUtil, GetData


# the data header columns are:
class DATACOLS(enum.IntEnum):
    DB = 0
    DB_Object_ID = 1
    DB_Object_Symbol = 2
    Qualifier = 3
    GO_ID = 4
    DB_Reference = 5
    Evidence_Code = 6
    With_From = 7
    Aspect = 8
    DB_Object_Name = 9
    DB_Object_Synonym = 10
    DB_Object_Type = 11
    Taxon_Interacting_taxon = 12
    Date = 13
    Assigned_By = 14
    Annotation_Extension = 15
    Gene_Product_Form_ID = 16


##############
# Class: UniProtKB GOA loader
#
# By: Phil Owen
# Date: 7/6/2020
# Desc: Class that loads the UniProtKB GOA data and creates KGX files for importing into a Neo4j graph.
##############
class GOALoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    node_norm_failures: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.data_file = 'goa_human.gaf.gz'
        self.test_mode = test_mode
        self.source_id = 'HumanGOA'
        self.source_db = 'UniProtKB'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.GOA.GOALoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_human_goa_data(self) -> (int):
        """
        Gets the human goa data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the GOA data file
            byte_count: int = gd.get_goa_http_file(self.data_path, self.data_file)
        else:
            byte_count: int = 1

        # return the byte count to the caller
        return byte_count

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_types=node['category'], node_properties=node['properties'])

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        loads/parses human GOA data files

        :param edges_output_file_path:
        :param nodes_output_file_path:
        :return:
        """
        self.logger.info(f'GOALoader - Start of GOA data processing.')

        # init the return
        load_metadata: dict = {}

        # get the human goa data
        byte_count = self.get_human_goa_data()

        # did we get all the files
        if byte_count > 0:
            # parse the data
            load_metadata = self.parse_data_file(os.path.join(self.data_path, self.data_file))

            self.logger.debug(f'File parsing complete.')

            # write the output files
            self.write_to_file(nodes_output_file_path, edges_output_file_path)
        else:
            self.logger.error(f'Error: Retrieving file {self.data_file} failed.')

        # do not remove the file if in debug mode
        if self.logger.level != logging.DEBUG and not self.test_mode:
            # remove the data file
            os.remove(os.path.join(self.data_path, self.data_file))

        self.logger.info(f'GOALoader - Processing complete.')

        # return the metadata results
        return load_metadata

    def parse_data_file(self, infile_path: str) -> dict:
        """
        Parses the data file for nodes/edges

        :param infile_path: the name of the GOA file to process
        :return: parsing meta data results
        """

        with gzip.open(infile_path, 'r') as zf:
            # read the file and make the list
            node_list, load_metadata = self.get_node_list(zf)

            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

            # get the list of unique edges
            self.final_edge_list = self.get_edge_list(df)

            self.logger.debug(f'{len(self.final_edge_list)} unique edges found.')

            # loop through all the nodes
            for n in node_list:
                # turn the string categories into a list
                if isinstance(n['category'], str):
                    n['category'] = n['category'].split('|')

                # add it to the final node list
                self.final_node_list.append(n)

        self.logger.debug(f'GOA data parsing and KGX file creation complete.\n')

        # return to the caller
        return load_metadata

    def get_edge_list(self, df: pd.DataFrame) -> list:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :return: list of KGX ready edges
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # init a list for edge normalizations
        edge_list: list = []

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # init variables for each group
            node_1_id: str = ''
            node_3_id: str = ''
            node_3_type: str = ''
            grp: str = ''

            # if we dont get a pair something is odd (but not necessarily bad)
            if len(rows) != 2:
                self.logger.debug(f'Debug: Mis-matched node grouping. {rows}')

            # for each row in the triplet
            for row in rows.iterrows():
                # save the node ids for the edges
                if row[1].node_num == 1:
                    grp = row[0]
                    node_1_id = row[1]['id']
                elif row[1].node_num == 3:
                    node_3_id = row[1]['id']
                    node_3_type = row[1]['category']

            # write out an edge that connects nodes 1 and 3
            """ An edge between the gene and the go term. If the go term is a molecular_activity, 
            then the edge should be (go term)-[enabled_by]->(gene). If the go term is a biological 
            process then it should be (gene)-[actively_involved_in]->(go term). If it is a cellular 
            component then it should be (go term)-[has_part]->(gene) """

            # init node 1 to node 3 edge details
            relation: str = ''
            src_node_id: str = ''
            obj_node_id: str = ''
            valid_type = True

            # find the predicate and edge relationships
            if node_3_type.find('MolecularActivity') > -1:
                relation = 'RO:0002333'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            elif node_3_type.find('BiologicalProcess') > -1:
                relation = 'RO:0002331'
                src_node_id = node_1_id
                obj_node_id = node_3_id
            elif node_3_type.find('CellularComponent') > -1:
                relation = 'RO:0001019'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            else:
                valid_type = False

            # was this a good value
            if not valid_type:
                self.logger.debug(f'Warning: Unrecognized node 3 type')
            elif src_node_id == '' or obj_node_id == '':
                self.logger.debug(f'Warning: Missing 1 or more node IDs. Node type 1: {node_1_id}, Node type 3: {node_3_id}')
            else:
                # create the KGX edge data for nodes 1 and 3
                edge_list.append({"id": "", "subject": f"{src_node_id}", "relation": f"{relation}", "object": f"{obj_node_id}", "properties": {'source_data_base': 'UniProtKB Human GOA'}})

        # return the list to the caller
        return edge_list

    def get_node_list(self, fp) -> (list, dict):
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :return: list of nodes for further processing
        """

        # create a csv parser
        lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

        # clear out the node list
        node_list: list = []

        # set the default category. could be overwritten in normalization
        default_category: str = 'biolink:Gene|biolink:GenomicEntity|biolink:MolecularEntity|biolink:BiologicalEntity|biolink:NamedThing|biolink:Entity'

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # while there are lines in the csv file
        for line in lines:
            # skip over data comments
            if line[0] == '!' or line[0][0] == '!':
                continue

            # increment the counter
            record_counter += 1

            try:
                # an example record looks like this
                """ UniProtKB       O73942  apeI            GO:0004518      GO_REF:0000043  IEA     UniProtKB-KW:KW-0540    F       
                    Homing endonuclease I-ApeI      apeI|APE_1929.1 protein 272557  20200229        UniProt """

                # create a unique group identifier
                grp: str = f'{line[DATACOLS.DB_Object_ID.value]}/{line[DATACOLS.GO_ID.value]}'

                # create node type 1
                """ A gene with identifier UniProtKB:O73942, and name "apeI", and description "Homing endonuclease I-ApeI". """
                node_list.append({'grp': grp, 'node_num': 1, 'id': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}', 'name': f'{line[DATACOLS.DB_Object_Symbol.value]}', 'category': f'{default_category}', 'properties': None})

                # create node type 3
                """ A node for the GO term GO:0004518. It should normalize, telling us the type/name. """
                node_list.append({'grp': grp, 'node_num': 3, 'id': f'{line[DATACOLS.GO_ID.value]}', 'name': '', 'category': '', 'properties': None})
            except Exception as e:
                # increment the counter
                skipped_record_counter += 1

                self.logger.error(f'Error: Exception: {e}')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # de-dupe the list
        node_list = [dict(t) for t in {tuple(d.items()) for d in node_list}]

        # normalize the group of entries on the data frame.
        node_list = self.normalize_node_data(node_list)

        # return to the caller
        return node_list, load_metadata

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the chemical substance node.
        the data comes in as a grouped data frame and we will normalize the node_2 and node_3 groups.

        :param node_list: data frame with items to normalize
        :return: the data frame passed in with updated node data
        """

        # storage for cached node normalizations
        cached_node_norms: dict = {}

        # loop through the list and only save the NCBI taxa nodes
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify taxa that has not been node normed
        tmp_normalize: set = set()

        # iterate through the data and get the keys to normalize
        while node_idx < node_count:
            # check to see if this one needs normalization data from the website
            if node_list[node_idx]['node_num'] == 3:
                if not node_list[node_idx]['id'] in cached_node_norms:
                    tmp_normalize.add(node_list[node_idx]['id'])

            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 5000

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique nodes will be normalized.')

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

                # get the data
                # resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))
                resp: requests.models.Response = requests.post('https://nodenormalization-sri.renci.org/get_normalized_nodes', json={'curies': data_chunk})

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**cached_node_norms, **rvs}

                    # save the merged list
                    cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.debug(f'response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx = 0

        # for each row in the slice add the new id and name
        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # is this something that has been normalized
            if node_list[node_idx]['node_num'] == 3:
                # save the target data element
                rv = node_list[node_idx]

                # did we find a normalized value
                if cached_node_norms[rv['id']] is not None:
                    if 'type' in cached_node_norms[rv['id']]:
                        node_list[node_idx]['category'] = '|'.join(cached_node_norms[rv['id']]['type'])
                else:
                    self.node_norm_failures.append(rv['id'])

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadGOA.py -p /projects/stars/Data_services/UniProtKB_data -g goa_human.gaf.gz -m json
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the UniProtKB data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    # get a reference to the processor
    goa = GOALoader(False)

    # load the data files and create KGX output
    goa.load(data_dir, data_dir)
