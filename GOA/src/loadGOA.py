import os
import argparse
import enum
import pandas as pd
import gzip
import logging
import datetime

from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from io import TextIOWrapper
from csv import reader
from Common.utils import LoggingUtil, GetData, NodeNormUtils


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

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
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

    def get_human_goa_data(self) -> (int, set):
        """
        Gets the human goa data.

        """
        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the uniprot kb ids that were curated by swiss-prot
            swiss_prots: set = gd.get_swiss_prot_id_set(self.data_path, self.test_mode)

            # get the GOA data file
            byte_count: int = gd.get_goa_http_file(self.data_path, self.data_file)
        else:
            swiss_prots: set = {'A0A024RBG1'}
            byte_count: int = 1

        # return the byte count and swiss prots to the caller
        return byte_count, swiss_prots

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

        # get the human goa data
        byte_count, swiss_prots = self.get_human_goa_data()

        # did we get all the files and swiss prots
        if byte_count > 0 and len(swiss_prots) > 0:
            # parse the data
            self.parse_data_file(os.path.join(self.data_path, self.data_file), swiss_prots)

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

    def parse_data_file(self, infile_path: str, swiss_prots: set):
        """
        Parses the data file for nodes/edges

        :param infile_path: the name of the GOA file to process
        :param swiss_prots: the list of uniprot ids that have been swiss curated
        :return:
        """

        with gzip.open(infile_path, 'r') as zf:
            # read the file and make the list
            node_list: list = self.get_node_list(zf, swiss_prots)

            # de-dupe the list
            node_list = [dict(t) for t in {tuple(d.items()) for d in node_list}]

            # normalize the group of entries on the data frame.
            nnu = NodeNormUtils(self.logger.level)

            # normalize the node data
            nnu.normalize_node_data(node_list)

            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

            # get the list of unique edges
            self.final_edge_list = self.get_edge_list(df)

            # loop through all the nodes
            for n in node_list:
                # turn the string categories into a list
                if isinstance(n['category'], str):
                    n['category'] = n['category'].split('|')

                # add it to the final node list
                self.final_node_list.append(n)

            self.logger.debug(f'{len(self.final_edge_list)} unique edges found.')

        self.logger.debug(f'GOA data parsing and KGX file creation complete.\n')

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
                self.logger.debug(f'Debug: Unrecognized node 3 type for {grp}')

            # was this a good value
            if valid_type:
                edge_list.append({"id": "", "subject": f"{src_node_id}", "relation": f"{relation}", "object": f"{obj_node_id}", "properties": {'source_data_base': 'UniProtKB Human GOA'}})

        # return the list to the caller
        return edge_list

    def get_node_list(self, fp, swiss_prots: set) -> list:
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :param swiss_prots: list of swiss-prot curated uniprot id entries
        :return: list of nodes for further processing
        """

        # create a csv parser
        lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

        # clear out the node list
        node_list: list = []

        # set the default category. could be overwritten in normalization
        default_category: str = 'biolink:Gene|biolink:GeneOrGeneProduct|biolink:MacromolecularMachine|biolink:GenomicEntity|biolink:MolecularEntity|biolink:BiologicalEntity|biolink:NamedThing'

        # while there are lines in the csv file
        for line in lines:
            # skip over data comments
            if line[0] == '!' or line[0][0] == '!':
                continue

            # is this a swiss-port curated entry
            if line[1] in swiss_prots:
                # set the default identifier. could be overwritten in normalization
                # default_identifier: str = f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}'

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
                    self.logger.error(f'Error: Exception: {e}')

        # return the list to the caller
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
