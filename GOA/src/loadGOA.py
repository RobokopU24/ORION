import os
import hashlib
import argparse
import enum
import pandas as pd
import gzip
import logging
from io import TextIOWrapper
from csv import reader
from Common.utils import LoggingUtil, GetData, NodeNormUtils
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.GOA.GOALoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


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
class GOALoader:
    def load(self, data_file_path, data_file_name: str, out_name: str, test_mode: bool = False) -> bool:
        """
        loads/parses goa data file from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/<ftp_dir_path>

        :param data_file_path: root directory of output data files
        :param data_file_name: the name of the goa input data file
        :param out_name: the output name prefix of the KGX files
        :param test_mode: flag to indicate test mode
        :return: True
        """
        logger.info(f'GOALoader - Start of GOA data processing.')

        # init the return flag
        ret_val: bool = False

        # and get a reference to the data gatherer
        gd = GetData()

        # do the real thing if we arent in debug mode
        if not test_mode:
            # get the uniprot kb ids that were curated by swiss-prot
            swiss_prots: set = gd.get_swiss_prot_id_set(data_file_path)

            # get the GOA data file
            byte_count: int = gd.get_goa_http_file(data_file_path, data_file_name)
        else:
            swiss_prots: set = {'A0A024RBG1'}
            byte_count: int = 1

        # did we get all the files and swiss prots
        if byte_count > 0 and len(swiss_prots) > 0:
            with open(os.path.join(data_file_path, f'{out_name}_node_file.tsv'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edge_file.tsv'), 'w', encoding="utf-8") as out_edge_f:
                # write out the node and edge data headers
                out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tsource_database\n')

                # parse the data
                self.parse_data_file(os.path.join(data_file_path, data_file_name), out_node_f, out_edge_f, swiss_prots)

                # do not remove the file if in debug mode
                if logger.level != logging.DEBUG and not test_mode:
                    # remove the data file
                    os.remove(os.path.join(data_file_path, data_file_name))

                logger.debug(f'File parsing complete.')

                # set the return flag
                ret_val = True
        else:
            logger.error(f'Error: Retrieving file {data_file_name} failed.')

        logger.info(f'GOALoader - Processing complete.')

        # return the pass/fail flag to the caller
        return ret_val

    def parse_data_file(self, infile_path: str, out_node_f, out_edge_f, swiss_prots: set):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param infile_path: the name of the intact file to process
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param swiss_prots: the list of uniprot ids that have been swiss curated
        :return:
        """

        with gzip.open(infile_path, 'r') as zf:
            # read the file and make the list
            node_list: list = self.get_node_list(zf, swiss_prots)

            logger.debug(f'Node list loaded with {len(node_list)} entries.')

            # de-dupe the list
            total_nodes = [dict(t) for t in {tuple(d.items()) for d in node_list}]

            logger.debug(f'Node list duplicates removed, now loaded with {len(total_nodes)} entries.')

            # normalize the group of entries on the data frame.
            nnu = NodeNormUtils()

            # normalize the node data
            nnu.normalize_node_data(total_nodes)

            logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(total_nodes, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

            # get the list of unique edges
            final_edges: set = self.get_edge_set(df)

            logger.debug(f'{len(final_edges)} unique edges found, creating KGX edge file.')

            # write out the unique edges
            for item in final_edges:
                out_edge_f.write(hashlib.md5(item.encode('utf-8')).hexdigest() + item)

            logger.debug(f'De-duplicating {len(total_nodes)} nodes')

            # init a set for the node de-duplication
            final_node_set: set = set()

            # write out the unique nodes
            for row in total_nodes:
                final_node_set.add(f"{row['id']}\t{row['name']}\t{row['category']}\t{row['equivalent_identifiers']}\n")

            logger.debug(f'Creating KGX node file with {len(final_node_set)} nodes.')

            for row in final_node_set:
                out_node_f.write(row)

        logger.debug(f'GOA data parsing and KGX file creation complete.\n')

        return True

    @staticmethod
    def get_edge_set(df: pd.DataFrame) -> set:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :return: list of KGX ready edges
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # init a set for the edges
        edge_set: set = set()

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # init variables for each group
            node_1_id: str = ''
            node_3_id: str = ''
            node_3_type: str = ''
            grp: str = ''

            # if we dont get a pair something is odd (but not necessarily bad)
            if len(rows) != 2:
                logger.warning(f'Warning: Mis-matched node grouping. {rows}')

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
            if node_3_type.find('molecular_activity') > -1:
                relation = 'biolink:enabled_by'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            elif node_3_type.find('biological_process') > -1:
                relation = 'biolink:actively_involved_in'
                src_node_id = node_1_id
                obj_node_id = node_3_id
            elif node_3_type.find('cellular_component') > -1:
                relation = 'biolink:has_part'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            else:
                valid_type = False
                logger.warning(f'Warning: Unrecognized node 3 type for {grp}')

            # was this a good value
            if valid_type:
                # create the KGX edge data for nodes 1 and 3
                edge_set.add(f'\t{src_node_id}\t{relation}\t{relation}\t{obj_node_id}\tGOA_EBI-Human\n')

        logger.debug(f'{len(edge_set)} unique edges identified.')

        # return the list to the caller
        return edge_set

    @staticmethod
    def get_node_list(fp, swiss_prots: set) -> list:
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :param swiss_prots: list of swiss-prot curated uniport id entries
        :return: list of nodes for further processing
        """

        # create a csv parser
        lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

        # clear out the node list
        node_list: list = []

        # while there are lines in the csv file
        for line in lines:
            # skip over data comments
            if line[0] == '!' or line[0][0] == '!':
                continue

            # is this a swiss-port curated entry
            if line[1] in swiss_prots:
                try:
                    # an example record looks like this
                    """ UniProtKB       O73942  apeI            GO:0004518      GO_REF:0000043  IEA     UniProtKB-KW:KW-0540    F       
                        Homing endonuclease I-ApeI      apeI|APE_1929.1 protein 272557  20200229        UniProt """

                    # create a unique group identifier
                    grp: str = f'{line[DATACOLS.DB_Object_ID.value]}{line[DATACOLS.GO_ID.value]}{line[DATACOLS.Taxon_Interacting_taxon.value]}'

                    # create node type 1
                    """ A gene with identifier UniProtKB:O73942, and name "apeI", and description "Homing endonuclease I-ApeI". """
                    node_list.append({'grp': grp, 'node_num': 1, 'id': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}', 'name': f'{line[DATACOLS.DB_Object_Symbol.value]}', 'category': 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing', 'equivalent_identifiers': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}'})

                    # create node type 3
                    """ A node for the GO term GO:0004518. It should normalize, telling us the type/name. """
                    node_list.append({'grp': grp, 'node_num': 3, 'id': f'{line[DATACOLS.GO_ID.value]}', 'name': '', 'category': '', 'equivalent_identifiers': ''})
                except Exception as e:
                    logger.error(f'Error: Exception: {e}')

        # return the list to the caller
        return node_list


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadGOA.py -d /projects/stars/Data_services/UniProtKB_data
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the UniProtKB data files')
    ap.add_argument('-g', '--data_file', required=True, help='The name of the GOA data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # UniProtKB_data_dir = '/projects/stars/Data_services/UniProtKB_data'
    # UniProtKB_data_dir = 'E:/Data_services/UniProtKB_data'
    data_dir = args['data_dir']
    data_file = args['data_file']  # goa_human.gaf.gz

    # get a reference to the processor
    goa = GOALoader()

    # load the data files and create KGX output
    goa.load(data_dir, data_file, 'Human_GOA')
