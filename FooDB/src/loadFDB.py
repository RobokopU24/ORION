import os
import hashlib
import argparse
import pandas as pd
import logging
import json

from io import TextIOWrapper
from csv import reader
from Common.utils import LoggingUtil, GetData, NodeNormUtils, EdgeNormUtils
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.FooDB.FooDBLoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 7/6/2020
# Desc: Class that loads the FooDB data and creates KGX files for importing into a Neo4j graph.
##############
class FDBLoader:
    def __init__(self, log_file_level=logging.INFO):
        """
        constructor
        :param log_file_level - overrides default log level
        """
        # was a new level specified
        if log_file_level != logging.INFO:
            logger.setLevel(log_file_level)

    def load(self, data_file_path, data_file_name: str, out_name: str, output_mode: str = 'json', test_mode: bool = False) -> bool:
        """
        loads/parses FooDB data files

        :param data_file_path: root directory of output data files
        :param data_file_name: the name of the FooDB input data file
        :param out_name: the output name prefix of the KGX files
        :param output_mode: the output mode (tsv or json)
        :param test_mode: flag to indicate test mode
        :return: True
        """
        logger.info(f'FooDBLoader - Start of FooDB data processing.')

        # init the return flag
        ret_val: bool = False

        # and get a reference to the data gatherer
        gd = GetData(logger.level)

        with open(os.path.join(data_file_path, f'{out_name}_node_file.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edge_file.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
            # depending on the output mode, write out the node and edge data headers
            if output_mode == 'json':
                out_node_f.write('{"nodes":[\n')
                out_edge_f.write('{"edges":[\n')
            else:
                out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tsource_database\n')

            # parse the data
            self.parse_data_file(os.path.join(data_file_path, data_file_name), out_node_f, out_edge_f, output_mode)

            # do not remove the file if in debug mode
            if logger.level != logging.DEBUG and not test_mode:
                # remove the data file
                os.remove(os.path.join(data_file_path, data_file_name))

            logger.debug(f'File parsing complete.')

            # set the return flag
            ret_val = True

        logger.info(f'FooDBLoader - Processing complete.')

        # return the pass/fail flag to the caller
        return ret_val

    def parse_data_file(self, infile_path: str, out_node_f, out_edge_f, output_mode: str):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param infile_path: the name of the intact file to process
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :return:
        """

        # int the storage for the nodes
        total_nodes: list = []

        # normalize the group of entries on the data frame.
        nnu = NodeNormUtils(logger.level)

        # normalize the node data
        nnu.normalize_node_data(total_nodes)

        logger.debug('Creating edges.')

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(total_nodes, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

        # get the list of unique edges
        final_edges: set = self.get_edge_set(df, output_mode)

        logger.debug(f'{len(final_edges)} unique edges found, creating KGX edge file.')

        # write out the unique edges
        for item in final_edges:
            # format the output depending on the mode and write it out
            if output_mode == 'json':
                out_edge_f.write(f'{{"id":"{hashlib.md5(item.encode("utf-8")).hexdigest()}"' + item)
            else:
                out_edge_f.write(hashlib.md5(item.encode('utf-8')).hexdigest() + item)

        # init a set for the node de-duplication
        final_node_set: set = set()

        # write out the unique nodes
        for row in total_nodes:
            # format the output depending on the mode
            if output_mode == 'json':
                # turn these into json
                category = json.dumps(row["category"].split('|'))
                identifiers = json.dumps(row["equivalent_identifiers"].split('|'))

                # save the node
                final_node_set.add(f'{{"id":"{row["id"]}", "name":"{row["name"]}", "category":{category}, "equivalent_identifiers":{identifiers}}},\n')
            else:
                # save the node
                final_node_set.add(f"{row['id']}\t{row['name']}\t{row['category']}\t{row['equivalent_identifiers']}\n")

        logger.debug(f'Creating KGX node file with {len(final_node_set)} nodes.')

        # write out the data
        for row in final_node_set:
            out_node_f.write(row)

        # finish off the json if we have to
        if output_mode == 'json':
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

    logger.debug(f'FooDB data parsing and KGX file creation complete.\n')

    @staticmethod
    def get_edge_set(df: pd.DataFrame, output_mode: str) -> set:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :param output_mode: the output mode (tsv or json)
        :return: list of KGX ready edges
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # init a list for edge normalizations
        edge_list: list = []

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
            predicate: str = ''
            relation: str = ''
            src_node_id: str = ''
            obj_node_id: str = ''
            valid_type = True

            # find the predicate and edge relationships
            if node_3_type.find('molecular_activity') > -1:
                predicate = 'RO:0002333'
                relation = 'biolink:enabled_by'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            elif node_3_type.find('biological_process') > -1:
                predicate = 'RO:0002331'
                relation = 'biolink:actively_involved_in'
                src_node_id = node_1_id
                obj_node_id = node_3_id
            elif node_3_type.find('cellular_component') > -1:
                predicate = 'RO:0000051'
                relation = 'biolink:has_part'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            else:
                valid_type = False
                logger.warning(f'Warning: Unrecognized node 3 type for {grp}')

            # was this a good value
            if valid_type:
                edge_list.append({"predicate": f"{predicate}", "subject": f"{src_node_id}", "relation": f"{relation}", "object": f"{obj_node_id}", "edge_label": f"{relation}"})

        # get a reference to the ege normalizer
        en = EdgeNormUtils(logger.level)

        # normalize the edges
        en.normalize_edge_data(edge_list)

        for item in edge_list:
            # depending on the output mode, create the KGX edge data for nodes 1 and 3
            if output_mode == 'json':
                edge_set.add(f', "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "source_database":"FooDB"}},\n')
            else:
                edge_set.add(f'\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["object"]}\tFooDB\n')

            logger.debug(f'{len(edge_set)} unique edges identified.')

        # return the list to the caller
        return edge_set

    @staticmethod
    def get_node_list(fp) -> list:
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :return: list of nodes for further processing
        """

        # create a csv parser
        lines = reader(TextIOWrapper(fp, "utf-8"), delimiter='\t')

        # clear out the node list
        node_list: list = []

        # return the list to the caller
        return node_list


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadFDB.py  -m json
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the FooDB data files')
    ap.add_argument('-g', '--data_file', required=True, help='The name of the FooDB data file')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']
    data_file = args['data_file']
    out_mode = args['out_mode']

    # get a reference to the processor
    fdb = FDBLoader()

    # load the data files and create KGX output
    fdb.load(data_dir, data_file, 'FooDB', out_mode)
