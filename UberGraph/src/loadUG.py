import os
import hashlib
import argparse
import pandas as pd
import logging
import json
from datetime import datetime
from io import TextIOBase
from csv import reader
from operator import itemgetter
from Common.utils import LoggingUtil, NodeNormUtils, DatasetDescription, EdgeNormUtils, GetData
from pathlib import Path

# create a logger
logger = LoggingUtil.init_logging("Data_services.UberGraph.UGLoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


##############
# Class: UberGraph data loader
#
# By: Phil Owen
# Date: 6/12/2020
# Desc: Class that loads the UberGraph data and creates KGX files for importing into a Neo4j graph.
##############
class UGLoader:
    # storage for cached node and edge normalizations
    cached_node_norms: dict = {}
    cached_edge_norms: dict = {}

    # for tracking counts
    total_nodes: int = 0
    total_edges: int = 0

    def __init__(self, log_file_level=logging.INFO):
        """
        constructor
        :param log_file_level - overrides default log level
        """
        # was a new level specified
        if log_file_level != logging.INFO:
            logger.setLevel(log_file_level)

    # init the node and edge data arrays
    def load(self, data_file_path: str, data_file_names: str, output_mode: str = 'json', block_size: int = 5000, test_mode: bool = False):
        """
        Loads/parsers the UberGraph data file to produce node/edge KGX files for importation into a graph database.

        :param data_file_path: the directory that will contain the UberGraph data file
        :param data_file_names: The input file name.
        :param output_mode: the output mode (tsv or json)
        :param block_size: the count threshold to write out the data
        :param test_mode: sets the usage of using a test data file
        :return: None
        """
        logger.info(f'UGLoader - Start of UberGraph data processing.')

        # split the input file names
        file_names = data_file_names.split(',')

        # loop through the data files
        for file_name in file_names:
            # init the node/edge counters
            self.total_nodes = 0
            self.total_edges = 0

            # get the output file name
            out_name = file_name.split('.')[0]

            logger.info(f'Parsing UberGraph data file: {file_name}.')

            with open(os.path.join(data_file_path, f'{out_name}_node_file.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edge_file.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
                # depending on the output mode, write out the node and edge data headers
                if output_mode == 'json':
                    out_node_f.write('{"nodes":[\n')
                    out_edge_f.write('{"edges":[\n')
                else:
                    out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                    out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tsource_database\n')

                # parse the data
                self.parse_data_file(data_file_path, file_name, out_node_f, out_edge_f, output_mode, block_size)

            # do not remove the file if in debug mode
            if logger.level != logging.DEBUG and not test_mode:
                # remove the data file
                os.remove(os.path.join(data_file_path, file_name))

        logger.info(f'UGLoader - Processing complete.')

    def parse_data_file(self, data_file_path: str, data_file_name: str, out_node_f, out_edge_f, output_mode: str, block_size: int):
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the UberGraph data file
        :param data_file_name: the name of the UberGraph file
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :param block_size: write out data threshold
        :return:
        """

        # get a reference to the node and edge normalization classes
        en = EdgeNormUtils(logger.level)
        nn = NodeNormUtils(logger.level)

        # get a reference to the data handler object
        gd = GetData(logger.level)

        # init a line counter
        line_counter: int = 0

        # storage for the nodes and edges
        node_list: list = []
        edge_list: list = []

        # storage for nodes and edges that failed normalization
        node_norm_failures: set = set()
        edge_norm_failures: set = set()

        # get the path to the zip file
        infile_path: str = os.path.join(data_file_path, data_file_name)

        with open(infile_path, 'r') as fp:
            # create a csv reader for it
            csv_reader: reader = reader(fp, delimiter=' ')

            # spin through the list and get the records
            for line in csv_reader:
                # increment the line counter
                line_counter += 1

                # insure we get node-edge-node per line
                # for some reason the data has " ." at the end of each line
                if len(line) == 4:
                    # strip off the url info and formatting characters from the node
                    n1: str = line[0].split('/')[-1].replace('_', ':')[:-1]

                    # column 2 nodes could be of two different types
                    # the first type is in the form http://identifiers.org/????/####
                    # where ????/#### is actually a curie
                    if 'identifiers' in line[2]:
                        tmp: list = line[2].split('/')
                        n2: str = tmp[-2].upper() + ':' + tmp[-1][:-1]
                    # the second type is the same as column 0
                    else:
                        n2: str = line[2].split('/')[-1].replace('_', ':')[:-1]

                    # get the relation (predicate) value
                    relation: str = line[1].split('/')[-1][:-1]

                    # sometimes it has some unnecessary prefixes
                    rel_arr = relation.split('#')

                    # if we found the unnecessary data take the second part
                    if len(rel_arr) > 1:
                        relation = rel_arr[1]
                    # else it is a predicate curie
                    else:
                        relation = relation.replace('_', ':')

                    # did we get something usable back
                    if len(n1) and len(relation) and len(n2):
                        # create the grouping
                        grp: str = f'{n1}/{relation}/{n2}'

                        # create the nodes
                        node_list.append({'grp': f'{grp}', 'node_num': 1, 'id': f'{n1}', 'name': f'{n1}', 'category': '', 'equivalent_identifiers': ''})
                        node_list.append({'grp': f'{grp}', 'node_num': 2, 'id': f'{n2}', 'name': f'{n2}', 'category': '', 'equivalent_identifiers': ''})
                        edge_list.append({'grp': f'{grp}', 'predicate': f'{relation}', 'relation': f'{relation}', 'edge_label': f'{relation}'})
                    else:
                        logger.warning(f'Input file record invalid at line: {line_counter}')

                    if len(edge_list) >= block_size:
                        # normalize the edges
                        failures: set = en.normalize_edge_data(edge_list, self.cached_edge_norms)

                        # save the edge failures
                        edge_norm_failures.update(failures)

                        # normalize the nodes
                        failures: set = nn.normalize_node_data(node_list, self.cached_node_norms)

                        # save the node failures
                        node_norm_failures.update(failures)

                        # write out the data
                        self.write_out_data(node_list, edge_list, out_node_f, out_edge_f, output_mode, 'UberGraph ' + data_file_name.split('.')[0])

                        # clear out the node_list
                        node_list.clear()
                        edge_list.clear()
                else:
                    logger.error(f'Invalid input line at {line_counter}')

                # output for the user
                if line_counter % 500000 == 0:
                    logger.info(f'{line_counter} relationships processed.')

            # save any remainders
            if len(node_list) > 0:
                # normalize the edges
                failures: set = en.normalize_edge_data(edge_list, self.cached_edge_norms)

                # save the edge failures
                edge_norm_failures.update(failures)

                # normalize the nodes
                failures: set = nn.normalize_node_data(node_list, self.cached_node_norms)

                # save the node failures
                node_norm_failures.update(failures)

                # write out the data
                self.write_out_data(node_list, edge_list, out_edge_f, out_node_f, output_mode, 'UberGraph_' + data_file_name)
                logger.info(f'{line_counter} relationships processed creating {self.total_nodes} nodes and {self.total_edges} edges.')

        # finish off the json if we have to
        if output_mode == 'json':
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

        # output the failures
        gd.format_failures(node_norm_failures, edge_norm_failures)

        # create the dataset KGX node data
        # self.get_dataset_provenance(data_file_path, data_prov)

    def write_out_data(self, node_list: list, edge_list: list, out_node_f: TextIOBase, out_edge_f: TextIOBase, output_mode: str, data_source_name: str):
        """
        writes out the data collected from the UberGraph file node list to KGX node and edge files

        :param node_list: the list of nodes create edges and to write out to file
        :param edge_list: the list of edge relations by group name
        :param out_node_f: the node file
        :param out_edge_f: the edge file
        :param output_mode: the output mode (tsv or json)
        :param data_source_name: the name of the source file
        :return: Nothing
        """

        logger.debug(f'Loading data frame with {len(node_list)} nodes.')

        # write out the edges
        self.write_edge_data(out_edge_f, node_list, edge_list, output_mode, data_source_name)

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

        # reshape the data frame and remove all node duplicates.
        new_df = df.drop(['grp', 'node_num'], axis=1)
        new_df = new_df.drop_duplicates(keep='first')

        logger.debug(f'{len(new_df.index)} nodes found.')

        # write out the unique nodes
        for item in new_df.iterrows():
            if output_mode == 'json':
                # turn these into json
                category = json.dumps(item[1]['category'].split('|'))
                identifiers = json.dumps(item[1]['equivalent_identifiers'].split('|'))

                # output the node
                out_node_f.write(f'{{"id":"{item[1]["id"]}", "name":"{item[1]["name"]}", "category":{category}, "equivalent_identifiers":{identifiers}}},\n')
            else:
                out_node_f.write(f"{item[1]['id']}\t{item[1]['name']}\t{item[1]['category']}\t{item[1]['equivalent_identifiers']}\n")

            # increment the total node counter
            self.total_nodes += 1

        logger.debug('Writing out to data file complete.')

    def write_edge_data(self, out_edge_f, node_list: list, edge_list: list, output_mode: str, data_source_name: str):
        """
        writes edges for the node list passed

        :param out_edge_f: the edge file
        :param node_list: list of node groups
        :param edge_list: list of edge relations by group
        :param output_mode: the output mode (tsv or json)
        :param data_source_name: the name of the source file
        :return: Nothing
        """

        logger.debug(f'Creating edges for {len(node_list)} nodes.')

        # init interaction group detection
        cur_grp_name: str = ''
        first: bool = True
        node_idx: int = 0

        # sort the list of interactions in the experiment group
        sorted_nodes = sorted(node_list, key=itemgetter('grp'))

        # get the number of records in this sorted experiment group
        node_count = len(sorted_nodes)

        # iterate through node groups and create the edge records.
        while node_idx < node_count:
            logger.debug(f'Working index: {node_idx}.')

            # if its the first time in prime the pump
            if first:
                # save the interaction name
                cur_grp_name = sorted_nodes[node_idx]['grp']

                # reset the first record flag
                first = False

            # init the list that will contain the node groups
            grp_list: list = []

            # for each entry member in the group
            while sorted_nodes[node_idx]['grp'] == cur_grp_name:
                # add the dict to the group
                grp_list.append(sorted_nodes[node_idx])

                # increment the node counter pairing
                node_idx += 1

                # insure we dont overrun the list
                if node_idx >= node_count:
                    break

            # de-duplicate the list of dicts
            grp_list = [dict(dict_tuple) for dict_tuple in {tuple(dict_in_list.items()) for dict_in_list in grp_list}]

            # init the group index counter
            grp_idx: int = 0

            # init the source and object ids
            source_node_id: str = ''
            object_node_id: str = ''
            edge_relation: str = ''
            grp: str = ''

            # now that we have a group create the edges
            while grp_idx < len(grp_list):
                if grp_list[grp_idx]['node_num'] == 1:
                    # get the source node id
                    source_node_id = grp_list[grp_idx]['id']

                    # get the group id if we haven't already
                    if grp == '':
                        grp = grp_list[grp_idx]['grp']

                    # did we find the group id yet
                    if edge_relation == '':
                        # get the edge relation using the group name
                        edge_relation = [d['relation'] for d in edge_list if d['grp'] == grp_list[grp_idx]["grp"]][0]
                elif grp_list[grp_idx]['node_num'] == 2:
                    # get the object node id
                    object_node_id = grp_list[grp_idx]['id']

                    # get the group id if we haven't already
                    if grp == '':
                        grp = grp_list[grp_idx]['grp']

                    # did we find the group id yet
                    if edge_relation == '':
                        # get the edge relation using the group name
                        edge_relation = [d['relation'] for d in edge_list if d['grp'] == grp_list[grp_idx]["grp"]][0]
                else:
                    logger.error(f'Unknown node number: {grp_list[grp_idx]["node_num"]}')

                # goto the next node in the group
                grp_idx += 1

            # did we get everything
            if len(source_node_id) and len(object_node_id) and len(edge_relation):
                if output_mode == 'json':
                    edge = f', "subject":"{source_node_id}", "relation":"{edge_relation}", "object":"{object_node_id}", "edge_label":"{edge_relation}", "source_database":"{data_source_name}"}},\n'
                else:
                    edge: str = f'\t{source_node_id}\t{edge_relation}\t{edge_relation}\t{object_node_id}\t{data_source_name}\n'

                # write out the edge
                out_edge_f.write(hashlib.md5(edge.encode('utf-8')).hexdigest() + edge)

                # increment the edge count
                self.total_edges += 1
            else:
                logger.debug(f'Node or edge relationship missing: {grp}. ({source_node_id})-[{edge_relation}]-({object_node_id})')

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the next interaction name
            cur_grp_name = sorted_nodes[node_idx]['grp']

        logger.debug(f'{node_idx} edges created.')

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
            'data_set_name': 'UberGraph',
            'data_set_title': 'UberGraph',
            'data_set_web_site': '',
            'data_set_download_url': '',
            'data_set_version': data_version.strftime("%Y%m%d"),
            'data_set_retrieved_on': now.strftime("%Y/%m/%d %H:%M:%S")}

        # create the data description KGX file
        DatasetDescription.create_description(data_path, ds, 'ubergraph')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load IntAct virus interaction data file and create KGX import files.')

    # command line should be like: python loadIA.py -d E:/Data_services/IntAct_data
    ap.add_argument('-u', '--data_dir', required=True, help='The UberGraph data file directory.')
    ap.add_argument('-s', '--data_file', required=True, help='Comma separated UberGraph data file(s) to parse.')

    # parse the arguments
    args = vars(ap.parse_args())

    # UG_data_dir = 'E:/Data_services/UberGraph'
    UG_data_dir = args['data_dir']
    UG_data_file = args['data_file']

    # get a reference to the processor
    ug = UGLoader()

    # load the data files and create KGX output files
    ug.load(UG_data_dir, UG_data_file, test_mode=True)
