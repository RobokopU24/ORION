import os
import hashlib
import argparse
import pandas as pd
import logging
import json
import time

from datetime import datetime
from rdflib import Graph
from operator import itemgetter
from Common.utils import LoggingUtil, NodeNormUtils, DatasetDescription, EdgeNormUtils, GetData
from pathlib import Path


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

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    # for tracking counts
    total_nodes: int = 0
    total_edges: int = 0

    # lists for the output data
    final_node_set: set = set()
    final_edge_set: set = set()

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
        self.logger = LoggingUtil.init_logging("Data_services.UberGraph.UGLoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    # init the node and edge data arrays
    def load(self, data_file_path: str, data_file_names: str, output_mode: str = 'json', file_size: int = 150000, test_mode: bool = False):
        """
        Loads/parsers the UberGraph data file to produce node/edge KGX files for importation into a graph database.

        :param data_file_path: the directory that will contain the UberGraph data file
        :param data_file_names: The input file name.
        :param output_mode: the output mode (tsv or json)
        :param file_size: the threshold data count to initiate writing data out
        :param test_mode: sets the usage of using a test data file
        :return: None
        """
        self.logger.info(f'UGLoader - Start of UberGraph data processing.')

        # split the input file names
        file_names = data_file_names.split(',')

        # loop through the data files
        for file_name in file_names:
            # init the node/edge counters
            self.total_nodes = 0
            self.total_edges = 0

            # get the output file name
            out_name = file_name.split('.')[0]

            # open the output files
            with open(os.path.join(data_file_path, f'{out_name}_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
                # depending on the output mode, write out the node and edge data headers
                if output_mode == 'json':
                    out_node_f.write('{"nodes":[\n')
                    out_edge_f.write('{"edges":[\n')
                else:
                    out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                    out_edge_f.write(f'id\tpredicate\tsubject\trelation\tedge_label\tobject\tsource_database\n')

                self.logger.info(f'Splitting UberGraph data file: {file_name}. {file_size} records per file + remainder')

                # parse the data
                split_files = self.parse_data_file(data_file_path, file_name, out_node_f, out_edge_f, output_mode, file_size)

            # do not remove the file if in debug mode
            if self.logger.level != logging.DEBUG and not test_mode:
                # remove the data file
                os.remove(os.path.join(data_file_path, file_name))

                # remove all the intermediate files
                for file in split_files:
                    os.remove(file)

        self.logger.info(f'UGLoader - Processing complete.')

    def parse_data_file(self, data_file_path: str, data_file_name: str, out_node_f, out_edge_f, output_mode: str, block_size: int) -> list:
        """
        Parses the data file for graph nodes/edges and writes them out the KGX tsv files.

        :param data_file_path: the path to the UberGraph data file
        :param data_file_name: the name of the UberGraph file
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :param block_size: the threshold data count to initiate writing data out
        :return: split_files: the temporary files created of the input file
        """

        # get a reference to the data handler object
        gd = GetData(self.logger.level)

        # storage for the nodes and edges
        node_list: list = []
        edge_list: list = []

        # init a list for the output data
        triple: list = []

        # split the file into pieces
        split_files: list = gd.split_file(data_file_path, data_file_name, block_size)

        # parse each file
        for file in split_files:
            self.logger.info(f'Working file: {file}')

            # get a time stamp
            tm_start = time.time()

            # get the biolink json-ld data
            g: Graph = gd.get_biolink_graph(file)

            # get the triples
            g_t = g.triples((None, None, None))

            # for every triple in the input data
            for t in g_t:
                # clear before use
                triple.clear()

                # get the curie for each element in the triple
                for n in t:
                    try:
                        # get the value
                        val: str = g.compute_qname(n)[2]

                        # if string is all lower it is not a curie
                        if not val.islower():
                            # replace the underscores to create a curie
                            val = g.compute_qname(n)[2].replace('_', ':')
                    except Exception as e:
                        # save it anyway
                        val = n
                        self.logger.warning(f'Exception parsing RDF qname {val}. {e}')

                    # add it to the group
                    triple.append(val)

                # create the grouping
                grp: str = '/'.join(triple)

                # create the nodes
                node_list.append({'grp': f'{grp}', 'node_num': 1, 'id': f'{triple[0]}', 'name': f'{triple[0]}', 'category': '', 'equivalent_identifiers': ''})
                edge_list.append({'grp': f'{grp}', 'predicate': f'{triple[1]}', 'relation': f'{triple[1]}', 'edge_label': f'{triple[1]}'})
                node_list.append({'grp': f'{grp}', 'node_num': 2, 'id': f'{triple[2]}', 'name': f'{triple[2]}', 'category': '', 'equivalent_identifiers': ''})

            # write out the data for this file
            self.write_out_data(node_list, edge_list, output_mode, 'UberGraph ' + data_file_name.split('.')[0])

            self.logger.debug(f'Loading complete for file {file.split(".")[2]} of {len(split_files)} in {round(time.time() - tm_start, 0)} seconds.')

        # write out the node data
        if output_mode == 'json':
            out_node_f.write(',\n'.join(self.final_node_set))
        else:
            out_node_f.write('\n'.join(self.final_node_set))

        # write out the edge data
        if output_mode == 'json':
            out_edge_f.write(',\n'.join(self.final_edge_set))
        else:
            out_edge_f.write('\n'.join(self.final_edge_set))

        # finish off the json if we have to
        if output_mode == 'json':
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

        # output the failures
        gd.format_normalization_failures(self.get_name(), self.node_norm_failures, self.edge_norm_failures)

        # create the dataset KGX node data
        # self.get_dataset_provenance(data_file_path, data_prov)

        # return the split file names so they can be removed if desired
        return split_files

    def write_out_data(self, node_list: list, edge_list: list, output_mode: str, data_source_name: str):
        """
        writes out the data collected from the UberGraph file node list to KGX node and edge files

        :param node_list: the list of nodes create edges and to write out to file
        :param edge_list: the list of edge relations by group name
        :param output_mode: the output mode (tsv or json)
        :param data_source_name: the name of the source file
        :return: Nothing
        """

        # get a reference to the node and edge normalization classes
        en = EdgeNormUtils(self.logger.level)
        nn = NodeNormUtils(self.logger.level)

        self.logger.debug(f'Normalizing data.')

        # normalize the nodes
        failures: list = nn.normalize_node_data(node_list, self.cached_node_norms, block_size=2900)

        # save the node failures
        self.node_norm_failures.extend(failures)

        # normalize the edges
        failures: list = en.normalize_edge_data(edge_list, self.cached_edge_norms, block_size=1000)

        # save the edge failures
        self.edge_norm_failures.extend(failures)

        self.logger.debug('Writing out data...')

        # write out the edges. nodes that have edges will be returned
        new_node_list: list = self.write_edge_data(node_list, edge_list, output_mode, data_source_name)

        # create a data frame with the node list
        df_nodes: pd.DataFrame = pd.DataFrame(new_node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

        # reshape the data frame and remove all node duplicates.
        new_df: pd.DataFrame = df_nodes.drop(['grp', 'node_num'], axis=1)
        new_df.drop_duplicates(keep='first', inplace=True)

        self.logger.debug(f'{len(new_df.index)} nodes found.')

        # write out the unique nodes
        for item in new_df.iterrows():
            if output_mode == 'json':
                # turn these into json
                category: str = json.dumps(item[1]['category'].split('|'))
                identifiers: str = json.dumps(item[1]['equivalent_identifiers'].split('|'))
                name: str = item[1]["name"].replace('"', '\\"')

                # output the node
                node: str = f'{{"id":"{item[1]["id"]}","name":"{name}","category":{category},"equivalent_identifiers":{identifiers}}}'
            else:
                node: str = f"{item[1]['id']}\t{item[1]['name']}\t{item[1]['category']}\t{item[1]['equivalent_identifiers']}"

            # save the node text
            self.final_node_set.add(node)

            # increment the total node counter
            self.total_nodes += 1

        # clear out for the next load
        node_list.clear()
        edge_list.clear()

        self.logger.debug('Writing out to data file complete.')

    def write_edge_data(self, node_list: list, edge_list: list, output_mode: str, data_source_name: str) -> list:
        """
        writes edges for the node list passed

        :param node_list: list of node groups
        :param edge_list: list of edge relations by group
        :param output_mode: the output mode (tsv or json)
        :param data_source_name: the name of the source file
        :return: Nothing
        """

        self.logger.debug(f'Creating edges for {len(node_list)} nodes.')

        # init group detection variables
        cur_grp_name: str = ''
        first: bool = True
        node_idx: int = 0
        grp_list: list = []

        # init the new node list to be returned
        new_node_list: list = []

        # convert the edge list into a dataframe for faster searching
        df_edges: pd.DataFrame = pd.DataFrame(edge_list, columns=['grp', 'predicate', 'relation', 'edge_label'])
        df_edges.set_index(keys=['grp'], inplace=True)

        # sort the list of interactions in the experiment group
        sorted_nodes: list = sorted(node_list, key=itemgetter('grp'))

        # get the number of records in this sorted experiment group
        node_count: int = len(sorted_nodes)

        # iterate through node groups and create the edge records.
        while node_idx < node_count:
            # if its the first time in prime the pump
            if first:
                # save the interaction name
                cur_grp_name = sorted_nodes[node_idx]['grp']

                # reset the first record flag
                first = False

            # init the list that will contain the node groups
            grp_list.clear()

            # for each entry member in the group
            while sorted_nodes[node_idx]['grp'] == cur_grp_name:
                # add the dict to the group
                grp_list.append(sorted_nodes[node_idx])

                # increment the node counter pairing
                node_idx += 1

                # insure we dont overrun the list
                if node_idx >= node_count:
                    break

            # if we didnt get a pair then we cant create an edge
            if len(grp_list) > 2:
                self.logger.info(f'Nodes in group > 2 {cur_grp_name}')
            elif len(grp_list) < 2:
                # insure we dont overrun the list
                if node_idx >= node_count:
                    break

                # save the next interaction name
                cur_grp_name = sorted_nodes[node_idx]['grp']

                continue

            # init the group index counter
            grp_idx: int = 0

            # init the source and object ids
            source_node: dict = {}
            object_node: dict = {}

            # get the predicate, relation and label using the group name
            edge_relation = df_edges.loc[cur_grp_name].relation
            edge_label = df_edges.loc[cur_grp_name].edge_label
            edge_predicate = df_edges.loc[cur_grp_name].predicate

            # now that we have a group create the edges
            while grp_idx < len(grp_list):
                if grp_list[grp_idx]['node_num'] == 1:
                    # get the source node id
                    source_node = grp_list[grp_idx]
                elif grp_list[grp_idx]['node_num'] == 2:
                    # get the object node id
                    object_node = grp_list[grp_idx]
                else:
                    self.logger.error(f'Unknown node number: {grp_list[grp_idx]["node_num"]}')

                # goto the next node in the group
                grp_idx += 1

            # did we get everything
            if len(source_node) > 0 and len(object_node) > 0 and edge_relation != '':
                # add the nodes to the new node list
                new_node_list.append(source_node)
                new_node_list.append(object_node)

                # create the record ID
                record_id: str = source_node['id'] + edge_relation + object_node['id']

                # write out the edge
                if output_mode == 'json':
                    self.final_edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}","predicate":"{edge_predicate}","subject":"{source_node["id"]}","relation":"{edge_relation}","object":"{object_node["id"]}","edge_label":"{edge_label}","source_database":"{data_source_name}"}}')
                else:
                    self.final_edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{edge_predicate}\t{source_node["id"]}\t{edge_relation}\t{edge_label}\t{object_node["id"]}\t{data_source_name}')

                # increment the edge count
                self.total_edges += 1
            else:
                self.logger.debug(f'Node or edge relationship missing: {cur_grp_name}. ({source_node["id"]})-[{edge_relation}]-({object_node["id"]})')

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the next interaction name
            cur_grp_name = sorted_nodes[node_idx]['grp']

        self.logger.debug(f'{node_idx} edges created.')

        # return the new node list to the caller
        return new_node_list

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
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json)')

    # parse the arguments
    args = vars(ap.parse_args())

    # UG_data_dir = 'E:/Data_services/UberGraph'
    UG_data_dir = args['data_dir']
    UG_data_file = args['data_file']

    # get the output mode
    out_mode = args['out_mode']

    # get a reference to the processor logging.DEBUG
    ug = UGLoader()

    # load the data files and create KGX output files
    ug.load(UG_data_dir, UG_data_file, out_mode, file_size=200000)
