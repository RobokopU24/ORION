import os

from Common.biolink_constants import *
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader


"""
NOTE these are in the molepro data file but aren't supported here (relation is deprecated)

'biolink:relation'
'biolink:update_date'
'attributes'

NOTE that FDA_approval_status is in the edges file headers but it should be highest_FDA_approval_status

"""


class MoleProLoader(SourceDataLoader):
    source_id: str = "MolePro"
    provenance_id: str = "infores:molepro"
    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = ' https://molepro.s3.amazonaws.com/'
        self.node_file_name: str = 'nodes.tsv'
        self.edge_file_name: str = 'edges.tsv'

        self.data_files = [
            self.node_file_name,
            self.edge_file_name
        ]

    def get_latest_source_version(self) -> str:
        return "1.0"

    def get_data(self) -> int:
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX files.

        :return: ret_val: record counts
        """
        record_counter = 0
        skipped_record_counter = 0
        skipped_node_counter = 0

        delimiter = '|'
        node_property_indexes = {}
        node_file_path: str = os.path.join(self.data_path, self.node_file_name)
        with open(node_file_path, 'r') as node_file:
            for line in node_file:
                node_file_line = line.split('\t')
                if not node_property_indexes:
                    # look at the file header and determine the indexes of the node properties, if they exist
                    # check for the properties with and without the biolink prefix
                    for node_property in BIOLINK_NODE_PROPERTIES + [f'biolink:{node_p}' for node_p in BIOLINK_NODE_PROPERTIES]:
                        try:
                            node_property_indexes[node_property] = node_file_line.index(node_property)
                        except ValueError:
                            pass
                    node_properties_to_split = [node_property.removeprefix('biolink:') for node_property in node_property_indexes
                                                if node_property.removeprefix('biolink:') in BIOLINK_PROPERTIES_THAT_ARE_LISTS]
                else:
                    # make a dictionary with the biolink properties on that line
                    next_node = {
                        node_property.removeprefix('biolink:'): node_file_line[node_property_indexes[node_property]]
                        for node_property in node_property_indexes if node_file_line[node_property_indexes[node_property]]
                    }
                    # check and make sure it has all the required node properties (except name could be empty)
                    if any(not next_node[node_property] for node_property in
                           REQUIRED_NODE_PROPERTIES if node_property is not NAME):
                        skipped_node_counter += 1
                        continue
                    # convert the properties that should be lists to lists and split on a delimiter
                    for node_property in node_properties_to_split:
                        if node_property in next_node:
                            next_node[node_property] = next_node[node_property].split(delimiter)
                    # write the node to file
                    self.output_file_writer.write_node(node_id=next_node.pop(NODE_ID),
                                                       node_name=next_node.pop(NAME),
                                                       node_types=next_node.pop(NODE_TYPES),
                                                       node_properties=next_node)
                    
        edge_property_indexes = {}
        edge_file_path: str = os.path.join(self.data_path, self.edge_file_name)
        with open(edge_file_path, 'r') as edge_file:
            for line in edge_file:
                edge_file_line = line.split('\t')
                if not edge_property_indexes:
                    # look at the file header and determine the indexes of the edge properties, if they exist
                    for edge_property in BIOLINK_EDGE_PROPERTIES + [f'biolink:{edge_p}' for edge_p in BIOLINK_EDGE_PROPERTIES]:
                        try:
                            edge_property_indexes[edge_property] = edge_file_line.index(edge_property)
                        except ValueError:
                            pass
                    edge_properties_to_split = [edge_property.removeprefix('biolink:') for edge_property in edge_property_indexes
                                                if edge_property.removeprefix('biolink:') in BIOLINK_PROPERTIES_THAT_ARE_LISTS]
                else:
                    if self.test_mode and record_counter > 20000:
                        break

                    # make a dictionary with the biolink properties on that line
                    next_edge = {
                        edge_property.removeprefix('biolink:'): edge_file_line[edge_property_indexes[edge_property]]
                        for edge_property in edge_property_indexes if edge_file_line[edge_property_indexes[edge_property]]
                    }
                    # check to make sure it has all the required properties
                    if any(not next_edge[edge_property] for edge_property in REQUIRED_EDGE_PROPERTIES):
                        skipped_record_counter += 1
                        continue
                    # convert the properties that should be lists to lists and split on a delimiter
                    for edge_property in edge_properties_to_split:
                        if edge_property in next_edge:
                            next_edge[edge_property] = next_edge[edge_property].split(delimiter)

                    # make sure there aren't multiple primary knowledge sources
                    next_edge[PRIMARY_KNOWLEDGE_SOURCE] = next_edge[PRIMARY_KNOWLEDGE_SOURCE].split('|')[0]

                    # write the edge to file
                    self.output_file_writer.write_normalized_edge(next_edge)
                    record_counter += 1

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter,
            'unusable_nodes': skipped_node_counter}
        return load_metadata
