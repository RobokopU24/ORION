import os
import orion.kgx_file_converter as kgx_file_converter
from orion.utils import LoggingUtil

logger = LoggingUtil.init_logging("ORION.orion.memgraph_tools", 
                                  line_format='medium',
                                  log_file_path=os.getenv('ORION_LOGS'))


def create_memgraph_dump(nodes_filepath: str,
                         edges_filepath: str,
                         output_directory: str,
                         graph_id: str = 'graph',
                         graph_version: str = '',
                         node_property_ignore_list: set = None,
                         edge_property_ignore_list: set = None):

    if graph_version:
        sub_name = f'{graph_id}_{graph_version}'
    else:
        sub_name = f'{graph_id}'

    output_csv_node_file = os.path.join(output_directory, f'memgraph_{sub_name}_nodes.csv')
    output_cypher_node_idx_file = os.path.join(output_directory,
                                               f'memgraph_{sub_name}_indexes.cypher')
    output_edge_base_file = os.path.join(output_directory, f'memgraph_{sub_name}_edges.csv')
    output_edge_manifest = os.path.splitext(output_edge_base_file)[0] + '_manifest.txt'

    try:
        if not os.path.exists(output_csv_node_file):
            logger.info(f'Creating memgraph node csv for {graph_id}({graph_version})...')
            kgx_file_converter.convert_node_jsonl_to_memgraph_csv(
                nodes_input_file=nodes_filepath,
                output_file=output_csv_node_file,
                node_property_ignore_list=node_property_ignore_list)

        if not os.path.exists(output_cypher_node_idx_file):
            logger.info(f'Creating memgraph index cypher for {graph_id}({graph_version})...')
            kgx_file_converter.add_indexes_to_memgraph_cypher(nodes_filepath, output_cypher_node_idx_file)

        if not os.path.exists(output_edge_manifest):
            logger.info(f'Creating memgraph edge csvs for {graph_id}({graph_version})...')
            kgx_file_converter.convert_edge_jsonl_to_memgraph_csv(edges_input_file=edges_filepath,
                                                                  output_base_file=output_edge_base_file,
                                                                  edge_property_ignore_list=edge_property_ignore_list)
    except Exception as e:
        logger.error(f'create_memgraph_dump() failed with exception: {e}')
        return False
    logger.info(f'Memgraph dump files created for {graph_id}({graph_version}).')
    return True
