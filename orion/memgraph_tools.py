import os
import orion.kgx_file_converter as kgx_file_converter


def create_memgraph_dump(nodes_filepath: str,
                         edges_filepath: str,
                         output_directory: str,
                         graph_id: str = 'graph',
                         graph_version: str = '',
                         node_property_ignore_list: set = None,
                         edge_property_ignore_list: set = None,
                         logger=None):

    if graph_version:
        sub_name = f'{graph_id}_{graph_version}'
    else:
        sub_name = f'{graph_id}'

    output_csv_node_file = os.path.join(output_directory, f'memgraph_{sub_name}_nodes.csv')
    output_cypher_node_idx_file = os.path.join(output_directory,
                                               f'memgraph_{sub_name}_indexes.cypher')
    output_edge_file = os.path.join(output_directory, f'memgraph_{sub_name}_edges.csv')
    if (os.path.exists(output_csv_node_file) and os.path.exists(output_cypher_node_idx_file) and
            os.path.exists(output_edge_file)):
        if logger:
            logger.info(f'Memgraph files were already created for {graph_id}({graph_version})')
    else:
        if logger:
            logger.info(f'Creating memgraph dump files for {graph_id}({graph_version})...')
        try:
            if not os.path.exists(output_csv_node_file):
                kgx_file_converter.convert_node_jsonl_to_memgraph_csv(
                    nodes_input_file=nodes_filepath,
                    output_file=output_csv_node_file,
                    node_property_ignore_list=node_property_ignore_list)

            if not os.path.exists(output_cypher_node_idx_file):
                kgx_file_converter.add_indexes_to_memgraph_cypher(nodes_filepath, output_cypher_node_idx_file)

            kgx_file_converter.convert_edge_jsonl_to_memgraph_csv(edges_input_file=edges_filepath,
                                                                  output_base_file=output_edge_file,
                                                                  edge_property_ignore_list=edge_property_ignore_list)
        except Exception as e:
            if logger:
                logger.error(f'create_memgraph_dump() failed with exception: {e}')
            raise e
        if logger:
            logger.info(f'Memgraph cypher dump file created for {graph_id}({graph_version})...')
    return True
