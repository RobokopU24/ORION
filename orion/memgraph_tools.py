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
    output_cypher_file = os.path.join(output_directory, f'memgraph_{graph_id}_{graph_version}.cypher')
    if os.path.exists(output_cypher_file):
        if logger:
            logger.info(f'Memgraph file {output_cypher_file} were already created for {graph_id}({graph_version})')
    else:
        if logger:
            logger.info(f'Creating memgraph dump cypher file for {graph_id}({graph_version})...')
        try:
            kgx_file_converter.convert_jsonl_to_memgraph_cypher(nodes_input_file=nodes_filepath,
                                                                edges_input_file=edges_filepath,
                                                                output_cypher_file=output_cypher_file,
                                                                node_property_ignore_list=node_property_ignore_list,
                                                                edge_property_ignore_list=edge_property_ignore_list)
        except Exception as e:
            if logger:
                logger.error(f'create_memgraph_dump() failed with exception: {e}')
                return False
            else:
                raise e
        if logger:
            logger.info(f'Memgraph cypher dump file created for {graph_id}({graph_version})...')
    return True
