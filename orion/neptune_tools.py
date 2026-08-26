import json
import os

from orion.kgx_neptune_converter import convert_jsonl_to_neptune_csv
from orion.logging import get_orion_logger

logger = get_orion_logger("orion.neptune_tools")

# Written alongside the csv files. It records what the bulk loader needs to know about them, so the
# load step doesn't have to re-derive it by reading the graph back in. See orion.neptune_loader.
NEPTUNE_MANIFEST_FILENAME = 'neptune_load_manifest.json'


def create_neptune_csvs(nodes_filepath: str,
                        edges_filepath: str,
                        output_directory: str,
                        graph_id: str = 'graph',
                        release_version: str = '',
                        node_property_ignore_list: set = None,
                        edge_property_ignore_list: set = None,
                        compress: bool = True):
    """Write the csv files and load manifest for a Neptune bulk load of the given KGX files.

    Unlike a neo4j dump this doesn't produce a single artifact, and no database is involved: the
    csv files are themselves what gets loaded, once they've been copied to S3.
    """
    if release_version:
        sub_name = f'{graph_id}_{release_version}'
    else:
        sub_name = f'{graph_id}'

    extension = '.csv.gz' if compress else '.csv'
    nodes_csv_filename = f'neptune_{sub_name}_nodes{extension}'
    edges_csv_filename = f'neptune_{sub_name}_edges{extension}'
    output_nodes_csv_file = os.path.join(output_directory, nodes_csv_filename)
    output_edges_csv_file = os.path.join(output_directory, edges_csv_filename)
    output_manifest_file = os.path.join(output_directory, NEPTUNE_MANIFEST_FILENAME)

    if os.path.exists(output_manifest_file):
        logger.info(f'Neptune csv files already exist for {graph_id}({release_version})')
        return True

    logger.info(f'Creating Neptune csv files for {graph_id}({release_version})...')
    edge_ids_included = convert_jsonl_to_neptune_csv(
        nodes_input_file=nodes_filepath,
        edges_input_file=edges_filepath,
        nodes_output_file=output_nodes_csv_file,
        edges_output_file=output_edges_csv_file,
        node_property_ignore_list=node_property_ignore_list,
        edge_property_ignore_list=edge_property_ignore_list)

    if not edge_ids_included:
        logger.warning(f'The edges for {graph_id}({release_version}) have no ids, so the load will have to '
                       f'let Neptune generate relationship ids. Building the graph with add_edge_id lets a '
                       f'failed load resume instead of reloading every relationship.')

    # The manifest is written last so that its presence means both csv files finished.
    # Nodes and edges are listed separately because they are loaded as two jobs from two different
    # S3 prefixes - see orion.neptune_loader.
    with open(output_manifest_file, 'w') as manifest_file:
        json.dump({
            'graph_id': graph_id,
            'release_version': release_version,
            'format': 'opencypher',
            'userProvidedEdgeIds': edge_ids_included,
            'nodes': [nodes_csv_filename],
            'edges': [edges_csv_filename]
        }, manifest_file, indent=4)

    logger.info(f'Neptune csv files created for {graph_id}({release_version}).')
    return True


def read_neptune_manifest(csv_directory: str):
    """Read the load manifest create_neptune_csvs() wrote for a directory of Neptune csv files."""
    manifest_path = os.path.join(csv_directory, NEPTUNE_MANIFEST_FILENAME)
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f'{manifest_path} not found - {csv_directory} does not contain a '
                                f'complete set of Neptune csv files.')
    with open(manifest_path) as manifest_file:
        return json.load(manifest_file)