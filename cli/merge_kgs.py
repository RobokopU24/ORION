import argparse
import sys
import os
import json
from datetime import datetime

from Common.kgx_file_merger import KGXFileMerger
from Common.kgxmodel import GraphSpec, GraphSource


# given a list of kgx jsonl node files and edge files,
# create a simple GraphSpec and use KGXFileMerge to merge the files into one node file and one edge file
def merge_kgx_files(output_dir: str, nodes_files: list = None, edges_files: list = None):
    if not nodes_files:
        nodes_files = []
    else:
        for node_file in nodes_files:
            if 'node' not in node_file:
                print('All node files must contain the text "node" in their file name.')
                return False

    if not edges_files:
        edges_files = []
    else:
        for edge_file in edges_files:
            if 'edge' not in edge_file:
                print(f'All edge files must contain the text "edge" in their file name. This file does not: {edge_file}')
                return False

    current_time = datetime.now()
    timestamp = current_time.strftime("%Y/%m/%d %H:%M:%S")
    # TODO it'd be nice to make this something reproducible from the inputs
    version = timestamp.replace('/', '_').replace(':', '_').replace(' ', '_')
    graph_source = GraphSource(id='cli_merge',
                               file_paths=nodes_files + edges_files)
    graph_spec = GraphSpec(
        graph_id='cli_merge',
        graph_name='',
        graph_description=f'Merged on {timestamp}',
        graph_url='',
        graph_version=version,
        graph_output_format='jsonl',
        sources=[graph_source],
        subgraphs=[]
    )
    file_merger = KGXFileMerger(graph_spec=graph_spec,
                                output_directory=output_dir,
                                nodes_output_filename=f'{version}_nodes.jsonl',
                                edges_output_filename=f'{version}_edges.jsonl')
    file_merger.merge()

    merge_metadata = file_merger.get_merge_metadata()
    if "merge_error" in merge_metadata:
        print(f'Merge error occured: {merge_metadata["merge_error"]}')
        return False
    else:
        metadata_output = os.path.join(output_dir, f"{version}_metadata.json")
        with open(metadata_output, 'w') as metadata_file:
            metadata_file.write(json.dumps(merge_metadata, indent=4))


if __name__ == '__main__':

    ap = argparse.ArgumentParser(description="Given a list of node files and/or a list of edge files "
                                             "in kgx jsonl format, merge them into one node and one edge file.")
    ap.add_argument(
        '-n', '--nodes',
        type=str,
        nargs='*',
        help='List of node file paths')

    ap.add_argument(
        '-e', '--edges',
        type=str,
        nargs='*',
        help='List of edge file paths')

    ap.add_argument(
        '-o', '--output_dir',
        type=str,
        required=True,
        help='The directory where the output should be saved')

    args = vars(ap.parse_args())
    if not (args["nodes"] or args["edges"]):
        print(f'To merge kgx files you must provide at least one file to merge.')
        sys.exit(1)

    merge_kgx_files(args["output_dir"], args["nodes"], args["edges"])

