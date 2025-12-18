import argparse
from Common.build_manager import graph_operations

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run post-merge stages (QC, MetaKG, output formats) on existing graph files')
    parser.add_argument('graph_id', help='ID of the graph')
    parser.add_argument('graph_directory',
                        help='Path to directory containing nodes.jsonl, edges.jsonl, and metadata')
    parser.add_argument('--output_format', type=str, default=None,
                        help='Output format (e.g., neo4j, neo4j+redundant_neo4j)')

    args = parser.parse_args()
    success = graph_operations(graph_id=args.graph_id,
                               graph_directory=args.graph_directory,
                               output_format=args.output_format)
    if not success:
        print(f'Graph operations failed for {args.graph_id}')