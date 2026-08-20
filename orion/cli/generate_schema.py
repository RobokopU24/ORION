import argparse
import os
import sys

from orion.kgx_bundle import KGXBundle
from orion.kgx_metadata import generate_kgx_schema_file


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(description='Generate a KGX schema describing the nodes, edges '
                                             'and attributes in a pair of KGX jsonl files.')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    ap.add_argument('-o', '--output', metavar='DIR',
                    help=f'directory to write {KGXBundle.SCHEMA_FILENAME} into '
                         f'(default: alongside the nodes file)')
    ap.add_argument('--graph-name', default='',
                    help='name of the graph the schema describes, used in the schema header')
    ap.add_argument('--graph-url', default='',
                    help='url of the graph the schema describes, used for its @id')
    ap.add_argument('--biolink-version', default=None,
                    help='Biolink Model version used to resolve node categories (default: latest)')
    ap.add_argument('--overwrite', action='store_true',
                    help='replace the schema file if it already exists')
    args = ap.parse_args()

    output_dir = args.output or os.path.dirname(args.nodes_filepath) or '.'
    schema_filepath = os.path.join(output_dir, KGXBundle.SCHEMA_FILENAME)
    if os.path.exists(schema_filepath) and not args.overwrite:
        print(f'Schema already exists! Did not overwrite. ({schema_filepath}) '
              f'Use --overwrite to replace it.', file=sys.stderr)
        return

    generate_kgx_schema_file(nodes_filepath=args.nodes_filepath,
                             edges_filepath=args.edges_filepath,
                             output_dir=output_dir,
                             graph_output_url=args.graph_url,
                             graph_name=args.graph_name,
                             biolink_version=args.biolink_version)
    print(f'Wrote schema to {schema_filepath}', file=sys.stderr)


if __name__ == '__main__':
    main()