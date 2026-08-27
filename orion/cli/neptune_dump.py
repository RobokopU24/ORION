import argparse
import sys

from orion.neptune_tools import create_neptune_csvs


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(description='Create Amazon Neptune bulk loader CSV files from KGX jsonl files.')
    ap.add_argument('nodes_filepath', help='KGX nodes file, .jsonl or .jsonl.gz.')
    ap.add_argument('edges_filepath', help='KGX edges file, .jsonl or .jsonl.gz.')
    ap.add_argument('output_directory')
    ap.add_argument('--graph_id', default='graph',
                    help='Graph id used to name the output files.')
    ap.add_argument('--release_version', default='',
                    help='Release version used to name the output files.')
    ap.add_argument('--no_compress', action='store_true',
                    help='Write plain .csv files. By default the files are gzipped, which the '
                         'Neptune bulk loader reads directly.')

    args = ap.parse_args()
    try:
        success = create_neptune_csvs(nodes_filepath=args.nodes_filepath,
                                      edges_filepath=args.edges_filepath,
                                      output_directory=args.output_directory,
                                      graph_id=args.graph_id,
                                      release_version=args.release_version,
                                      compress=not args.no_compress)
    except Exception as e:
        print(f'Neptune csv conversion failed: {e}', file=sys.stderr)
        sys.exit(1)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()