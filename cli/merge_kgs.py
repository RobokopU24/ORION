import argparse
import sys

from orion.kgx_file_merger import merge_kgx_files


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

