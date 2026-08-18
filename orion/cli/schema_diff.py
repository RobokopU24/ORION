import argparse
import json
import os
import sys

from orion.kgx_schema_diff import SCHEMA_DIFF_FILENAME, diff_schema_files


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(description='Diff two KGX schema.json files and report what '
                                             'changed between them.')
    ap.add_argument('old_schema_filepath', help='the earlier schema.json')
    ap.add_argument('new_schema_filepath', help='the later schema.json')
    ap.add_argument('-o', '--output', metavar='PATH',
                    help=f'write the diff to PATH instead of stdout. Pass a directory to write '
                         f'PATH/{SCHEMA_DIFF_FILENAME}.')
    ap.add_argument('--overwrite', action='store_true',
                    help='replace the output file if it already exists')
    args = ap.parse_args()

    output_path = None
    if args.output:
        output_path = os.path.join(args.output, SCHEMA_DIFF_FILENAME) \
            if os.path.isdir(args.output) else args.output
        if os.path.exists(output_path) and not args.overwrite:
            print(f'Schema diff already exists! Did not overwrite. ({output_path}) '
                  f'Use --overwrite to replace it.', file=sys.stderr)
            return

    schema_diff = diff_schema_files(args.old_schema_filepath, args.new_schema_filepath)

    if output_path:
        with open(output_path, 'w') as output_file:
            json.dump(schema_diff, output_file, indent=2)
        print(f'Wrote schema diff to {output_path}', file=sys.stderr)
    else:
        print(json.dumps(schema_diff, indent=2))


if __name__ == '__main__':
    main()