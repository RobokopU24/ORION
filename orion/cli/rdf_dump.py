import argparse
from orion.kgx_rdf_converter import convert_kgx_to_rdf

def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(description='Create an RDF 1.2 N-Triples file (.nt.gz) from KGX jsonl files.')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    ap.add_argument('output_filepath', help='path to the output .nt.gz file')
    ap.add_argument('--biolink_version', default=None,
                    help='Biolink version for the prefix map and slot lookups (defaults to the configured version)')

    args = vars(ap.parse_args())
    convert_kgx_to_rdf(nodes_input_file=args['nodes_filepath'],
                       edges_input_file=args['edges_filepath'],
                       output_file=args['output_filepath'],
                       biolink_version=args['biolink_version'])


if __name__ == '__main__':
    main()
