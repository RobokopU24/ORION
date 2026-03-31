import argparse
from orion.memgraph_tools import create_memgraph_dump

def main():

    ap = argparse.ArgumentParser(description='Create memgraph CSV import files from KGX jsonl files.')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    ap.add_argument('output_directory')

    args = vars(ap.parse_args())
    n_filepath = args['nodes_filepath']
    e_filepath = args['edges_filepath']
    output_directory = args['output_directory']

    create_memgraph_dump(n_filepath, e_filepath, output_directory)


if __name__ == '__main__':
    main()

