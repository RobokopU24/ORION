import argparse
from orion.neo4j_tools import create_neo4j_dump

def main():

    ap = argparse.ArgumentParser(description='Create a neo4j dump from KGX jsonl files.')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    ap.add_argument('output_directory')

    args = vars(ap.parse_args())
    n_filepath = args['nodes_filepath']
    e_filepath = args['edges_filepath']
    output_directory = args['output_directory']

    create_neo4j_dump(nodes_filepath=n_filepath,
                      edges_filepath=e_filepath,
                      output_directory=output_directory)


if __name__ == '__main__':
    main()

