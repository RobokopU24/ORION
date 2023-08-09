import argparse
from Common.neo4j_tools import create_neo4j_dump

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='')
    ap.add_argument('graph_directory')
    ap.add_argument('nodes_filename')
    ap.add_argument('edges_filename')

    args = vars(ap.parse_args())
    g_directory = args['graph_directory']
    n_filename = args['nodes_filename']
    e_filename = args['edges_filename']
    create_neo4j_dump(graph_directory=g_directory,
                      nodes_filename=n_filename,
                      edges_filename=e_filename)
