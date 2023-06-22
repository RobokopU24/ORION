import argparse
from Common.neo4j_tools import Neo4jTools

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='')
    ap.add_argument('graph_id')
    ap.add_argument('graph_version')
    ap.add_argument('graph_directory')
    ap.add_argument('--neo4j', action='store_true')

    args = vars(ap.parse_args())
    g_id = args['graph_id']
    g_version = args['graph_version']
    g_directory = args['graph_directory']
    create_neo4j_dump = args['neo4j']
    if create_neo4j_dump:
        neo4j_tools = Neo4jTools(graph_id=g_id,
                                 graph_version=g_version)
        neo4j_tools.create_neo4j_dump(graph_id=g_id,
                                      graph_version=g_version,
                                      graph_directory=g_directory)
