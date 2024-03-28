import argparse
import os
from Common.utils import LoggingUtil
from Common.neo4j_tools import create_neo4j_dump

logger = LoggingUtil.init_logging("ORION.cli.neo4j_dump",
                                  line_format='medium',
                                  log_file_path=os.environ['ORION_LOGS'])

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='')
    ap.add_argument('graph_directory')
    ap.add_argument('nodes_filename')
    ap.add_argument('edges_filename')
    ap.add_argument('output_directory')

    args = vars(ap.parse_args())
    g_directory = args['graph_directory']
    n_filename = args['nodes_filename']
    e_filename = args['edges_filename']
    output_directory = args['output_directory']

    create_neo4j_dump(graph_directory=g_directory,
                      nodes_filename=n_filename,
                      edges_filename=e_filename,
                      output_directory=output_directory,
                      logger=logger)
