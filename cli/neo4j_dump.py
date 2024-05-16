import argparse
import os
from Common.utils import LoggingUtil
from Common.neo4j_tools import create_neo4j_dump

logger = LoggingUtil.init_logging("ORION.cli.neo4j_dump",
                                  line_format='medium',
                                  log_file_path=os.environ['ORION_LOGS'])

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    ap.add_argument('output_directory')

    args = vars(ap.parse_args())
    n_filepath = args['nodes_filepath']
    e_filepath = args['edges_filepath']
    output_directory = args['output_directory']

    create_neo4j_dump(nodes_filepath=n_filepath,
                      edges_filepath=e_filepath,
                      output_directory=output_directory,
                      logger=logger)

