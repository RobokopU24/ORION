
import argparse
import os
from Common.load_manager import SourceDataLoadManager
from Common.build_manager import GraphBuilder

from parsers.cord19.src.loadCord19 import Cord19Loader

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files. Build neo4j graphs.")
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    args = parser.parse_args()

    if 'DATA_SERVICES_TEST_MODE' in os.environ:
        test_mode_from_env = os.environ['DATA_SERVICES_TEST_MODE']
    else:
        test_mode_from_env = False
    test_mode_from_env = True

    loader_test_mode = args.test_mode or test_mode_from_env
    loader_test_mode = False 
    fresh_start_mode = True
   # load_manager = SourceDataLoadManager(test_mode=loader_test_mode)
#    load_manager.start()

#    graph_builder = GraphBuilder()
 #   graph_builder.build_all_graphs()

    #data_source = "Cord19"
    data_source = "Scent"
    #data_source = "HMDB"
    load_manager = SourceDataLoadManager(source_subset=[data_source],
                                                 test_mode=loader_test_mode,
                                                 fresh_start_mode=fresh_start_mode)
    print("Starting")
    load_manager.start()

