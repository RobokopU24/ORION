
import argparse
import os
from Common.load_manager import SourceDataLoadManager
from Common.build_manager import GraphBuilder


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform data sources into KGX files. Build neo4j graphs.")
    parser.add_argument('-t', '--test_mode', action='store_true', help='Test mode will load a small sample version of the data.')
    args = parser.parse_args()

    if 'DATA_SERVICES_TEST_MODE' in os.environ:
        test_mode_from_env = os.environ['DATA_SERVICES_TEST_MODE']
    else:
        test_mode_from_env = False

    loader_test_mode = args.test_mode or test_mode_from_env
    load_manager = SourceDataLoadManager(test_mode=loader_test_mode)
    load_manager.start()

    graph_builder = GraphBuilder()
    graph_builder.build_all_graphs()
