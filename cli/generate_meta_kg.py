import argparse
import os
from Common.meta_kg import MetaKnowledgeGraphBuilder, META_KG_FILENAME, TEST_DATA_FILENAME

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Generate MetaKG and test data files '
                                             'from a pair of node and edge jsonl files.')
    ap.add_argument('nodes_filepath')
    ap.add_argument('edges_filepath')
    args = vars(ap.parse_args())
    nodes_filepath = args['nodes_filepath']
    edges_filepath = args['edges_filepath']

    if '/' in nodes_filepath:
        metakg_output_filepath = nodes_filepath.rsplit('/', maxsplit=1)[0] + '/' + META_KG_FILENAME
        test_data_output_filepath = nodes_filepath.rsplit('/', maxsplit=1)[0] + '/' + TEST_DATA_FILENAME
    else:
        metakg_output_filepath = './' + META_KG_FILENAME
        test_data_output_filepath = './' + TEST_DATA_FILENAME

    mkgb = MetaKnowledgeGraphBuilder(nodes_file_path=nodes_filepath,
                                     edges_file_path=edges_filepath)
    print(f'Generated meta kg and test data. Writing to file..')
    if not os.path.exists(metakg_output_filepath):
        mkgb.write_meta_kg_to_file(metakg_output_filepath)
        print(f'Meta KG complete ({metakg_output_filepath})')
    else:
        print(f'Meta KG already exists! Did not overwrite. ({metakg_output_filepath})')

    if not os.path.exists(test_data_output_filepath):
        mkgb.write_test_data_to_file(test_data_output_filepath)
        print(f'Test data complete ({test_data_output_filepath})')
    else:
        print(f'Test data already exists! Did not overwrite. ({test_data_output_filepath})')