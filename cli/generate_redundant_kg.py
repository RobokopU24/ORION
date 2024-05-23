import argparse
from Common.redundant_kg import generate_redundant_kg
                

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Generate redundant edge files. '
                                             'currently expanding from predicate and qualified_predicate.')
    ap.add_argument('-i', '--infile', help='Input edge file path', required=True)
    ap.add_argument('-o', '--outfile', help='Output edge file path', required=False)
    args = vars(ap.parse_args())

    infile = args['infile']
    edges_file_path = args['outfile']
    generate_redundant_kg(infile, edges_file_path)
