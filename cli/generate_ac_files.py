import argparse
from Common.answercoalesce_build import generate_ac_files

if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description='Generate node labels, names, links, backlinks, and other AnswerCoalesce files from KGX node/edge files.'
    )
    ap.add_argument('-n', '--nodes', help='Input node file path (JSONL)', required=True)
    ap.add_argument('-e', '--edges', help='Input edge file path (JSONL)', required=True)
    ap.add_argument('-o', '--outdir', help='Output directory', required=False)

    args = vars(ap.parse_args())

    generate_ac_files(
        input_node_file=args['nodes'],
        input_edge_file=args['edges'],
        output_dir=args['outdir']
    )
