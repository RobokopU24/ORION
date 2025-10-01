import argparse
from Common.answercoalesce_build import generate_ac_files

if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description='Generate node labels, names, links, backlinks, and other AnswerCoalesce files from KGX node/edge files.'
    )
    ap.add_argument('-n', '--nodes', help='Input node file path (JSONL)', required=True)
    ap.add_argument('-e', '--edges', help='Input edge file path (JSONL)', required=True)
    ap.add_argument('--output_nodelabels', help='Output node labels file', default='nodelabels.txt')
    ap.add_argument('--output_nodenames', help='Output node names file', default='nodenames.txt')
    ap.add_argument('--output_category_count', help='Output category count file', default='category_count.txt')
    ap.add_argument('--output_prov', help='Output provenance file', default='prov.txt')
    ap.add_argument('--output_links', help='Output links file', default='links.txt')
    ap.add_argument('--output_backlinks', help='Output backlinks file', default='backlinks.txt')

    args = vars(ap.parse_args())

    generate_ac_files(
        input_node_file=args['nodes'],
        input_edge_file=args['edges'],
        output_nodelabels=args['output_nodelabels'],
        output_nodenames=args['output_nodenames'],
        output_category_count=args['output_category_count'],
        output_prov=args['output_prov'],
        output_links=args['output_links'],
        output_backlinks=args['output_backlinks']
    )
