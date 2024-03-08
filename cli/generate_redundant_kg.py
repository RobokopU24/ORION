import os
import sys
import re
import argparse
from itertools import product
from tqdm import tqdm
from Common.utils import quick_jsonl_file_iterator, snakify
from Common.kgx_file_writer import KGXFileWriter

from bmt import Toolkit


bmt = Toolkit()


QUALIFIER_KEYS = ['object_aspect_qualifier', 'object_direction_qualifier', 'qualified_predicate', 'species_context_qualifier']
ASPECT_QUALIFIER = 'object_aspect_qualifier'
DIRECTION_QUALIFIER = 'object_direction_qualifier'
QUALIFIED_PREDICATE = 'qualified_predicate'


def get_ancestor_predicates_biolink(predicate):
    cur_predicate = predicate.split(':')[-1]
    return set([f'{snakify(curie)}' for curie in bmt.get_ancestors(cur_predicate, formatted=True)])

def check_qualifier(ed):
    qfs = []
    for k in ed.keys():
        if bmt.is_qualifier(k):
            qfs.append(k)
    return qfs

def write_edge_no_q(ed, predicate):
    tmp_edge = dict(ed) # Not sure if it's still tied to original edge dictionary
    tmp_edge['predicate'] = f"{predicate}"
    tmp_edge.pop(DIRECTION_QUALIFIER, None)
    tmp_edge.pop(ASPECT_QUALIFIER, None)
    tmp_edge.pop(QUALIFIED_PREDICATE, None)
    return tmp_edge

def generate_redundant_kg(infile, edges_file_path):
    
    num_edges = 0
    num_bio_edges = 0
    num_other_edges = 0
    non_biolink_predicates = set()
    with KGXFileWriter(edges_output_file_path=edges_file_path) as kgx_file_writer:
        for edge in tqdm(quick_jsonl_file_iterator(infile)):
            ancestor_predicates = set()
            # qual_predicate = 'No_qualified_predicate'
            aspect_values = []
            direction_values = [None]
            try:
                #if re.match('biolink.*', edge['predicate']): # bmt may already conform all edges to this format. Conditional statement could be omitted once confirmed.
                ancestor_predicates = ancestor_predicates.union(get_ancestor_predicates_biolink(edge['predicate']))

                qualifiers = check_qualifier(edge)
                
                if ASPECT_QUALIFIER in qualifiers:
                    aspect_values += bmt.get_permissible_value_ancestors(permissible_value=edge[ASPECT_QUALIFIER], enum_name='GeneOrGeneProductOrChemicalEntityAspectEnum')
                    if DIRECTION_QUALIFIER in qualifiers:
                        # store tuples that represent all combinations of aspect_qualifier and direction_qualifier
                        direction_values = [edge[DIRECTION_QUALIFIER], None]

                
                # Log the uncharted qualifiers here. Ignored, save for future investigation
                #for k in qualifiers:
                #    if k not in QUALIFIER_KEYS:
                #        print(k)
    
                expand_edges = []
                
                for cur_pre in ancestor_predicates:
                    
                    #if (cur_pre == edge['predicate'].strip('biolink:')) and (len(aspect_values) >0): # There might be a function in bmt that retrieves normalized predicate names that could be updated here.
                    if (cur_pre == edge['predicate']) and (len(aspect_values) >0): # get_ancestor(formatted=True ) should resolve it.
                        # run tuples and change q_edge, will need another copy to enable popping key:value pairs of qualifiers
                        for (a,d) in product(aspect_values, direction_values):
                            q_edge = dict(edge)
                            if d == None:
                                q_edge.pop(DIRECTION_QUALIFIER, None)

                            else:
                                q_edge[DIRECTION_QUALIFIER] = d
                                
                            q_edge[ASPECT_QUALIFIER] = a
                            expand_edges.append(q_edge)
                    
                    expand_edges.append(write_edge_no_q(edge, cur_pre))
                    
                            
                #expand_edges.append(dict()) # Just to separate inoput instance for verification purpose. Should be deleted at production runs.
                kgx_file_writer.write_normalized_edges(iter(expand_edges))
                
                
            except KeyError:
                print("There is no key named predicate in the dictionary. This does not look like an edge object")
         

        
if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Generate redundant edge files. '
                                             'currently expanding from predicate and qualified_predicate.')
    ap.add_argument('-i', '--infile', help='Input edge file path', required=True)
    ap.add_argument('-o', '--outfile', help='Output edge file path', required=False)
    args = vars(ap.parse_args())

    infile =  args['infile'] #"/home/Documents/RENCI/ROBOKOP/generate_redundant_graphs/data/edges.jsonl.10"
    edges_file_path = args['outfile']  #"/home/Documents/RENCI/ROBOKOP/generate_redundant_graphs/data/edges.redundant.jsonl.10"
    
    generate_redundant_kg(infile, edges_file_path)