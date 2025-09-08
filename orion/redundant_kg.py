from itertools import product
from functools import cache
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from orion.biolink_utils import get_biolink_model_toolkit
from orion.biolink_constants import OBJECT_ASPECT_QUALIFIER, OBJECT_DIRECTION_QUALIFIER, SPECIES_CONTEXT_QUALIFIER, \
    QUALIFIED_PREDICATE, PREDICATE
from orion.utils import quick_jsonl_file_iterator, snakify
from orion.kgx_file_writer import KGXFileWriter

bmt = get_biolink_model_toolkit()

# TODO - really we should get the full list of qualifiers from Common/biolink_constants.py,
#  but because we currently cannot deduce the association types of edges and/or permissible value enumerators,
#  we have to hard code qualifier handling anyway, we might as well check against a smaller list
QUALIFIER_KEYS = [OBJECT_ASPECT_QUALIFIER,
                  OBJECT_DIRECTION_QUALIFIER]
# we do have these qualifiers but we cant do any redundancy with them so ignore for now:
# QUALIFIED_PREDICATE -
# SPECIES_CONTEXT_QUALIFIER -


# bmt does a lot of caching, but because we are doing the string manipulation it's prob a lot faster to cache these
@cache
def get_ancestor_predicates_biolink(predicate):
    cur_predicate = predicate.split(':')[-1]
    return set([f'{snakify(curie)}' for curie in bmt.get_ancestors(cur_predicate, formatted=True, reflexive=False)])


def check_qualifier(ed):
    qfs = []
    for k in ed.keys():
        if bmt.is_qualifier(k):
            qfs.append(k)
    return qfs


def write_edge_no_q(edge, predicate):
    tmp_edge = edge.copy()
    tmp_edge[PREDICATE] = f"{predicate}"
    tmp_edge.pop(OBJECT_DIRECTION_QUALIFIER, None)
    tmp_edge.pop(OBJECT_ASPECT_QUALIFIER, None)
    tmp_edge.pop(QUALIFIED_PREDICATE, None)
    return tmp_edge


def generate_redundant_kg(infile, edges_file_path):

    with KGXFileWriter(edges_output_file_path=edges_file_path) as kgx_file_writer:
        for edge in tqdm(quick_jsonl_file_iterator(infile)) if TQDM_AVAILABLE else quick_jsonl_file_iterator(infile):

            try:
                edge_predicate = edge['predicate']
            except KeyError:
                print(f"Redundant Graph Failed - missing predicate on edge: {edge}")
                break

            ancestor_predicates = get_ancestor_predicates_biolink(edge_predicate)

            # qualifiers = check_qualifier(edge) <- it would be better to do something like this but because we're not
            # handling other qualifiers anyway it's faster to just do the following:
            qualifiers = [qualifier for qualifier in QUALIFIER_KEYS if qualifier in edge]

            # The following looks up the permissible values for ancestors of the current qualfier values.
            # Aspects and directions are handled slightly differently, because when we have aspect AND direction,
            # you cant remove the aspect, but you can remove the direction.

            # for aspect overwrite [None] so that permutations don't include options with no aspect
            aspect_values = [None]
            if OBJECT_ASPECT_QUALIFIER in qualifiers:
                aspect_values = bmt.get_permissible_value_ancestors(permissible_value=edge[OBJECT_ASPECT_QUALIFIER],
                                                                    enum_name='GeneOrGeneProductOrChemicalEntityAspectEnum')

            # for direction include None so permutations include options with no direction
            direction_values = [None]
            if OBJECT_DIRECTION_QUALIFIER in qualifiers:
                direction_values += bmt.get_permissible_value_ancestors(permissible_value=edge[OBJECT_DIRECTION_QUALIFIER],
                                                                        enum_name='DirectionQualifierEnum')

            # permutations of permissible qualifier values and their ancestors, write an edge for each permutation
            edges_to_write = []
            for (a, d) in product(aspect_values, direction_values):
                edge_copy = edge.copy()
                if a:
                    edge_copy[OBJECT_ASPECT_QUALIFIER] = a
                else:
                    edge_copy.pop(OBJECT_ASPECT_QUALIFIER, None)
                if d:
                    edge_copy[OBJECT_DIRECTION_QUALIFIER] = d
                else:
                    edge_copy.pop(OBJECT_DIRECTION_QUALIFIER, None)
                edges_to_write.append(edge_copy)

            # if there was an aspect qualifier, write the edge with no qualifiers because it hasn't happened yet
            if OBJECT_ASPECT_QUALIFIER in qualifiers:
                edges_to_write.append(write_edge_no_q(edge, edge_predicate))

            # write an edge for every ancestor predicate of the original predicate, with no qualifiers
            for ancestor_predicate in ancestor_predicates:
                edges_to_write.append(write_edge_no_q(edge, ancestor_predicate))

            kgx_file_writer.write_normalized_edges(edges_to_write)
