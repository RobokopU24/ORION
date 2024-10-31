try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from Common.biolink_constants import OBJECT_ASPECT_QUALIFIER, OBJECT_DIRECTION_QUALIFIER, SPECIES_CONTEXT_QUALIFIER, \
    QUALIFIED_PREDICATE, PREDICATE
from Common.utils import quick_jsonl_file_iterator
from Common.kgx_file_writer import KGXFileWriter

### The goal of this script is to collapse the qualifiers, which are in edge properties, into a single statement, then replace the
### existing predicate label with the collapsed qualifier statement.


# TODO - really we should get the full list of qualifiers from Common/biolink_constants.py,
#  but because we currently cannot deduce the association types of edges and/or permissible value enumerators,
#  we have to hard code qualifier handling anyway, we might as well check against a smaller list
QUALIFIER_KEYS = [OBJECT_DIRECTION_QUALIFIER, OBJECT_ASPECT_QUALIFIER]
# we do have these qualifiers but we cant do any redundancy with them so ignore for now:
# QUALIFIED_PREDICATE -
# SPECIES_CONTEXT_QUALIFIER -

def write_edge_no_q(edge, predicate):
    tmp_edge = edge.copy()
    tmp_edge[PREDICATE] = f"{predicate}"
    tmp_edge.pop(OBJECT_DIRECTION_QUALIFIER, None)
    tmp_edge.pop(OBJECT_ASPECT_QUALIFIER, None)
    tmp_edge.pop(QUALIFIED_PREDICATE, None)
    return tmp_edge

#
def object_direction_qualifier_semantic_adjustment(object_direction_qualifier):
    object_direction_conversion_map = {
        'increased': 'increases',
        'decreased': 'decreases',
        'upregulated': 'upregulates', 
        'downregulated': 'downregulates',
    }
    try:
        object_direction_conversion = object_direction_conversion_map[object_direction_qualifier]
    except KeyError:
        object_direction_conversion = object_direction_qualifier
    return object_direction_conversion

def object_aspect_qualifier_semantic_adjustment(object_aspect_qualifier):
    # TODO check if other object aspect qualifiers besides molecular interaction need to be treated differently.
    if object_aspect_qualifier.split('_')[-1] == 'molecular_interaction':
        object_aspect_conversion = object_aspect_qualifier + "_with"
    else:
        object_aspect_conversion = object_aspect_qualifier + "_of"
    return object_aspect_conversion

def generate_collapsed_qualifiers_kg(infile, edges_file_path):

    with KGXFileWriter(edges_output_file_path=edges_file_path) as kgx_file_writer:
        for edge in tqdm(quick_jsonl_file_iterator(infile)) if TQDM_AVAILABLE else quick_jsonl_file_iterator(infile):

            try:
                edge_predicate = edge['predicate']
            except KeyError:
                print(f"Collapsed Qualifiers Graph Failed - missing predicate on edge: {edge}")
                break

            # qualifiers = check_qualifier(edge) <- it would be better to do something like this but because we're not
            # handling other qualifiers anyway it's faster to just do the following:
            qualifiers = [qualifier for qualifier in QUALIFIER_KEYS if qualifier in edge]

            qualifier_statement = ""

            object_direction_qualifier_exists = False
            # The following crafts a new collapsed qualifier statement to replace the edge predicate, but needs to do some semantic adjustment.
            if OBJECT_DIRECTION_QUALIFIER in qualifiers:
                object_direction_qualifier_exists = True
                qualifier_statement+= object_direction_qualifier_semantic_adjustment(edge[OBJECT_DIRECTION_QUALIFIER])
            
            if OBJECT_ASPECT_QUALIFIER in qualifiers:
                if object_direction_qualifier_exists == True:
                    qualifier_statement+= "_"
                else: # Currently, we'll just say "affects_something" if no direction is specified.
                    qualifier_statement+= "affects_"
                qualifier_statement+= object_aspect_qualifier_semantic_adjustment(edge[OBJECT_ASPECT_QUALIFIER])
            
            edges_to_write = []
            
            # Either rewrite the original edge if no qualifier collapsing happened, or rewrite with new predicate from qualifier_statement.
            if qualifier_statement != "":
                edges_to_write.append(write_edge_no_q(edge, qualifier_statement))
            else: 
                edges_to_write.append(edge)

            kgx_file_writer.write_normalized_edges(edges_to_write)
