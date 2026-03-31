try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from orion.biolink_constants import PREDICATE, QUALIFIED_PREDICATE, SUBJECT_DERIVATIVE_QUALIFIER, SUBJECT_FORM_OR_VARIANT_QUALIFIER, SUBJECT_PART_QUALIFIER, \
    SUBJECT_DIRECTION_QUALIFIER, SUBJECT_ASPECT_QUALIFIER, OBJECT_DERIVATIVE_QUALIFIER, OBJECT_FORM_OR_VARIANT_QUALIFIER, \
    OBJECT_PART_QUALIFIER, OBJECT_DIRECTION_QUALIFIER, OBJECT_ASPECT_QUALIFIER, CAUSAL_MECHANISM_QUALIFIER, \
    ANATOMICAL_CONTEXT_QUALIFIER, SPECIES_CONTEXT_QUALIFIER
from orion.biolink_utils import get_biolink_model_toolkit
from orion.utils import quick_jsonl_file_iterator
from orion.kgx_file_writer import KGXFileWriter

### The goal of this script is to collapse the qualifiers, which are in edge properties, into a single statement, then replace the
### existing predicate label with the collapsed qualifier statement.

### Call the biolink model toolkit to get the list of all qualifiers. This may change, but the way qualifiers are handled is currently hard-coded in this script.
bmt = get_biolink_model_toolkit()

def write_edge_no_q(edge, predicate, qualifiers):
    tmp_edge = edge.copy()
    tmp_edge[PREDICATE] = f"{predicate}"
    for qualifier in qualifiers.keys():
        tmp_edge.pop(qualifier, None)
    return tmp_edge

def aspect_qualifier_semantic_adjustment(aspect_qualifier):
    # TODO check if other aspect qualifiers besides molecular interaction need to be treated differently.
    if aspect_qualifier.split('_')[-1] == 'interaction':
        aspect_conversion = aspect_qualifier + "_with"
    else:
        aspect_conversion = aspect_qualifier + "_of"
    return aspect_conversion

def form_or_variant_qualifier_semantic_adjustment(form_or_variant_qualifier):
    # TODO check if other form_or_variant_qualifier qualifiers besides molecular interaction need to be treated differently.
    form_or_variant_conversion = form_or_variant_qualifier + "_of"
    return form_or_variant_conversion

def causal_mechanism_qualifier_semantic_adjustment(causal_mechanism_qualifier):
    # TODO check if other causal_mechanism qualifiers besides molecular interaction need to be treated differently.
    causal_mechanism_qualifier = "via_"+ causal_mechanism_qualifier
    return causal_mechanism_qualifier

def species_context_qualifier_semantic_adjustment(species_context_qualifier):
    species_context_qualifier = "in_"+ species_context_qualifier
    return species_context_qualifier

def anatomical_context_qualifier_semantic_adjustment(anatomical_context_qualifier, species_context_qualifier=False):
    if species_context_qualifier == False:
        anatomical_context_qualifier = "in_"+ anatomical_context_qualifier
    return anatomical_context_qualifier

def generate_collapsed_qualifiers_kg(infile, edges_file_path):

    with KGXFileWriter(edges_output_file_path=edges_file_path) as kgx_file_writer:
        for edge in tqdm(quick_jsonl_file_iterator(infile)) if TQDM_AVAILABLE else quick_jsonl_file_iterator(infile):

            try:
                edge_predicate = edge['predicate']
            except KeyError:
                print(f"Collapsed Qualifiers Graph Failed - missing predicate on edge: {edge}")
                break

            qualifiers = {key:value for key, value in edge.items() if bmt.is_qualifier(key)}
            # Count the number of qualifiers and print a warning if number of qualifiers we handle in the next section doesn't match number of qualifiers detected.
            # This will help warn us if new qualifiers are added in the future while giving us the option to still run the script as is.
            qualifier_count = len(qualifiers.keys())
            counted_qualifiers = 0

            # The following section crafts a new collapsed qualifier statement to replace the edge predicate, but needs to do some semantic adjustment.
            # This is where to edit if the biolink model ever changes and handles qualifiers differently.
            # Take guidance from: https://biolink.github.io/biolink-model/reading-a-qualifier-based-statement/
            # Example jsonl edge used here: {"subject":"UNII:7PK6VC94OU","predicate":"biolink:affects","object":"NCBIGene:6531","primary_knowledge_source":"infores:ctd","description":"decreases activity of","NCBITaxon":"9606","publications":["PMID:30776375"],"knowledge_level":"knowledge_assertion","agent_type":"manual_agent","subject_direction_qualifier":"increased","subject_aspect_qualifier":"abundance","subject_form_or_variant_qualifier":"mutant_form","subject_derivative_qualifier":"transcript","subject_part_qualifier":"polyA_tail","object_aspect_qualifier":"activity","object_direction_qualifier":"upregulated","object_form_or_variant_qualifier":"wildtype_form","object_derivative_qualifier":"protein","object_part_qualifier":"catalytic_site","causal_mechanism_qualifier":"phosyphorylation","species_context_qualifier":"human","anatomical_context_qualifier":"liver","qualified_predicate":"biolink:causes"}

            qualifier_statement = ""
            
            # Add on subject direction and aspect qualifiers first. eg. "increased_abundance_of_"
            if SUBJECT_DIRECTION_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= qualifiers[SUBJECT_DIRECTION_QUALIFIER]
                qualifier_statement+= "_"
            if SUBJECT_ASPECT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= aspect_qualifier_semantic_adjustment(qualifiers[SUBJECT_ASPECT_QUALIFIER])
                qualifier_statement+= "_"
            # Add on subject form_or_variant qualifiers. eg. "increased_abundance_of_mutant_form_of_<subject_node>"
            if SUBJECT_FORM_OR_VARIANT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= form_or_variant_qualifier_semantic_adjustment(qualifiers[SUBJECT_FORM_OR_VARIANT_QUALIFIER])
                qualifier_statement+= "_"
            # Add placeholder slot for subject node. eg. "increased_abundance_of_mutant_form_of_<subject_node>"
            qualifier_statement+= "<subject_node>_"
            # Add on subject derivative and part qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail"
            if SUBJECT_DERIVATIVE_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= qualifiers[SUBJECT_DERIVATIVE_QUALIFIER]
                qualifier_statement+= "_"
            if SUBJECT_PART_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= qualifiers[SUBJECT_PART_QUALIFIER]
                qualifier_statement+= "_"

            # Add the qualified predicate. eg. "increased_abundance_of_mutant_form_of_<subject_node>_transcript_poly_A_tail_causes"
            if QUALIFIED_PREDICATE in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= qualifiers[QUALIFIED_PREDICATE].replace("biolink:","")
                qualifier_statement+= "_"

            # Add on object direction and aspect qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of"
            if OBJECT_DIRECTION_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= qualifiers[OBJECT_DIRECTION_QUALIFIER]
                qualifier_statement+= "_"
            if OBJECT_ASPECT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= aspect_qualifier_semantic_adjustment(qualifiers[OBJECT_ASPECT_QUALIFIER])
                qualifier_statement+= "_"
            # Add on object form_or_variant qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of"
            if OBJECT_FORM_OR_VARIANT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= form_or_variant_qualifier_semantic_adjustment(qualifiers[OBJECT_FORM_OR_VARIANT_QUALIFIER])
                qualifier_statement+= "_"
            # Add placeholder slot for object node. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of_<object_node>"
            qualifier_statement+= "<object_node>"

            # Add on object derivative and part qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of_<object_node>_protein_catalytic_site"
            # Need to start putting "_" before each qualifier as any given one could be the last in the statement.
            if OBJECT_DERIVATIVE_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= "_"
                qualifier_statement+= qualifiers[OBJECT_DERIVATIVE_QUALIFIER]
            if OBJECT_PART_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= "_"
                qualifier_statement+= qualifiers[OBJECT_PART_QUALIFIER]
   
            # Add on mechanism qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of_<object_node>_protein_catalytic_site_via_phosphorylation"
            if CAUSAL_MECHANISM_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= "_"
                qualifier_statement+= causal_mechanism_qualifier_semantic_adjustment(qualifiers[CAUSAL_MECHANISM_QUALIFIER])

            # Add on species qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of_<object_node>_protein_catalytic_site_via_phosphorylation_in_human"
            if SPECIES_CONTEXT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= "_"
                qualifier_statement+= species_context_qualifier_semantic_adjustment(qualifiers[SPECIES_CONTEXT_QUALIFIER])

            # Add on anatomical context qualifiers. eg. "increased_abundance_of_mutant_form_of<subject_node>_transcript_poly_A_tail_causes_upregulated_activity_of_mutant_form_of_<object_node>_protein_catalytic_site_via_phosphorylation_in_human_liver"
            if ANATOMICAL_CONTEXT_QUALIFIER in qualifiers.keys():
                counted_qualifiers+= 1
                qualifier_statement+= "_"
                if SPECIES_CONTEXT_QUALIFIER in qualifiers.keys():
                    species_qualifier = True
                else:
                    species_qualifier = False
                qualifier_statement+= anatomical_context_qualifier_semantic_adjustment(qualifiers[ANATOMICAL_CONTEXT_QUALIFIER], species_qualifier)
            
            if counted_qualifiers < qualifier_count:
                print(f"Qualifiers on edge: {edge} are not all being handled correctly. Please revise collapse_qualifiers.py to handle all qualifiers.")
            
            # Either rewrite the original edge if no qualifier collapsing happened, or rewrite with new predicate from qualifier_statement.
            edges_to_write = []
            if qualifier_statement != "":
                edges_to_write.append(write_edge_no_q(edge, qualifier_statement, qualifiers))
            else: 
                edges_to_write.append(edge)

            kgx_file_writer.write_normalized_edges(edges_to_write)
