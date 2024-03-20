# A collection of constants for biolink variable names and types
# TODO it would be nice to verify these with bmt
NAMED_THING = 'biolink:NamedThing'
BIOLOGICAL_ENTITY = 'biolink:BiologicalEntity'
DISEASE_OR_PHENOTYPIC_FEATURE = 'biolink:DiseaseOrPhenotypicFeature'
DISEASE = 'biolink:Disease'
PHENOTYPIC_FEATURE = 'biolink:PhenotypicFeature'
MOLECULAR_ENTITY = 'biolink:MolecularEntity'
CHEMICAL_SUBSTANCE = 'biolink:ChemicalSubstance'
DRUG = 'biolink:Drug'
METABOLITE = 'biolink:Metabolite'
ANATOMICAL_ENTITY = 'biolink:AnatomicalEntity'
GENE = 'biolink:Gene'
GENE_PRODUCT = 'biolink:GeneProduct'
GENE_OR_GENE_PRODUCT = 'biolink:GeneOrGeneProduct'
SEQUENCE_VARIANT = 'biolink:SequenceVariant'
BIOLOGICAL_PROCESS_OR_ACTIVITY = 'biolink:BiologicalProcessOrActivity'
MOLECULAR_ACTIVITY = 'biolink:MolecularActivity'
BIOLOGICAL_PROCESS = 'biolink:BiologicalProcess'
PATHWAY = 'biolink:Pathway'
CELLULAR_COMPONENT = 'biolink:CellularComponent'
CELL = 'biolink:Cell'
GROSS_ANATOMICAL_STRUCTURE = 'biolink:GrossAnatomicalStructure'
GENETIC_CONDITION = 'biolink:GeneticCondition'
UNSPECIFIED = 'biolink:Unspecified'
GENE_FAMILY = 'biolink:GeneFamily'
GENOMIC_ENTITY = 'biolink:GenomicEntity'
FOOD = 'biolink:Food'
MACROMOLECULAR_COMPLEX = 'biolink:MacromolecularComplex'


# properties on edges
SUBJECT_ID = 'subject'
OBJECT_ID = 'object'
PREDICATE = 'predicate'
NODE_TYPES = 'category'
PRIMARY_KNOWLEDGE_SOURCE = 'biolink:primary_knowledge_source'
AGGREGATOR_KNOWLEDGE_SOURCES = 'biolink:aggregator_knowledge_source'
DESCRIPTION = 'description'
PUBLICATIONS = 'publications'
XREFS = 'xref'
P_VALUE = 'p_value'
ADJUSTED_P_VALUE = 'adjusted_p_value'
AGENT_TYPE = 'agent_type'
FDA_APPROVAL_STATUS = 'highest_FDA_approval_status'
KNOWLEDGE_LEVEL = 'knowledge_level'
MECHANISM_OF_ACTION = 'mechanism_of_action'

# these aren't in biolink but we use them on edges
AFFINITY = 'affinity'
AFFINITY_PARAMETER = 'affinity_parameter'
INFORMATION_CONTENT = 'information_content'


# edge qualifier properties
ANATOMICAL_CONTEXT_QUALIFIER = 'anatomical_context_qualifier'
CAUSAL_MECHANISM_QUALIFIER = 'causal_mechanism_qualifier'
CONTEXT_QUALIFIER = 'context_qualifier'
DERIVATIVE_QUALIFIER = 'derivative_qualifier'
OBJECT_ASPECT_QUALIFIER = 'object_aspect_qualifier'
OBJECT_DERIVATIVE_QUALIFIER = 'object_derivative_qualifier'
OBJECT_DIRECTION_QUALIFIER = 'object_direction_qualifier'
OBJECT_FORM_OR_VARIANT_QUALIFIER = 'object_form_or_variant_qualifier'
OBJECT_PART_QUALIFIER = 'object_part_qualifier'
QUALIFIED_PREDICATE = 'qualified_predicate'
SPECIES_CONTEXT_QUALIFIER = 'species_context_qualifier'
SUBJECT_ASPECT_QUALIFIER = 'subject_aspect_qualifier'
SUBJECT_DERIVATIVE_QUALIFIER = 'subject_derivative_qualifier'
SUBJECT_DIRECTION_QUALIFIER = 'subject_direction_qualifier'
SUBJECT_FORM_OR_VARIANT_QUALIFIER = 'subject_form_or_variant_qualifier'
SUBJECT_PART_QUALIFIER = 'subject_part_qualifier'


# this should probably be changed to a valid biolink synonym property but don't want to break downstream tools yet
SYNONYMS = 'equivalent_identifiers'

BIOLINK_EDGE_PROPERTIES = [
    SUBJECT_ID,
    OBJECT_ID,
    PREDICATE,
    NODE_TYPES,
    PRIMARY_KNOWLEDGE_SOURCE,
    AGGREGATOR_KNOWLEDGE_SOURCES,
    PUBLICATIONS,
    SYNONYMS,
    DESCRIPTION,
    XREFS,
    P_VALUE,
    ADJUSTED_P_VALUE,
    AGENT_TYPE,
    FDA_APPROVAL_STATUS,
    KNOWLEDGE_LEVEL,
    MECHANISM_OF_ACTION,
    # qualifiers
    ANATOMICAL_CONTEXT_QUALIFIER,
    CAUSAL_MECHANISM_QUALIFIER,
    CONTEXT_QUALIFIER,
    DERIVATIVE_QUALIFIER,
    OBJECT_ASPECT_QUALIFIER,
    OBJECT_DERIVATIVE_QUALIFIER,
    OBJECT_DIRECTION_QUALIFIER,
    OBJECT_FORM_OR_VARIANT_QUALIFIER,
    OBJECT_PART_QUALIFIER,
    QUALIFIED_PREDICATE,
    SPECIES_CONTEXT_QUALIFIER,
    SUBJECT_ASPECT_QUALIFIER,
    SUBJECT_DERIVATIVE_QUALIFIER,
    SUBJECT_DIRECTION_QUALIFIER,
    SUBJECT_FORM_OR_VARIANT_QUALIFIER,
    SUBJECT_PART_QUALIFIER,
]



