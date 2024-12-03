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

# properties on nodes
NODE_ID = 'id'
NODE_TYPES = 'category'
NAME = 'name'
SYNONYM = 'synonym'
TRADE_NAME = 'trade_name'
CHEMICAL_ROLE = 'chemical_role'
HAS_CHEMICAL_FORMULA = 'has_chemical_formula'
IN_TAXON = 'in_taxon'
ROUTES_OF_DELIVERY = 'routes_of_delivery'
SYMBOL = 'symbol'


# properties on edges
EDGE_ID = 'id'
SUBJECT_ID = 'subject'
OBJECT_ID = 'object'
PREDICATE = 'predicate'
PRIMARY_KNOWLEDGE_SOURCE = 'primary_knowledge_source'
AGGREGATOR_KNOWLEDGE_SOURCES = 'aggregator_knowledge_source'
SUPPORTING_DATA_SOURCE = 'supporting_data_source'
P_VALUE = 'p_value'
ADJUSTED_P_VALUE = 'adjusted_p_value'
AGENT_TYPE = 'agent_type'
KNOWLEDGE_LEVEL = 'knowledge_level'
MAX_RESEARCH_PHASE = 'max_research_phase'
HAS_SUPPORTING_STUDY_RESULT = 'has_supporting_study_result'

# enums for knowledge level
KNOWLEDGE_ASSERTION = 'knowledge_assertion'
LOGICAL_ENTAILMENT = 'logical_entailment'
PREDICATION = 'prediction'
STATISTICAL_ASSOCIATION = 'statistical_association'
OBSERVATION = 'observation'
NOT_PROVIDED = 'not_provided'

# enums for agent type
MANUAL_AGENT = 'manual_agent'
AUTOMATED_AGENT = 'automated_agent'
DATA_PIPELINE = 'data_analysis_pipeline'
COMPUTATIONAL_MODEL = 'computational_model'
TEXT_MINING_AGENT = 'text_mining_agent'
IMAGE_PROCESSING_AGENT = 'image_processing_agent'
MANUAL_VALIDATION_OF_AUTOMATED_AGENT = 'manual_validation_of_automated_agent'

# properties that could be on edges or nodes (I think?)
DESCRIPTION = 'description'
PUBLICATIONS = 'publications'
XREFS = 'xref'

FDA_APPROVAL_STATUS = 'highest_FDA_approval_status'
MECHANISM_OF_ACTION = 'mechanism_of_action'

# these aren't in biolink, but we use them on edges
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


# this should probably be changed to the valid biolink synonym property but don't want to break downstream tools yet
SYNONYMS = 'equivalent_identifiers'

BIOLINK_NODE_PROPERTIES = [
    NODE_ID,
    NODE_TYPES,
    NAME,
    DESCRIPTION,
    PUBLICATIONS,
    XREFS,
    SYNONYM,
    TRADE_NAME,
    CHEMICAL_ROLE,
    HAS_CHEMICAL_FORMULA,
    FDA_APPROVAL_STATUS,
    MECHANISM_OF_ACTION,
    IN_TAXON,
    ROUTES_OF_DELIVERY,
    SYMBOL
]

REQUIRED_NODE_PROPERTIES = [
    NODE_ID,
    NODE_TYPES,
    NAME
]

BIOLINK_EDGE_PROPERTIES = [
    EDGE_ID,
    SUBJECT_ID,
    OBJECT_ID,
    PREDICATE,
    PRIMARY_KNOWLEDGE_SOURCE,
    AGGREGATOR_KNOWLEDGE_SOURCES,
    SUPPORTING_DATA_SOURCE,
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
    MAX_RESEARCH_PHASE,
    HAS_SUPPORTING_STUDY_RESULT,
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

REQUIRED_EDGE_PROPERTIES = [
    SUBJECT_ID,
    OBJECT_ID,
    PREDICATE,
    PRIMARY_KNOWLEDGE_SOURCE
]

BIOLINK_PROPERTIES_THAT_ARE_LISTS = [
    SYNONYMS,
    SYNONYM,
    NODE_TYPES,
    AGGREGATOR_KNOWLEDGE_SOURCES,
    PUBLICATIONS,
    XREFS
]

# biolink compliant predicates
SUBCLASS_OF = 'biolink:subclass_of'