# A collection of constants for biolink variable names and types
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

# The root of all biolink_model entities
ROOT_ENTITY = NAMED_THING

# a property name for listing node types that did not normalize
CUSTOM_NODE_TYPES = 'custom_node_types'

node_types = [
    NAMED_THING,
    BIOLOGICAL_ENTITY,
    DISEASE_OR_PHENOTYPIC_FEATURE,
    DISEASE,
    PHENOTYPIC_FEATURE,
    MOLECULAR_ENTITY,
    CHEMICAL_SUBSTANCE,
    DRUG,
    METABOLITE,
    ANATOMICAL_ENTITY,
    GENE,
    SEQUENCE_VARIANT,
    BIOLOGICAL_PROCESS_OR_ACTIVITY,
    MOLECULAR_ACTIVITY,
    BIOLOGICAL_PROCESS,
    PATHWAY,
    CELLULAR_COMPONENT,
    CELL,
    GROSS_ANATOMICAL_STRUCTURE,
    GENETIC_CONDITION,
    UNSPECIFIED,
    GENE_FAMILY,
    FOOD
]

# The following are used by edges:
SUBJECT_ID = 'subject'
OBJECT_ID = 'object'
PREDICATE = 'predicate'
NODE_TYPES = 'category'
SYNONYMS = 'equivalent_identifiers'
INFORMATION_CONTENT = 'information_content'

FALLBACK_EDGE_PREDICATE = 'biolink:related_to'

PRIMARY_KNOWLEDGE_SOURCE = 'biolink:primary_knowledge_source'
AGGREGATOR_KNOWLEDGE_SOURCES = 'biolink:aggregator_knowledge_source'
PUBLICATIONS = 'publications'
AFFINITY = 'affinity'
AFFINITY_PARAMETER = 'affinityParameter'
XREFS = 'xref'

