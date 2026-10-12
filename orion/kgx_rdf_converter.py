import gzip
import json
import math
import os
from collections import Counter
from urllib.parse import quote
from typing import Callable

from linkml_runtime.linkml_model.meta import SlotDefinition

from orion.biolink_constants import (
    NODE_ID, NODE_TYPES, NAME, EDGE_ID, SUBJECT_ID, PREDICATE, OBJECT_ID, NEGATED,
    PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES, SUPPORTING_DATA_SOURCE, PUBLICATIONS, XREFS,
    SYNONYMS, QUALIFIED_PREDICATE, SPECIES_CONTEXT_QUALIFIER, ANATOMICAL_CONTEXT_QUALIFIER,
    DISEASE_CONTEXT_QUALIFIER, TAXON, RETRIEVAL_SOURCE_ID, ORIGINAL_SUBJECT, ORIGINAL_OBJECT,
)
from orion.biolink_utils import get_biolink_model_toolkit, get_biolink_prefix_map
from orion.logging import get_orion_logger
from orion.utils import quick_jsonl_file_iterator

logger = get_orion_logger("orion.kgx_rdf_converter")

# Converts KGX jsonl nodes/edges into RDF 1.2 N-Triples. Each edge becomes an asserted triple plus a reifier
# (rdf:reifies <<( s p o )>>) that carries the edge properties. The reifier is an IRI minted from the edge id when
# the edge has one, and a blank node otherwise. Negated edges are reified but not asserted.

# encodingFormat for the RDF distribution entry in graph-metadata.json
RDF_ENCODING_FORMAT = 'application/n-triples'

RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
RDFS_NS = 'http://www.w3.org/2000/01/rdf-schema#'
XSD_NS = 'http://www.w3.org/2001/XMLSchema#'
BIOLINK_NS = 'https://w3id.org/biolink/vocab/'
# namespace for node and edge properties that are not Biolink slots
ORION_NS = 'https://w3id.org/orion/vocab/'
# namespace for edge reifier IRIs, followed by the percent-encoded edge id
ORION_EDGE_NS = 'https://w3id.org/orion/edge/'

RDF_TYPE = f'<{RDF_NS}type>'
RDF_REIFIES = f'<{RDF_NS}reifies>'
RDFS_LABEL = f'<{RDFS_NS}label>'
XSD_BOOLEAN = f'<{XSD_NS}boolean>'
XSD_INTEGER = f'<{XSD_NS}integer>'
XSD_DOUBLE = f'<{XSD_NS}double>'

# Prefixes ORION data uses that the Biolink prefix map does not cover. infores follows Bioregistry.
RDF_PREFIX_SUPPLEMENTS = {
    'infores': 'https://w3id.org/information-resource-registry/',
}

# Properties whose values are CURIEs, written as IRIs. Values of all other properties are written as literals.
CURIE_VALUED_PROPERTIES = frozenset({
    PRIMARY_KNOWLEDGE_SOURCE,
    AGGREGATOR_KNOWLEDGE_SOURCES,
    SUPPORTING_DATA_SOURCE,
    PUBLICATIONS,
    XREFS,
    SYNONYMS,  # equivalent_identifiers
    QUALIFIED_PREDICATE,
    SPECIES_CONTEXT_QUALIFIER,
    ANATOMICAL_CONTEXT_QUALIFIER,
    DISEASE_CONTEXT_QUALIFIER,
    TAXON,
    ORIGINAL_SUBJECT,
    ORIGINAL_OBJECT,
    RETRIEVAL_SOURCE_ID,
    'upstream_resource_ids',
    'in_taxon',
    'has_supporting_study_result',
})

# Characters not allowed in an N-Triples IRIREF, percent-encoded when they appear in a CURIE local id.
_IRI_UNSAFE_CHARS = {c: f'%{ord(c):02X}' for c in '<>"{}|^`\\'}
_IRI_UNSAFE_CHARS.update({chr(i): f'%{i:02X}' for i in range(0x21)})
_IRI_ESCAPE_TABLE = str.maketrans(_IRI_UNSAFE_CHARS)

_LITERAL_ESCAPE_TABLE = str.maketrans({'\\': '\\\\', '"': '\\"', '\n': '\\n', '\r': '\\r'})


class _TermMapper:
    """Maps KGX identifiers, property names, and values to N-Triples terms, counting values it can't map."""

    def __init__(self, prefix_map: dict, is_biolink_slot: Callable[[str], bool]):
        self.prefix_map = prefix_map
        self.is_biolink_slot = is_biolink_slot
        self.property_iris = {}
        self.unmapped_prefixes = Counter()

    def curie_to_iri(self, curie: str):
        if curie.startswith('http://') or curie.startswith('https://'):
            return f'<{curie.translate(_IRI_ESCAPE_TABLE)}>'
        prefix, sep, local_id = curie.partition(':')
        namespace = self.prefix_map.get(prefix) if sep else None
        if namespace is None:
            self.unmapped_prefixes[prefix if sep else '(no prefix)'] += 1
            return None
        return f'<{namespace}{local_id.translate(_IRI_ESCAPE_TABLE)}>'

    def property_iri(self, prop: str):
        iri = self.property_iris.get(prop)
        if iri is None:
            namespace = BIOLINK_NS if self.is_biolink_slot(prop) else ORION_NS
            iri = f'<{namespace}{prop.translate(_IRI_ESCAPE_TABLE)}>'
            self.property_iris[prop] = iri
        return iri

    @staticmethod
    def literal(value):
        if isinstance(value, bool):
            return f'"{"true" if value else "false"}"^^{XSD_BOOLEAN}'
        if isinstance(value, int):
            return f'"{value}"^^{XSD_INTEGER}'
        if isinstance(value, float):
            if math.isnan(value):
                lexical = 'NaN'
            elif math.isinf(value):
                lexical = 'INF' if value > 0 else '-INF'
            else:
                lexical = repr(value)
            return f'"{lexical}"^^{XSD_DOUBLE}'
        if not isinstance(value, str):
            value = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
        return f'"{value.translate(_LITERAL_ESCAPE_TABLE)}"'

    def value_term(self, prop: str, value):
        # CURIE-valued properties become IRIs; a value that can't be expanded falls back to a string literal
        if prop in CURIE_VALUED_PROPERTIES and isinstance(value, str):
            iri = self.curie_to_iri(value)
            if iri is not None:
                return iri
        return self.literal(value)


def _write_properties(out, mapper: _TermMapper, subject: str, properties: dict, bnode_prefix: str):
    """Write one triple per property value. Lists become repeated triples, dicts become nested blank nodes."""
    nested_count = 0
    for prop, value in properties.items():
        if value is None:
            continue
        prop_iri = mapper.property_iri(prop)
        for item in (value if isinstance(value, list) else [value]):
            if item is None:
                continue
            if isinstance(item, dict):
                nested_bnode = f'{bnode_prefix}_{nested_count}'
                nested_count += 1
                out.write(f'{subject} {prop_iri} {nested_bnode} .\n')
                _write_properties(out, mapper, nested_bnode, item, nested_bnode)
            else:
                out.write(f'{subject} {prop_iri} {mapper.value_term(prop, item)} .\n')


def _write_nodes(out, mapper: _TermMapper, nodes_file: str, property_ignore_list: set):
    written = skipped = 0
    for node_index, node in enumerate(quick_jsonl_file_iterator(nodes_file)):
        node_iri = mapper.curie_to_iri(node[NODE_ID])
        if node_iri is None:
            skipped += 1
            continue
        categories = node.get(NODE_TYPES) or []
        for category in (categories if isinstance(categories, list) else [categories]):
            category_iri = mapper.curie_to_iri(category)
            if category_iri is not None:
                out.write(f'{node_iri} {RDF_TYPE} {category_iri} .\n')
        if node.get(NAME):
            out.write(f'{node_iri} {RDFS_LABEL} {mapper.literal(node[NAME])} .\n')
        properties = {k: v for k, v in node.items()
                      if k not in (NODE_ID, NODE_TYPES, NAME) and k not in property_ignore_list}
        _write_properties(out, mapper, node_iri, properties, f'_:n{node_index}')
        written += 1
    return written, skipped


def _write_edges(out, mapper: _TermMapper, edges_file: str, property_ignore_list: set):
    written = skipped = negated = 0
    for edge_index, edge in enumerate(quick_jsonl_file_iterator(edges_file)):
        subject_iri = mapper.curie_to_iri(edge[SUBJECT_ID])
        predicate_iri = mapper.curie_to_iri(edge[PREDICATE])
        object_iri = mapper.curie_to_iri(edge[OBJECT_ID])
        if subject_iri is None or predicate_iri is None or object_iri is None:
            skipped += 1
            continue
        triple = f'{subject_iri} {predicate_iri} {object_iri}'
        # a negated edge states the triple is false, so it is reified without being asserted
        if edge.get(NEGATED) is True:
            negated += 1
        else:
            out.write(f'{triple} .\n')
        edge_id = edge.get(EDGE_ID)
        reifier = f'<{ORION_EDGE_NS}{quote(str(edge_id), safe=":")}>' if edge_id else f'_:e{edge_index}'
        out.write(f'{reifier} {RDF_REIFIES} <<( {triple} )>> .\n')
        properties = {k: v for k, v in edge.items()
                      if k not in (SUBJECT_ID, PREDICATE, OBJECT_ID) and k not in property_ignore_list}
        _write_properties(out, mapper, reifier, properties, f'_:e{edge_index}')
        written += 1
    return written, skipped, negated


def load_rdf_prefix_map(biolink_version: str = None) -> dict:
    prefix_map = get_biolink_prefix_map(biolink_version=biolink_version)
    for prefix, namespace in RDF_PREFIX_SUPPLEMENTS.items():
        prefix_map.setdefault(prefix, namespace)
    return prefix_map


def biolink_slot_checker(biolink_version: str = None) -> Callable[[str], bool]:
    """Return a function reporting whether a KGX property name is exactly the name of a Biolink slot."""
    toolkit = get_biolink_model_toolkit(biolink_version=biolink_version)
    cache = {}

    def is_biolink_slot(prop: str) -> bool:
        if prop not in cache:
            # get_element also matches aliases and mappings, so the slot name has to match the property itself
            element = toolkit.get_element(prop)
            cache[prop] = (isinstance(element, SlotDefinition)
                           and element.name.replace(' ', '_') == prop)
        return cache[prop]
    return is_biolink_slot


def convert_kgx_to_rdf(nodes_input_file: str,
                       edges_input_file: str,
                       output_file: str,
                       biolink_version: str = None,
                       prefix_map: dict = None,
                       is_biolink_slot: Callable[[str], bool] = None,
                       node_property_ignore_list: set = None,
                       edge_property_ignore_list: set = None):
    """
    Convert KGX jsonl nodes and edges files into a gzipped RDF 1.2 N-Triples file.
    :param nodes_input_file: path to nodes jsonl file (optionally gzipped)
    :param edges_input_file: path to edges jsonl file (optionally gzipped)
    :param output_file: path to the output .nt.gz file
    :param biolink_version: Biolink version used for the prefix map and slot lookups
    :param prefix_map: CURIE prefix to IRI namespace map, loaded from Biolink when not provided
    :param is_biolink_slot: function deciding whether a property is a Biolink slot, uses bmt when not provided
    :param node_property_ignore_list: node properties to leave out
    :param edge_property_ignore_list: edge properties to leave out
    """
    if not output_file.endswith('.nt.gz'):
        raise ValueError(f'RDF output file must end with .nt.gz: {output_file}')
    if prefix_map is None:
        prefix_map = load_rdf_prefix_map(biolink_version)
    if is_biolink_slot is None:
        is_biolink_slot = biolink_slot_checker(biolink_version)
    mapper = _TermMapper(prefix_map, is_biolink_slot)

    # write to a partial file and rename on success, so an existing output file is always complete
    partial_file = f'{output_file}.partial'
    with gzip.open(partial_file, 'wt', encoding='utf-8', compresslevel=6) as out:
        nodes_written, nodes_skipped = _write_nodes(out, mapper, nodes_input_file,
                                                    node_property_ignore_list or set())
        edges_written, edges_skipped, edges_negated = _write_edges(out, mapper, edges_input_file,
                                                                   edge_property_ignore_list or set())
    os.replace(partial_file, output_file)

    logger.info(f'RDF conversion wrote {nodes_written} nodes and {edges_written} edges '
                f'({edges_negated} negated edges reified without being asserted) to {output_file}.')
    if nodes_skipped or edges_skipped:
        logger.warning(f'RDF conversion skipped {nodes_skipped} nodes and {edges_skipped} edges '
                       f'whose identifiers could not be expanded to IRIs.')
    if mapper.unmapped_prefixes:
        logger.warning(f'CURIE prefixes with no IRI mapping (occurrences): '
                       f'{dict(mapper.unmapped_prefixes.most_common())}')
    return {'nodes_written': nodes_written,
            'nodes_skipped': nodes_skipped,
            'edges_written': edges_written,
            'edges_skipped': edges_skipped,
            'edges_negated': edges_negated,
            'unmapped_prefixes': dict(mapper.unmapped_prefixes)}
