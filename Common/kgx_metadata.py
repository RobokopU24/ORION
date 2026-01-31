
import json
from collections import defaultdict
from dataclasses import dataclass, field

from Common.utils import quick_jsonl_file_iterator
from Common.biolink_utils import BiolinkUtils
from Common.biolink_constants import *


@dataclass
class KGXSource:
    id: str = ""
    name: str = ""
    description: str = ""
    license: str = ""
    attribution: str = ""
    url: str = ""
    citation: str = ""
    fullCitation: str = ""
    version: str = ""
    contentUrl: str = ""

    def to_dict(self):
        return {
            "@type": "Dataset",
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "license": self.license,
            "attribution": self.attribution,
            "url": self.url,
            "citation": self.citation,
            "fullCitation": self.fullCitation,
            "version": self.version,
            "contentUrl": self.contentUrl
        }

@dataclass
class KGXNodeType:
    categories: list = None
    id_prefixes: defaultdict = field(default_factory=lambda: defaultdict(int))
    attributes: defaultdict = field(default_factory=lambda: defaultdict(int))

@dataclass
class KGXEdgeType:
    subject_categories: list = None
    predicate: str = None
    object_categories: list = None
    primary_knowledge_sources: defaultdict = field(default_factory=lambda: defaultdict(int))
    qualifiers: defaultdict = field(default_factory=lambda: defaultdict(int))
    attributes: defaultdict = field(default_factory=lambda: defaultdict(int))
    subject_id_prefixes: defaultdict = field(default_factory=lambda: defaultdict(int))
    object_id_prefixes: defaultdict = field(default_factory=lambda: defaultdict(int))


@dataclass
class KGXGraphMetadata:
    # Graph-level metadata (schema.org Dataset)
    id: str = ""
    name: str = ""
    description: str = ""
    license: str = ""
    url: str = ""
    version: str = ""
    date_created: str = ""
    date_published: str = ""
    date_modified: str = ""
    biolink_version: str = ""
    babel_version: str = ""
    keywords: list[str] = field(default_factory=list)
    creator: list[dict] = field(default_factory=list)
    contact_point: list[dict] = field(default_factory=list)
    funder: list[dict] = field(default_factory=list)
    conforms_to: list[dict] = field(default_factory=list)
    schema: dict = field(default_factory=dict)
    kgx_sources: list[KGXSource] = field(default_factory=list)
    distribution_file_format: str = "tar.xz"
    distribution_encoding_format: str = "application/x-xz"

    def to_json(self):
        result = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "https://mlcommons.org/croissant/"
            },
            "@id": self.id,
            "@type": "Dataset",
            "name": self.name,
            "description": self.description,
            "license": self.license,
            "url": self.url,
            "version": self.version,
            "dateCreated": self.date_created,
            "datePublished": self.date_published,
            "dateModified": self.date_modified,
            "keywords": self.keywords,
            "creator": self.creator,
            "contactPoint": self.contact_point,
            "funder": self.funder,
            "conformsTo": self.conforms_to,
            "schema": self.schema,
            "biolinkVersion": self.biolink_version,
            "babelVersion": self.babel_version,
            "distribution": [{
                "@id": f"{self.name}.{self.distribution_file_format}",
                "@type": "cr:FileObject",
                "contentUrl": f"{self.name}.{self.distribution_file_format}",
                "encodingFormat": self.distribution_encoding_format,
                "description": "Compressed tar archive containing the KGX files: "
                               "nodes.jsonl, edges.jsonl, and graph-metadata.json"
            }],
            "isBasedOn": [source.to_dict() for source in self.kgx_sources]
        }
        return json.dumps(result, indent=2)


@dataclass
class KGXSchema:
    # Schema document metadata (schema.org DigitalDocument)
    id: str = ""
    name: str = ""
    description: str = ""
    encoding_format: str = "application/ld+json"
    kg_id: str = ""
    kg_name: str = ""
    schema: dict = field(default_factory=dict)

    def to_json(self):
        result = {
            "@context": {
                "@vocab": "https://schema.org/",
                "biolink": "https://w3id.org/biolink/"
            },
            "@type": "DigitalDocument",
            "@id": self.id,
            "name": self.name,
            "description": self.description,
            "encodingFormat": self.encoding_format,
            "isPartOf": {
                "@type": "Dataset",
                "@id": self.kg_id,
                "name": self.kg_name
            },
            "schema": self.schema
        }
        return json.dumps(result, indent=2)


def prepare_to_serialize_nodes(nodes):
    nodes_list = []
    for node_categories, kgx_node_type in nodes.items():
        nodes_list.append({
            NODE_TYPES: list(node_categories),
            "count": sum(kgx_node_type.id_prefixes.values()),
            "id_prefixes": sort_dict_by_values(kgx_node_type.id_prefixes),
            "attributes": sort_dict_by_values(kgx_node_type.attributes),
        })
    return nodes_list

def generate_nodes_summary(nodes):
    # Aggregate statistics across all node types
    total_count = 0
    aggregated_id_prefixes = defaultdict(int)
    aggregated_attributes = defaultdict(int)
    for node_categories, kgx_node_type in nodes.items():
        total_count += sum(kgx_node_type.id_prefixes.values())
        for prefix, count in kgx_node_type.id_prefixes.items():
            aggregated_id_prefixes[prefix] += count
        for attribute, count in kgx_node_type.attributes.items():
            aggregated_attributes[attribute] += count
    return {
        "total_count": total_count,
        "id_prefixes": sort_dict_by_values(aggregated_id_prefixes),
        "attributes": sort_dict_by_values(aggregated_attributes)
    }

def prepare_to_serialize_edges(edges):
    edges_list = []
    for (subject_categories, predicate, object_categories), kgx_edge_type in edges.items():
        edges_list.append({
            "subject_category": list(subject_categories),
            PREDICATE: predicate,
            "object_category": list(object_categories),
            "count": sum(kgx_edge_type.primary_knowledge_sources.values()),
            "primary_knowledge_sources": sort_dict_by_values(kgx_edge_type.primary_knowledge_sources),
            "qualifiers": sort_dict_by_values(kgx_edge_type.qualifiers),
            "attributes": sort_dict_by_values(kgx_edge_type.attributes),
            "subject_id_prefixes": sort_dict_by_values(kgx_edge_type.subject_id_prefixes),
            "object_id_prefixes": sort_dict_by_values(kgx_edge_type.object_id_prefixes)
        })
    return edges_list

def generate_edges_summary(edges):
    # Aggregate statistics across all edge types
    total_count = 0
    aggregated_primary_knowledge_sources = defaultdict(int)
    aggregated_predicates = defaultdict(int)
    aggregated_predicates_by_ks = defaultdict(lambda: defaultdict(int))
    aggregated_qualifiers = defaultdict(int)
    aggregated_attributes = defaultdict(int)

    for (subject_categories, predicate, object_categories), kgx_edge_type in edges.items():
        edge_count = sum(kgx_edge_type.primary_knowledge_sources.values())
        total_count += edge_count
        aggregated_predicates[predicate] += edge_count

        for ks, count in kgx_edge_type.primary_knowledge_sources.items():
            aggregated_primary_knowledge_sources[ks] += count
            aggregated_predicates_by_ks[ks][predicate] += count

        for qualifier, count in kgx_edge_type.qualifiers.items():
            aggregated_qualifiers[qualifier] += count

        for attribute, count in kgx_edge_type.attributes.items():
            aggregated_attributes[attribute] += count

    return {
        "total_count": total_count,
        "predicates": sort_dict_by_values(aggregated_predicates),
        "primary_knowledge_sources": sort_dict_by_values(aggregated_primary_knowledge_sources),
        "predicates_by_knowledge_source": {ks: sort_dict_by_values(preds)
                                            for ks, preds in aggregated_predicates_by_ks.items()},
        "qualifiers": sort_dict_by_values(aggregated_qualifiers),
        "attributes": sort_dict_by_values(aggregated_attributes),
    }


def sort_dict_by_values(dict_to_sort):
    return dict(sorted(dict_to_sort.items(), key=lambda item_tuple: item_tuple[1], reverse=True))


def generate_schema(nodes_file_path: str,
                    edges_file_path: str,
                    biolink_version: str):

    bl_utils = BiolinkUtils(biolink_version=biolink_version)

    # Nodes
    nodes = defaultdict(KGXNodeType)
    node_categories_lookup = {}
    for node in quick_jsonl_file_iterator(nodes_file_path):

        # organize nodes by their leaf categories
        # (remove categories that are parents of other categories for that node)
        node_categories = bl_utils.find_biolink_leaves(frozenset(node[NODE_TYPES]))

        # count the number of nodes with each curie prefix
        node_curie = node['id']
        nodes[node_categories].id_prefixes[node_curie.partition(':')[0]] += 1

        # count the number of occurrences of each attribute
        for attribute in node.keys():
            if attribute not in ["id", NODE_TYPES]:
                nodes[node_categories].attributes[attribute] += 1

        # store the categories of each node for lookup during edge analysis
        node_categories_lookup[node_curie] = node_categories

    # Edges
    core_attributes = {SUBJECT_ID, PREDICATE, OBJECT_ID, PRIMARY_KNOWLEDGE_SOURCE, "sources"}
    edges = defaultdict(KGXEdgeType)
    for edge in quick_jsonl_file_iterator(edges_file_path):
        # get references to some edge properties
        subject_id = edge[SUBJECT_ID]
        object_id = edge[OBJECT_ID]
        predicate = edge[PREDICATE]
        subject_categories = node_categories_lookup[subject_id]
        object_categories = node_categories_lookup[object_id]
        primary_ks = edge.get(PRIMARY_KNOWLEDGE_SOURCE)
        if primary_ks is None:
            for retrieval_source in edge.get("sources", []):
                if retrieval_source["resource_role"] == PRIMARY_KNOWLEDGE_SOURCE:
                    primary_ks = retrieval_source["resource_id"]
        # TODO - is it worth handling aggregators?
        #  it's much more complicated with the "sources" attribute because they are not necessarily sequential

        # get a reference to the KGXEdgeType object for this type of triple
        edge_type = edges[(subject_categories, predicate, object_categories)]

        edge_type.primary_knowledge_sources[primary_ks] += 1
        edge_type.subject_id_prefixes[subject_id.partition(':')[0]] += 1
        edge_type.object_id_prefixes[object_id.partition(':')[0]] += 1

        for key, value in edge.items():
            if key in core_attributes:
                continue
            if bl_utils.is_qualifier(key):
                # TODO do we need to record possible values for qualifiers?
                #  for some of them that could be a huge id space
                edge_type.qualifiers[key] += 1
            else:
                edge_type.attributes[key] += 1

    return {"nodes": prepare_to_serialize_nodes(nodes),
            "nodes_summary": generate_nodes_summary(nodes),
            "edges": prepare_to_serialize_edges(edges),
            "edges_summary": generate_edges_summary(edges)}