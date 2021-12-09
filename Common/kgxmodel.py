from dataclasses import dataclass
from Common.node_types import NAMED_THING


class kgxnode:
    def __init__(self,
                 identifier,
                 name='',
                 categories=None,
                 nodeprops=None):
        self.identifier = identifier
        self.name = name
        self.categories = categories if categories else [NAMED_THING]
        self.properties = nodeprops if nodeprops else {}


class kgxedge:
    def __init__(self,
                 subject_id,
                 object_id,
                 predicate=None,
                 relation=None,
                 original_knowledge_source=None,
                 primary_knowledge_source=None,
                 aggregator_knowledge_sources=None,
                 edgeprops=None):
        self.subjectid = subject_id
        self.objectid = object_id
        self.relation = relation
        self.predicate = predicate
        self.original_knowledge_source = original_knowledge_source
        self.primary_knowledge_source = primary_knowledge_source
        self.aggregator_knowledge_sources = aggregator_knowledge_sources
        if edgeprops:
            self.properties = edgeprops
        else:
            self.properties = {}


KNOWLEDGE_SOURCE = "knowledge_source"
SUBGRAPH = "sub_graph"

@dataclass
class GraphSpec:
    graph_id: str
    graph_version: str
    graph_output_format: str
    sources: list

    def get_metadata_representation(self):
        return {
            'graph_id': self.graph_id,
            'graph_version': self.graph_version,
            'graph_output_format': self.graph_output_format,
            'sources': [{'source_id': source.source_id,
                         'source_type': source.source_type,
                         'source_version': source.source_version,
                         'parsing_version': source.parsing_version,
                         'node_normalization_version': source.node_normalization_version,
                         'edge_normalization_version': source.edge_normalization_version,
                         'strict_normalization': source.strict_normalization,
                         'merge_strategy': source.merge_strategy} for source in self.sources]
        }



@dataclass
class GraphSource:
    source_id: str
    source_type: str
    source_version: str
    parsing_version: str = 'latest'
    node_normalization_version: str = 'latest'
    edge_normalization_version: str = 'latest'
    strict_normalization: bool = True
    merge_strategy: str = 'default'
    file_paths: list = None

