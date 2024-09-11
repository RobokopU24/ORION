from dataclasses import dataclass
from Common.biolink_constants import NAMED_THING
from Common.metadata import GraphMetadata
from Common.normalization import NormalizationScheme

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
                 primary_knowledge_source=None,
                 aggregator_knowledge_sources: list = None,
                 edgeprops=None):
        self.subjectid = subject_id
        self.objectid = object_id
        self.predicate = predicate
        self.primary_knowledge_source = primary_knowledge_source
        self.aggregator_knowledge_sources = aggregator_knowledge_sources
        if edgeprops:
            self.properties = edgeprops
        else:
            self.properties = {}


@dataclass
class GraphSpec:
    graph_id: str
    graph_name: str
    graph_description: str
    graph_url: str
    graph_version: str
    graph_output_format: str
    sources: list = None
    subgraphs: list = None

    def get_metadata_representation(self):
        return {
            'graph_id': self.graph_id,
            'graph_name': self.graph_name,
            'graph_description': self.graph_description,
            'graph_url': self.graph_url,
            'graph_version': self.graph_version,
            'subgraphs': [subgraph.get_metadata_representation() for subgraph in self.subgraphs] if self.subgraphs else [],
            'sources': [source.get_metadata_representation() for source in self.sources] if self.sources else []
        }


@dataclass
class GraphSource:
    id: str
    version: str = None
    merge_strategy: str = 'default'
    file_paths: list = None


@dataclass
class SubGraphSource(GraphSource):
    graph_metadata: GraphMetadata = None

    def get_metadata_representation(self):
        return {'graph_id': self.id,
                'release_version': self.version,
                'merge_strategy:': self.merge_strategy,
                'graph_metadata': self.graph_metadata.metadata if self.graph_metadata else None}


@dataclass
class DataSource(GraphSource):
    normalization_scheme: NormalizationScheme = None
    source_version: str = None
    parsing_version: str = None
    supplementation_version: str = None
    release_info: dict = None

    def get_metadata_representation(self):
        metadata = {'source_id': self.id,
                'source_version': self.source_version,
                'release_version': self.version,
                'parsing_version': self.parsing_version,
                'supplementation_version': self.supplementation_version,
                'normalization_scheme': self.normalization_scheme.get_metadata_representation(),
                'merge_strategy': self.merge_strategy}
        if self.release_info:
            metadata.update(self.release_info)
        return metadata


