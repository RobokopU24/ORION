from dataclasses import dataclass, InitVar
from typing import Callable
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
    source_version: InitVar[str] = None
    parsing_version: str = None
    supplementation_version: str = None
    release_info: dict = None

    # This function serves as an optional way to provide a callable function which can determine the source version,
    # instead of setting it during initialization. This is used like lazy initialization, because determining the
    # source version of a data source can be expensive and error-prone, and we don't want to do it if we don't need to.
    get_source_version: InitVar[Callable[[str], str]] = None
    _source_version: str = None
    _get_source_version: Callable[[str], str] = None

    def __post_init__(self, source_version, get_source_version):
        self._get_source_version = get_source_version
        # if a source_version is provided in initialization, just store that and return it
        if source_version:
            self._source_version = source_version
        # if neither the source version nor a function to determine it is provided, throw an error
        if not source_version and not get_source_version:
            raise Exception(f'Invalid DataSource initialization - '
                            f'source_version or get_source_version must be provided.')

    # when
    def __getattribute__(self, name):
        if name == "source_version":
            if not self._source_version:
                self._source_version = self._get_source_version(self.id)
            return self._source_version
        else:
            return object.__getattribute__(self, name)

    def get_metadata_representation(self):
        metadata = {'source_id': self.id,
                    'source_version': self.source_version,  # this may produce an IDE warning but it's right
                    'release_version': self.version,
                    'parsing_version': self.parsing_version,
                    'supplementation_version': self.supplementation_version,
                    'normalization_scheme': self.normalization_scheme.get_metadata_representation(),
                    'merge_strategy': self.merge_strategy}
        if self.release_info:
            metadata.update(self.release_info)
        return metadata


