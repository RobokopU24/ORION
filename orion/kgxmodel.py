from dataclasses import dataclass, field

from orion.biolink_constants import NAMED_THING
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION
from orion.kgx_metadata import source_ids_from_graph_metadata
from orion.metadata import get_source_build_version
from orion.normalization import NormalizationScheme


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
    # release_version is the human-facing semver, populated once chosen by GraphBuilder.determine_versions().
    # build_version is the deterministic hash of the graph's inputs. See orion/graph_versioning.py.
    # base_release_version is the release_version floor declared by the graph spec (via the
    # `base_release_version:` key in YAML).
    release_version: str = None
    graph_output_format: str = None
    build_version: str = None
    base_release_version: str = DEFAULT_BASE_RELEASE_VERSION
    add_edge_id: bool = None
    overwrite_edge_ids: bool = True
    edge_id_type: str = None
    edge_merging_attributes: list = None
    sources: list = None
    # Populated by the resolver chain in build_dependencies. Empty until then.
    resolved_sources: list = field(default_factory=list)

    def get_metadata_representation(self):
        return {
            'graph_id': self.graph_id,
            'graph_name': self.graph_name,
            'graph_description': self.graph_description,
            'graph_url': self.graph_url,
            'release_version': self.release_version,
            'build_version': self.build_version,
            'base_release_version': self.base_release_version,
            'edge_merging_attributes': self.edge_merging_attributes,
            'add_edge_id': self.add_edge_id,
            'overwrite_edge_ids': self.overwrite_edge_ids,
            'edge_id_type': self.edge_id_type,
            'sources': [source.get_metadata_representation() for source in self.sources] if self.sources else []
        }


# The sources list in a GraphSpec is made up of GraphSource entries. The id of a GraphSource must 
# refer to a parser in ORION or another graph available through GraphSpecs or a Graph Registry.
# 
# GraphSource entries can be either:
#  - pinned:  A parser or graph with a release_version or build_version specified. These can only be resolved by 
#             lookup, never built from scratch.
#  - recipe:  No release_version or build_version is specified, parameters like source_version / parsing_version / 
#             normalization_scheme are optionally set and otherwise programmatically determine the latest/current 
#             versions for these settings and derive the build version from those. Recipe settings only apply to 
#             parsers; an unpinned graph GraphSource will use it's own GraphSpec as its recipe.
@dataclass
class GraphSource:
    id: str
    merge_strategy: str = None
    release_version: str = None
    build_version: str = None
    normalization_scheme: NormalizationScheme = None
    source_version: str = None
    parsing_version: str = None
    supplementation_version: str = None

    # The deterministic source-build hash identifying a recipe entry's contribution. Returns the
    # explicit build_version when pinned by hash; otherwise computes it from the recipe, or None
    # when source_version hasn't been resolved yet (callers fill it in lazily before asking).
    # Uses get_source_build_version so the value matches what the source ingest pipeline computes.
    def generate_build_version(self):
        if self.build_version is not None:
            return self.build_version
        if self.source_version is None:
            return None
        return get_source_build_version(self.id,
                                        self.source_version,
                                        self.parsing_version,
                                        self.normalization_scheme.get_composite_normalization_version(),
                                        self.supplementation_version)

    def get_metadata_representation(self):
        return {
            'id': self.id,
            'release_version': self.release_version,
            'build_version': self.build_version,
            'source_version': self.source_version,
            'parsing_version': self.parsing_version,
            'supplementation_version': self.supplementation_version,
            'normalization_scheme': self.normalization_scheme.get_metadata_representation()
                                    if self.normalization_scheme else None,
            'merge_strategy': self.merge_strategy,
        }


@dataclass
class GraphFileSource:
    """Resolved merge input: a built KGX graph (or raw parser output) on disk.

    After build_dependencies populates GraphSpec.resolved_sources, every entry
    is one of these — regardless of how it was obtained (already on disk,
    pulled from the registry, built as a subgraph, or just-materialized from
    an ingest pipeline run).

    Exactly one of release_version / build_version is set, depending on what
    this source represents:
    - release_version (semver): for built graphs and subgraphs.
    - build_version (hash):     for raw parser output coming straight from
                                the ingest pipeline, or for a single-source
                                graph matched by its source's build hash.
    """
    id: str
    file_paths: list
    merge_strategy: str = None
    release_version: str = None
    build_version: str = None
    kgx_graph_metadata: dict = None

    @property
    def version_identifier(self) -> str | None:
        """release_version when set (built graphs / subgraphs), else build_version (raw parser output)."""
        return self.release_version or self.build_version

    def get_node_file_paths(self):
        if self.file_paths is None:
            raise Exception(f'File paths were requested before they were established for GraphFileSource {self.id}')
        return [file_path for file_path in self.file_paths if 'node' in file_path]

    def get_edge_file_paths(self):
        if self.file_paths is None:
            raise Exception(f'File paths were requested before they were established for GraphFileSource {self.id}')
        return [file_path for file_path in self.file_paths if 'edge' in file_path]

    def get_constituent_source_ids(self) -> list[str]:
        """The set of underlying source_ids represented by these files."""
        if self.kgx_graph_metadata is not None:
            ids = source_ids_from_graph_metadata(self.kgx_graph_metadata)
            if ids:
                return ids
        return [self.id]

    def get_metadata_representation(self):
        return {
            'id': self.id,
            'release_version': self.release_version,
            'build_version': self.build_version,
            'merge_strategy': self.merge_strategy,
        }