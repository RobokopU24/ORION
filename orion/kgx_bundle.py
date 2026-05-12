"""KGX graph bundle on disk: nodes, edges, graph-metadata, schema.

`KGXBundle` provides utilities for one KGX graph (nodes and edges files) and its metadata.
It manages the files - resolving paths (handling .gz or not), checking which files are
present, and loading the JSON metadata files.
"""

import gzip
import json
import os
import shutil


class KGXBundle:

    NODES_FILENAME = 'nodes.jsonl'
    EDGES_FILENAME = 'edges.jsonl'
    GRAPH_METADATA_FILENAME = 'graph-metadata.json'
    SCHEMA_FILENAME = 'schema.json'

    def __init__(self, graph_dir: str):
        self.graph_dir = graph_dir

    @property
    def nodes_path(self) -> str | None:
        return self._get_path(self.NODES_FILENAME)

    @property
    def edges_path(self) -> str | None:
        return self._get_path(self.EDGES_FILENAME)

    @property
    def graph_metadata_path(self) -> str:
        return self._get_path(self.GRAPH_METADATA_FILENAME)

    @property
    def schema_path(self) -> str:
        return self._get_path(self.SCHEMA_FILENAME)

    def has_nodes_and_edges(self) -> bool:
        return os.path.exists(self.nodes_path) and os.path.exists(self.edges_path)

    def has_graph_metadata(self) -> bool:
        return os.path.exists(self.graph_metadata_path)

    def has_schema(self) -> bool:
        return os.path.exists(self.schema_path)

    def load_graph_metadata(self) -> dict | None:
        with open(self.graph_metadata_path) as f:
            return json.load(f)

    def load_schema(self) -> dict | None:
        with open(self.schema_path) as f:
            return json.load(f)

    def _get_path(self, file_name: str) -> str | None:
        path = os.path.join(self.graph_dir, file_name)
        if os.path.exists(path + '.gz'):
            return path + '.gz'
        return path

    def compress_nodes_and_edges(self):
        for f in [self.nodes_path, self.edges_path]:
            if not f.endswith('.gz'):
                self.compress_jsonl(f)

    # Stream-gzip jsonl_path to jsonl_path + '.gz' and remove the original.
    # Writes to a temp file and renames so a crash mid-compression won't leave
    # a half-written .gz next to the original.
    @staticmethod
    def compress_jsonl(jsonl_path: str):
        gz_path = jsonl_path + '.gz'
        if os.path.exists(gz_path):
            return
        tmp_path = gz_path + '.tmp'
        with open(jsonl_path, 'rb') as src, gzip.open(tmp_path, 'wb', compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        os.replace(tmp_path, gz_path)
        os.remove(jsonl_path)

    def decompress_nodes_and_edges(self):
        for f in [self.nodes_path, self.edges_path]:
            if f.endswith('.gz'):
                tmp_path = f + '.tmp'
                with gzip.open(f, 'rb') as src, open(tmp_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                os.replace(tmp_path, f)
