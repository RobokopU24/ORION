import os
import shutil

import requests

from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)

DEFAULT_REGISTRY_URL = 'https://robokop-graph-registry.apps.renci.org'


class GraphRegistryError(Exception):
    pass


class GraphRegistryClient:

    def __init__(self, base_url: str = DEFAULT_REGISTRY_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path: str):
        url = f'{self.base_url}{path}'
        try:
            response = self.session.get(url, timeout=self.timeout)
        except requests.RequestException as e:
            raise GraphRegistryError(f'Request to {url} failed: {e}') from e
        if response.status_code != 200:
            raise GraphRegistryError(
                f'Request to {url} returned HTTP {response.status_code}: {response.text[:200]}'
            )
        try:
            return response.json()
        except ValueError as e:
            raise GraphRegistryError(f'Response from {url} was not valid JSON: {e}') from e

    def list_graphs(self) -> list[str]:
        """Return the list of graph IDs known to the registry.

        The /graphs endpoint returns {"graphs": [{"<graph_id>": [<version>, ...]}, ...]};
        this method flattens that to just the graph IDs.
        """
        payload = self._get('/graphs')
        graphs = payload.get('graphs', []) if isinstance(payload, dict) else []
        graph_ids = []
        for entry in graphs:
            if isinstance(entry, dict):
                graph_ids.extend(entry.keys())
        return graph_ids

    def list_graphs_with_versions(self) -> dict[str, list[dict]]:
        """Return a mapping of graph_id -> list of version dicts as reported by /graphs."""
        payload = self._get('/graphs')
        graphs = payload.get('graphs', []) if isinstance(payload, dict) else []
        result: dict[str, list[dict]] = {}
        for entry in graphs:
            if isinstance(entry, dict):
                for graph_id, versions in entry.items():
                    result[graph_id] = versions or []
        return result

    def get_versions(self, graph_id: str) -> list[dict]:
        """Return the list of version records for a graph.

        Each record looks like {"version": "...", "release_date": "...", "latest": bool}.
        """
        return self._get(f'/versions/{graph_id}')

    def get_latest_version(self, graph_id: str) -> str | None:
        """Return the version string flagged as latest for the given graph, or None."""
        versions = self.get_versions(graph_id)
        for v in versions:
            if v.get('latest'):
                return v.get('version')
        return versions[0].get('version') if versions else None

    def get_graph_metadata(self, graph_id: str, release_version: str | None = None) -> dict:
        """Fetch graph_metadata for a graph version, or the latest if release_version is None."""
        if release_version:
            return self._get(f'/graph_metadata/{graph_id}/{release_version}')
        return self._get(f'/graph_metadata/{graph_id}')

    def list_files(self, graph_id: str, release_version: str) -> list[dict]:
        """Return the file manifest for a graph version.

        Each entry looks like {"file_path": "<graph_id>/<version>/<name>",
        "file_size_bytes": <int>}. Paths are relative to the graph's contentUrl.
        """
        return self._get(f'/files/{graph_id}/{release_version}')

    @staticmethod
    def _content_base_url(graph_metadata: dict) -> str | None:
        distribution = graph_metadata.get('distribution') or []
        for entry in distribution:
            content_url = entry.get('contentUrl')
            if content_url:
                return content_url if content_url.endswith('/') else content_url + '/'
        return None

    def download_file(self,
                      graph_id: str,
                      release_version: str,
                      filename: str,
                      destination_path: str,
                      graph_metadata: dict | None = None) -> str:
        """Download a single file from a graph version's distribution.

        filename is the basename within the graph's directory (e.g., 'nodes.jsonl.gz').
        Resolves the absolute URL via graph_metadata['distribution'][0]['contentUrl']
        when provided; otherwise fetches metadata to find it.
        """
        if graph_metadata is None:
            graph_metadata = self.get_graph_metadata(graph_id, release_version)
        base = self._content_base_url(graph_metadata)
        if not base:
            raise GraphRegistryError(
                f'No distribution.contentUrl found for {graph_id}/{release_version}; '
                f'cannot resolve download URL for {filename}.'
            )
        url = f'{base}{filename}'
        os.makedirs(os.path.dirname(destination_path) or '.', exist_ok=True)
        tmp_path = destination_path + '.tmp'
        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as response:
                if response.status_code != 200:
                    raise GraphRegistryError(
                        f'Download of {url} returned HTTP {response.status_code}'
                    )
                with open(tmp_path, 'wb') as out:
                    shutil.copyfileobj(response.raw, out, length=1024 * 1024)
        except requests.RequestException as e:
            raise GraphRegistryError(f'Download of {url} failed: {e}') from e
        os.replace(tmp_path, destination_path)
        return destination_path

    # ------------------------------------------------------------------------------
    # Source-build endpoints.
    #
    # Source builds are the content-addressed, per-source cache that ORION uses to dedup
    # ingested+normalized+merged contributions across every graph that consumes them. Keyed
    # by (source_id, build_version), where build_version is a deterministic hash over the
    # source_version, parsing_version, normalization_scheme, and supplementation_version.
    # The endpoints below mirror the existing /graphs catalog, but in a separate namespace
    # so user-facing graphs and the internal source cache don't share identifiers.
    #
    # Endpoints (see docs/registry_source_builds_plan.md):
    #   GET /sources                                       -> {"sources": [<source_id>, ...]}
    #   GET /sources/{source_id}/builds                    -> [{"build_version": ..., "build_time": ...}, ...]
    #   GET /sources/{source_id}/builds/{build_version}    -> graph-metadata.json
    #   GET /sources/{source_id}/builds/{build_version}/files -> [{"file_path": "<sid>/<bv>/<name>",
    #                                                              "file_size_bytes": int}, ...]
    # ------------------------------------------------------------------------------
    def list_sources(self) -> list[str]:
        """Return the list of source_ids with at least one published build."""
        payload = self._get('/sources')
        if isinstance(payload, dict):
            return payload.get('sources', []) or []
        return payload or []

    def list_source_builds(self, source_id: str) -> list[dict]:
        """Return the list of published source builds for a source, each entry like
        {"build_version": "...", "build_time": "...", "node_count": ..., "edge_count": ...}.
        """
        return self._get(f'/sources/{source_id}/builds')

    def get_source_build_metadata(self, source_id: str, build_version: str) -> dict:
        """Fetch the graph-metadata.json (KGX graph metadata) for one source build."""
        return self._get(f'/sources/{source_id}/builds/{build_version}')

    def list_source_build_files(self, source_id: str, build_version: str) -> list[dict]:
        """Return the file manifest for one source build.

        Each entry looks like {"file_path": "<source_id>/<build_version>/<name>",
        "file_size_bytes": <int>}. Paths are relative to the source build's contentUrl.
        """
        return self._get(f'/sources/{source_id}/builds/{build_version}/files')

    def download_source_build_file(self,
                                    source_id: str,
                                    build_version: str,
                                    filename: str,
                                    destination_path: str,
                                    build_metadata: dict | None = None) -> str:
        """Download a single file from a source build's distribution.

        filename is the basename within the build's directory (e.g., 'nodes.jsonl' or
        'nodes.jsonl.gz'). Resolves the absolute URL via
        build_metadata['distribution'][0]['contentUrl'] when provided, otherwise fetches
        the build's metadata to find it.
        """
        if build_metadata is None:
            build_metadata = self.get_source_build_metadata(source_id, build_version)
        base = self._content_base_url(build_metadata)
        if not base:
            raise GraphRegistryError(
                f'No distribution.contentUrl found for source build {source_id}/{build_version}; '
                f'cannot resolve download URL for {filename}.'
            )
        url = f'{base}{filename}'
        os.makedirs(os.path.dirname(destination_path) or '.', exist_ok=True)
        tmp_path = destination_path + '.tmp'
        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as response:
                if response.status_code != 200:
                    raise GraphRegistryError(
                        f'Download of {url} returned HTTP {response.status_code}'
                    )
                with open(tmp_path, 'wb') as out:
                    shutil.copyfileobj(response.raw, out, length=1024 * 1024)
        except requests.RequestException as e:
            raise GraphRegistryError(f'Download of {url} failed: {e}') from e
        os.replace(tmp_path, destination_path)
        return destination_path

    def get_all_graph_metadata(self, latest_only: bool = True) -> dict[str, dict | list[dict]]:
        """Fetch graph_metadata for every graph in the registry.

        When latest_only is True (default), returns {graph_id: metadata_dict} for the
        latest version of each graph. When False, returns {graph_id: [metadata_dict, ...]}
        with one entry per known version.
        """
        graphs = self.list_graphs_with_versions()
        result: dict[str, dict | list[dict]] = {}
        for graph_id, versions in graphs.items():
            if latest_only:
                try:
                    result[graph_id] = self.get_graph_metadata(graph_id)
                except GraphRegistryError as e:
                    logger.warning(f'Failed to fetch latest metadata for {graph_id}: {e}')
            else:
                metadata_list = []
                for version_record in versions:
                    version = version_record.get('version')
                    if not version:
                        continue
                    try:
                        metadata_list.append(self.get_graph_metadata(graph_id, version))
                    except GraphRegistryError as e:
                        logger.warning(
                            f'Failed to fetch metadata for {graph_id} version {version}: {e}'
                        )
                result[graph_id] = metadata_list
        return result
