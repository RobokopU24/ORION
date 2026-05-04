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

    def get_graph_metadata(self, graph_id: str, graph_version: str | None = None) -> dict:
        """Fetch graph_metadata for a graph version, or the latest if graph_version is None."""
        if graph_version:
            return self._get(f'/graph_metadata/{graph_id}/{graph_version}')
        return self._get(f'/graph_metadata/{graph_id}')

    def list_files(self, graph_id: str, graph_version: str) -> list[dict]:
        """Return the file manifest for a graph version.

        Each entry looks like {"file_path": "<graph_id>/<version>/<name>",
        "file_size_bytes": <int>}. Paths are relative to the graph's contentUrl.
        """
        return self._get(f'/files/{graph_id}/{graph_version}')

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
                      graph_version: str,
                      filename: str,
                      destination_path: str,
                      graph_metadata: dict | None = None) -> str:
        """Download a single file from a graph version's distribution.

        filename is the basename within the graph's directory (e.g., 'nodes.jsonl.gz').
        Resolves the absolute URL via graph_metadata['distribution'][0]['contentUrl']
        when provided; otherwise fetches metadata to find it.
        """
        if graph_metadata is None:
            graph_metadata = self.get_graph_metadata(graph_id, graph_version)
        base = self._content_base_url(graph_metadata)
        if not base:
            raise GraphRegistryError(
                f'No distribution.contentUrl found for {graph_id}/{graph_version}; '
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