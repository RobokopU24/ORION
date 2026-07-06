import os
import shutil

import requests

from orion.config import config
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)


class GraphRegistryError(Exception):
    pass


class GraphRegistryClient:

    def __init__(self, base_url: str = config.ORION_GRAPH_REGISTRY_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self._versions_cache: dict[str, list[dict]] = {}

    def _get(self, path: str):
        url = f'{self.base_url}{path}'
        try:
            response = self.session.get(url, timeout=self.timeout)
        except requests.RequestException as e:
            raise GraphRegistryError(f'Could not reach graph registry at {url}: {e}') from e
        if response.status_code == 404:
            logger.debug(f'Registry has no resource at {url} (HTTP 404).')
            return None
        if response.status_code != 200:
            raise GraphRegistryError(
                f'Request to {url} returned HTTP {response.status_code}: {response.text[:200]}'
            )
        try:
            return response.json()
        except ValueError as e:
            raise GraphRegistryError(f'Response from {url} was not valid JSON: {e}') from e

    def get_versions(self, graph_id: str) -> list[dict]:
        """Return the list of version records for a graph, cached for the client's lifetime.

        Each record looks like {"version": "...", "build_version": "...", "release_date": "...",
        "latest": bool}.
        """
        if graph_id not in self._versions_cache:
            self._versions_cache[graph_id] = self._get(f'/versions/{graph_id}') or []
        return self._versions_cache[graph_id]

    def release_version_for_build_version(self, graph_id: str, build_version: str) -> str | None:
        """Map a build_version to its release_version via the cached /versions
        records, so a caller holding only a build_version can use the release-keyed endpoints."""
        for record in self.get_versions(graph_id):
            if record.get('build_version') == build_version:
                return record.get('version')
        return None

    def get_graph_metadata(self, graph_id: str, release_version: str | None = None) -> dict | None:
        """Fetch graph_metadata for a graph release_version, or the latest if release_version is None.
        Returns None when the graph/version isn't published."""
        if release_version:
            return self._get(f'/graph_metadata/{graph_id}/{release_version}')
        return self._get(f'/graph_metadata/{graph_id}')

    def list_files(self, graph_id: str, release_version: str) -> list[dict]:
        """Return the file manifest for a graph release_version.

        Each entry looks like {"file_path": "<graph_id>/<version>/<name>", "file_size_bytes": <int>};
        paths are relative to the graph's contentUrl. Callers only list files for a version they've
        already resolved, so a missing manifest is a registry inconsistency, not an expected miss —
        this raises GraphRegistryError rather than returning empty.
        """
        files = self._get(f'/files/{graph_id}/{release_version}')
        if files is None:
            raise GraphRegistryError(f'Registry lists no file manifest for {graph_id}/{release_version}.')
        return files

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
                      filename: str,
                      destination_path: str,
                      graph_metadata: dict) -> str:
        """Download a single file from a graph's distribution.

        filename is the basename within the graph's directory (e.g., 'nodes.jsonl.gz').
        The absolute URL is resolved via graph_metadata['distribution'][0]['contentUrl'],
        so this works the same whether the metadata was fetched by release or build version.
        """
        base = self._content_base_url(graph_metadata)
        if not base:
            raise GraphRegistryError(
                f'No distribution.contentUrl found for {graph_id}; '
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
