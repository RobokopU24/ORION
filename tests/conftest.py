"""Session-wide test configuration.

Disables the remote graph registry so tests don't reach out to a live service.
Individual tests can re-enable it with monkeypatch if they want to exercise that path.
"""

import pytest

from orion.config import config


@pytest.fixture(autouse=True, scope='session')
def _disable_graph_registry_for_tests():
    config.ORION_USE_GRAPH_REGISTRY = False
    yield