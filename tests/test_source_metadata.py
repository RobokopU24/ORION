import json
import os

import pytest

from orion.data_sources import SOURCE_DATA_LOADER_CLASS_IMPORTS, get_data_source_metadata_path
from orion.kgx_metadata import KGXKnowledgeSource

ORION_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# load every single source metadata file to make sure it initializes successfully as a KGXKnowledgeSource
# then dump it back to a dict and make sure it has an identifier
@pytest.mark.parametrize("source_id", SOURCE_DATA_LOADER_CLASS_IMPORTS.keys())
def test_source_metadata(source_id):
    metadata_path = os.path.join(ORION_ROOT, get_data_source_metadata_path(source_id))
    with open(metadata_path, 'r') as f:
        data = json.load(f)

    source = KGXKnowledgeSource.from_dict(data)
    result = source.to_dict()

    assert result.get("identifier"), f"{source_id}: missing identifier"
