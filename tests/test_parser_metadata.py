"""Validate that every registered data source has a loadable source.json metadata file.

IngestPipeline.load_parser_metadata reads each source's source.json at runtime; a missing
or malformed file silently breaks orion-build for that source. This test fails loudly at
CI time if a new parser is added without its metadata, or an existing one is renamed/moved.
"""

import pytest

from orion.data_sources import get_available_data_sources
from orion.ingest_pipeline import IngestPipeline


# Loaded once at collection time so each id appears as its own test case.
ALL_SOURCE_IDS = get_available_data_sources()


# Existing source.json files that are present but contain empty strings for all fields.
# Tracked here so the test doesn't silently regress while the stubs are filled in.
KNOWN_STUB_METADATA = {
    'Cord19', 'Costanza2016Data', 'LitCoin', 'LitCoinBagelService', 'MONDOProps',
    'MolePro', 'OHD-Carolina', 'Scent', 'UniRef', 'YeastGSE61888',
    'YeastGaschDiamideGeneExpression', 'YeastHistoneMapping', 'textminingkp',
}


@pytest.mark.parametrize("source_id", ALL_SOURCE_IDS)
def test_parser_metadata_loads(source_id):
    metadata = IngestPipeline.load_parser_metadata(source_id)
    assert isinstance(metadata, dict)


@pytest.mark.parametrize("source_id", sorted(set(ALL_SOURCE_IDS) - KNOWN_STUB_METADATA))
def test_parser_metadata_has_name(source_id):
    metadata = IngestPipeline.load_parser_metadata(source_id)
    assert metadata.get('name'), f"{source_id}'s source.json is missing a 'name' field"