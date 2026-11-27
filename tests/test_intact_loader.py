import json
import shutil
from pathlib import Path

from parsers.IntAct.src.loadIA import IALoader


TEST_INTACT_ZIP = Path(__file__).parent / 'resources' / 'intact.zip'


def test_intact_protein_taxon_is_a_curie(tmp_path):
    loader = IALoader(source_data_dir=str(tmp_path))
    shutil.copy(TEST_INTACT_ZIP, Path(loader.data_path) / loader.data_file)
    nodes_path = tmp_path / 'nodes.jsonl'
    edges_path = tmp_path / 'edges.jsonl'

    loader.load(str(nodes_path), str(edges_path))
    nodes = [json.loads(line) for line in nodes_path.read_text().splitlines()]

    protein_taxa = {node['taxon'] for node in nodes if node['id'].startswith('UniProtKB:')}
    assert protein_taxa
    assert all(taxon.startswith('NCBITaxon:') for taxon in protein_taxa), protein_taxa
