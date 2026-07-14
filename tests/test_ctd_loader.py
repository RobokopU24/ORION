import io
import tarfile

from orion.biolink_constants import TAXON
from orion.prefixes import NCBITAXON
from parsers.CTD.src.loadCTD import CTDLoader


class CapturingWriter:
    def __init__(self):
        self.nodes = []
        self.edges = []

    def write_kgx_node(self, node):
        self.nodes.append(node)

    def write_kgx_edge(self, edge):
        self.edges.append(edge)


def test_chemical_gene_taxon_is_edge_context_not_node_property(tmp_path):
    ctd_row = '\t'.join([
        'MESH:C000',
        'test chemical',
        'increases expression of',
        '->',
        'NCBIGene:1',
        'test gene',
        'protein',
        'NCBITaxon:9606',
        'PMID:1|PMID:2|PMID:3',
    ])
    archive_path = tmp_path / 'ctd.tar.gz'
    file_bytes = f'header ignored by parser\n{ctd_row}\n'.encode()
    tar_info = tarfile.TarInfo('ctd-grouped-pipes.tsv')
    tar_info.size = len(file_bytes)
    with tarfile.open(archive_path, 'w:gz') as archive:
        archive.addfile(tar_info, io.BytesIO(file_bytes))

    loader = CTDLoader(test_mode=True, source_data_dir=str(tmp_path))
    loader.output_file_writer = CapturingWriter()

    loader.chemical_to_gene_exp(str(archive_path), 'ctd-grouped-pipes.tsv')

    gene_node = next(node for node in loader.output_file_writer.nodes if node.identifier == 'NCBIGENE:1')
    assert NCBITAXON not in gene_node.properties

    assert len(loader.output_file_writer.edges) == 1
    edge = loader.output_file_writer.edges[0]
    assert edge.properties[TAXON] == 'NCBITaxon:9606'
