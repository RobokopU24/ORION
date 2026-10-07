import io
import json
import tarfile

from orion.biolink_constants import SPECIES_CONTEXT_QUALIFIER, TAXON
from orion.merging import MemoryGraphMerger
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


def ctd_chemical_gene_row(taxon_id):
    return '\t'.join([
        'MESH:C000',
        'test chemical',
        'increases expression of',
        '->',
        'NCBIGene:1',
        'test gene',
        'protein',
        taxon_id,
        'PMID:1|PMID:2|PMID:3',
    ])


def run_chemical_to_gene(tmp_path, rows):
    archive_path = tmp_path / 'ctd.tar.gz'
    file_bytes = ('header ignored by parser\n' + ''.join(f'{row}\n' for row in rows)).encode()
    tar_info = tarfile.TarInfo('ctd-grouped-pipes.tsv')
    tar_info.size = len(file_bytes)
    with tarfile.open(archive_path, 'w:gz') as archive:
        archive.addfile(tar_info, io.BytesIO(file_bytes))

    loader = CTDLoader(test_mode=True, source_data_dir=str(tmp_path))
    loader.output_file_writer = CapturingWriter()
    loader.chemical_to_gene_exp(str(archive_path), 'ctd-grouped-pipes.tsv')
    return loader.output_file_writer


def test_chemical_gene_taxon_is_species_context_qualifier_not_node_property(tmp_path):
    writer = run_chemical_to_gene(tmp_path, [ctd_chemical_gene_row('NCBITaxon:9606')])

    gene_node = next(node for node in writer.nodes if node.identifier == 'NCBIGENE:1')
    assert NCBITAXON not in gene_node.properties

    assert len(writer.edges) == 1
    edge = writer.edges[0]
    assert edge.properties[SPECIES_CONTEXT_QUALIFIER] == 'NCBITaxon:9606'
    assert TAXON not in edge.properties


# the species qualifier is part of the default edge merge key so two edges with different taxa should stay distinct 
def test_chemical_gene_edges_from_different_species_do_not_merge(tmp_path):
    writer = run_chemical_to_gene(tmp_path, [ctd_chemical_gene_row('NCBITaxon:9606'),
                                             ctd_chemical_gene_row('NCBITaxon:10116')])

    merger = MemoryGraphMerger()
    merger.merge_edges([{'subject': edge.subjectid,
                         'predicate': edge.predicate,
                         'object': edge.objectid,
                         'primary_knowledge_source': edge.primary_knowledge_source,
                         **edge.properties} for edge in writer.edges])
    merged_edges = [json.loads(line) for line in merger.get_merged_edges_jsonl()]
    merged_species = sorted(edge[SPECIES_CONTEXT_QUALIFIER] for edge in merged_edges)
    assert merged_species == ['NCBITaxon:10116', 'NCBITaxon:9606']