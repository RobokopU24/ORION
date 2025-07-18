# A collection of biolink prefixes.

# The better way to do this would be to maintain a list of uris, which are more stable, and then use BMT to turn them
# into actual prefixes.
#i.e. you would have something like
# class Prefixer
#   __init__
#      self.uris={'DRUGBANK':'http://drugbank.org/whatever/,...}
#      self.convert()
#  def convert(self)
#      self.prefixes = { k, bmt(v) for k,v in self.uris.items() }
#  def get_prefix(self,prefixkey):
#      return self.prefixes[prefixkey]
#
#  Then, no matter what the prefix changed to, it would always be tied to the URL and could always be looked up within
# data services using a fixed key independent of biolink changes.

REACTOME = 'REACT'
CHEBI='CHEBI'
CTD='CTD'
CHEMBL='CHEMBL'
CHEMBL_MECHANISM='CHEMBL.MECHANISM'
CLINVAR='CLINVAR'
CLINGEN_ALLELE_REGISTRY='CAID'
DBSNP='DBSNP'
DGIDB='DGIdb'
DRUGBANK='DRUGBANK'
DRUGCENTRAL='DrugCentral'
DOID='DOID'
EFO='EFO'
ENSEMBL='ENSEMBL'
GTOPDB='GTOPDB'
GO='GO'
HETIO='hetio'
HGVS='HGVS'
HGNC='HGNC'
HGNC_FAMILY='HGNC.FAMILY'
HP='HP'
HMDB='HMDB'
INCHIKEY='INCHIKEY'
KEGG_COMPOUND='KEGG.COMPOUND'
KEGG_GLYCAN='KEGG.GLYCAN'
MEDDRA='MEDDRA'
MESH='MESH'
MONDO='MONDO'
NCBIGENE='NCBIGene'
NCBITAXON='NCBITaxon'
NCIT='NCIT'
OMIM='OMIM'
ORPHANET='ORPHANET'
PUBCHEM_COMPOUND='PUBCHEM.COMPOUND'
PUBMED='PMID'
RNACENTRAL='RNACENTRAL'
UBERON='UBERON'
UNIPROTKB='UniProtKB'
UMLS='UMLS'
