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


DRUGBANK="DrugBank"
DRUGCENTRAL="DrugCentral"
UMLS="UMLS"
MEDDRA='MEDDRA'
UNIPROTKB='UniProtKB'
PUBMED='PMID'
CTD='CTD'
NCBITAXON='NCBITaxon'
MESH='MESH'
GTOPDB='GTOPDB'
ENSEMBL='ENSEMBL'
HGNC='HGNC'
HGNC_FAMILY='HGNC.FAMILY'
HMDB='HMDB'
OMIM='OMIM'