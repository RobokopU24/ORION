from collections import defaultdict
import importlib

BINDING_DB = 'BINDING-DB'
BIOLINK = 'Biolink'
CAM_KP = 'CAM-KP'
CHEBI_PROPERTIES = 'CHEBIProps'
REACTOME = 'Reactome'
CORD19 = 'Cord19'
CTD = 'CTD'
DRUG_CENTRAL = 'DrugCentral'
# FOODB = 'FooDB' # this is on hold, data needs review after latest release of data.
GENOME_ALLIANCE_ORTHOLOGS = 'GenomeAllianceOrthologs'
GTEX = 'GTEx'
GTOPDB = 'GtoPdb'
GWAS_CATALOG = 'GWASCatalog'
HETIO = 'Hetio'
HGNC = 'HGNC'
HMDB = 'HMDB'
HUMAN_GOA = 'HumanGOA'
INTACT = 'IntAct'
MONDO_PROPS = 'MONDOProps'
ONTOLOGICAL_HIERARCHY = 'OntologicalHierarchy'
PANTHER = 'PANTHER'
PHAROS = 'PHAROS'
PLANT_GOA = 'PlantGOA'
SCENT = 'Scent'
SGD = 'SGD'
HUMAN_STRING = 'STRING-DB-Human'
TEXT_MINING_KP = 'textminingkp'
UBERGRAPH = 'Ubergraph'
UBERGRAPH_REDUNDANT = 'UbergraphRedundant'
UNIREF = 'UniRef'
VP = 'ViralProteome'
YEAST_HISTONES = 'YeastHistoneMapping'
YEAST_COSTANZA = 'Costanza2016Data'
YEAST_GSE61888 = 'YeastGSE61888'
YEAST_GASCHDIAMIDE = 'YeastGaschDiamideGeneExpression'
YEAST_STRING = 'STRING-DB-Yeast'

RESOURCE_HOGS = [GTEX, GWAS_CATALOG, UNIREF, ONTOLOGICAL_HIERARCHY, UBERGRAPH_REDUNDANT,
                 SGD, HUMAN_STRING, CAM_KP]

SOURCE_DATA_LOADER_CLASS_IMPORTS = {
    BINDING_DB: ("parsers.BINDING.src.loadBINDINGDB", "BINDINGDBLoader"),
    BIOLINK: ("parsers.biolink.src.loadBL", "BLLoader"),
    CAM_KP: ("parsers.camkp.src.loadCAMKP", "CAMKPLoader"),
    CHEBI_PROPERTIES: ("parsers.chebi.src.loadChebiProperties", "ChebiPropertiesLoader"),
    REACTOME: ("parsers.Reactome.src.loadReactome", "ReactomeLoader"),
    CORD19: ("parsers.cord19.src.loadCord19", "Cord19Loader"),
    CTD: ("parsers.CTD.src.loadCTD", "CTDLoader"),
    DRUG_CENTRAL: ("parsers.drugcentral.src.loaddrugcentral", "DrugCentralLoader"),
    GENOME_ALLIANCE_ORTHOLOGS: ("parsers.GenomeAlliance.src.loadGenomeAlliance", "GenomeAllianceOrthologLoader"),
    GTEX: ("parsers.GTEx.src.loadGTEx", "GTExLoader"),
    GTOPDB: ("parsers.gtopdb.src.loadGtoPdb", "GtoPdbLoader"),
    GWAS_CATALOG: ("parsers.GWASCatalog.src.loadGWASCatalog", "GWASCatalogLoader"),
    HETIO: ("parsers.hetio.src.loadHetio", "HetioLoader"),
    HGNC: ("parsers.hgnc.src.loadHGNC", "HGNCLoader"),
    HMDB: ("parsers.hmdb.src.loadHMDB", "HMDBLoader"),
    HUMAN_GOA: ("parsers.GOA.src.loadGOA", "HumanGOALoader"),
    HUMAN_STRING: ("parsers.STRING.src.loadSTRINGDB", "HumanSTRINGDBLoader"),
    INTACT: ("parsers.IntAct.src.loadIA", "IALoader"),
    MONDO_PROPS: ("parsers.MONDOProperties.src.loadMP", "MPLoader"),
    ONTOLOGICAL_HIERARCHY: ("parsers.UberGraph.src.loadUG", "OHLoader"),
    PANTHER: ("parsers.panther.src.loadPanther", "PLoader"),
    PHAROS: ("parsers.PHAROS.src.loadPHAROS", "PHAROSLoader"),
    PLANT_GOA: ("parsers.GOA.src.loadGOA", "PlantGOALoader"),
    SCENT: ("parsers.scent.src.loadScent", "ScentLoader"),
    SGD: ("parsers.SGD.src.loadSGD", "SGDLoader"),
    TEXT_MINING_KP: ("parsers.textminingkp.src.loadTMKP", "TMKPLoader"),
    UBERGRAPH: ("parsers.UberGraph.src.loadUG", "UGLoader"),
    UBERGRAPH_REDUNDANT: ("parsers.UberGraph.src.loadUG", "UGRedundantLoader"),
    UNIREF: ("parsers.ViralProteome.src.loadUniRef", "UniRefSimLoader"),
    VP: ("parsers.ViralProteome.src.loadVP", "VPLoader"),
    YEAST_HISTONES: ("parsers.yeast.src.loadHistoneMap", "YeastHistoneMapLoader"),
    YEAST_COSTANZA: ("parsers.yeast.src.loadCostanza2016", "Costanza2016Loader"),
    YEAST_GASCHDIAMIDE: ("parsers.yeast.src.loadYeastGeneExpressionGasch", "YeastGaschDiamideLoader"),
    YEAST_GSE61888: ("parsers.yeast.src.loadYeastNucleosomesGSE61888", "YeastGSE61888Loader"),
    YEAST_STRING: ("parsers.STRING.src.loadSTRINGDB", "YeastSTRINGDBLoader")
}


def get_available_data_sources():
    return sorted(list(SOURCE_DATA_LOADER_CLASS_IMPORTS.keys()))


# This class allows defaultdicts to instantiate entries dynamically based on what key is requested
# Taken from https://stackoverflow.com/a/44731234
class KeyBasedDefaultDict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = self.default_factory(key)
        return self[key]


# Taken from https://stackoverflow.com/a/61435983.
def get_data_loader_class(key):

    (module_path, class_name) = SOURCE_DATA_LOADER_CLASS_IMPORTS[key]

    # This imports the module pointed to in the first item in the tuple.
    module_ptr = importlib.import_module(module_path)
    # This loads the loader class attribute from the module loaded above
    class_ptr = getattr(module_ptr, class_name)

    # One example of this is the Scent class. First we load in parsers.scent.src.loadScent
    # then get the ScentLoader object from it. The final result is <parsers.scent.src.loadScent.ScentLoader>
    return class_ptr


# This looks overly complex but it is just a defaultdict for which whenever we ask for a new key it runs:
# return get_data_loader_class(key)
#
# This is done solely to implement lazy importing, loader classes that are not used are not imported unnecessarily
class SourceDataLoaderClassFactory(KeyBasedDefaultDict):
    def __init__(self):
        super(KeyBasedDefaultDict, self).__init__(get_data_loader_class)

