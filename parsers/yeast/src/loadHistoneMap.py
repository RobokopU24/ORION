import os
import enum
import requests as rq
import pandas as pd

from parsers.SGD.src.sgd_source_retriever import SGDAllGenes
from parsers.yeast.src.yeast_constants import YEAST_GENOME_RESOLUTION, SGD_ALL_GENES_FILE, HISTONE_LOCI_FILE
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE

#List of Binned Histone Modifications
class HISTONEMODBINS_EDGEUMAN(enum.IntEnum):
    ID = 0
    CHROMOSOME = 1
    STARTLOCATION = 2
    ENDLOCATION = 3
    LOCI = 4
    MODIFICATION = 5

# Maps Histone Modifications to Genes
class HISTONEMODGENE_EDGEUMAN(enum.IntEnum):
    ID = 0
    CHROMOSOME = 1
    STARTLOCATION = 2
    ENDLOCATION = 3
    LOCI = 4
    MODIFICATION = 5
    GENE = 6

# Maps Histone Modifications to GO Terms
class HISTONEMODGOTERMS_EDGEUMAN(enum.IntEnum):
    ID = 0
    PRED = 1
    GOID = 2
    GONAME = 3

##############
# Class: Loading all histone nodes and histone modifications to GO Terms
#
# By: Jon-Michael Beasley
# Date: 05/08/2023
##############
class YeastHistoneMapLoader(SourceDataLoader):

    source_id: str = 'YeastHistoneMap'
    provenance_id: str = 'infores:yeasthistones'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.genome_resolution = YEAST_GENOME_RESOLUTION

        #self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'
        self.histone_mod_list_file_name = HISTONE_LOCI_FILE
        self.histone_mod_to_gene_file_name = "HistoneMod2Gene.csv"
        self.histone_mod_to_go_term_file_name = "HistonePTM2GO.csv"
        
        self.data_files = [
            self.histone_mod_list_file_name,
            self.histone_mod_to_gene_file_name,
            self.histone_mod_to_go_term_file_name,
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'yeast_v2'

    def get_data(self) -> int:
        """
        Gets the yeast data.

        """
        SGDAllGenes(data_directory=self.data_path)
        self.fetch_histone_data(genome_resolution=self.genome_resolution,
                                output_directory=self.data_path,
                                generate_gene_mapping=True)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)

        #This file is a list of histone modification genomic loci.
        histone_modification_file: str = os.path.join(self.data_path, self.histone_mod_list_file_name)
        with open(histone_modification_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODBINS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'name': f"{line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value]} ({line[HISTONEMODBINS_EDGEUMAN.CHROMOSOME.value]}:{line[HISTONEMODBINS_EDGEUMAN.STARTLOCATION.value]}-{line[HISTONEMODBINS_EDGEUMAN.ENDLOCATION.value]})",
                                                'categories': ['biolink:NucleosomeModification','biolink:PosttranslationalModification'],
                                                'histoneModification': line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value],
                                                'chromosomeLocation': line[HISTONEMODBINS_EDGEUMAN.LOCI.value]}, # subject props
                                  lambda line: {},  # object props
                                  lambda line: { PRIMARY_KNOWLEDGE_SOURCE: "infores:yeasthistones"},#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Genes to BinnedHistonePTMs.
        gene_to_histone_mod_edges_file: str = os.path.join(self.data_path, self.histone_mod_to_gene_file_name)
        with open(gene_to_histone_mod_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODGENE_EDGEUMAN.ID.value], #subject id
                                  lambda line: line[HISTONEMODGENE_EDGEUMAN.GENE.value],  # object id
                                  lambda line: "biolink:located_in",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "infores:yeasthistones"
                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Binned Histone PTMS to general PTMs.
        histone_modification_file: str = os.path.join(self.data_path, self.histone_mod_list_file_name)
        with open(histone_modification_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODBINS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: "HisPTM:"+line[HISTONEMODBINS_EDGEUMAN.MODIFICATION.value],  # object id
                                  lambda line: "biolink:subclass_of",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "infores:yeasthistones"
                                                },#edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #General PTMs to GO Terms.
        histone_mod2go_term_file: str = os.path.join(self.data_path, self.histone_mod_to_go_term_file_name)
        with open(histone_mod2go_term_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.ID.value],  # subject id
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.GOID.value],  # object id
                                  lambda line: line[HISTONEMODGOTERMS_EDGEUMAN.PRED.value],  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                PRIMARY_KNOWLEDGE_SOURCE: "infores:yeasthistones"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        return extractor.load_metadata

    def fetch_histone_data(self,
                           genome_resolution: int,
                           output_directory: str,
                           generate_gene_mapping: bool = True):

        # Creates sliding windows of hypothetical genome locations of all Histone PTMs.
        n = int(genome_resolution)  # Sets sliding window resolution.
        self.logger.debug(
            f"---------------------------------------------------\nCreating sliding window of resolution {n} of yeast genome loci...\n---------------------------------------------------\n")

        data = {'hisPTMid': [], 'chromosomeID': [], 'start': [], 'end': [], 'loci': [], 'histoneMod': []}

        # Reference: https://wiki.yeastgenome.org/index.php/Systematic_Sequencing_Table

        chromosome_lengths = {'chrI': 230218, 'chrII': 813184, 'chrIII': 316620,
                              'chrIV': 1531933, 'chrV': 576874, 'chrVI': 270161, 'chrVII': 1090940,
                              'chrVIII': 562643, 'chrIX': 439888, 'chrX': 745751, 'chrXI': 666816,
                              'chrXII': 1078177, 'chrXIII': 924431, 'chrXIV': 784333, 'chrXV': 1091291,
                              'chrXVI': 948066, 'chrmt': 85779}

        histonePTMs = [
            'H2AK5ac', 'H2AS129ph', 'H3K14ac', 'H3K18ac', 'H3K23ac',
            'H3K27ac', 'H3K36me', 'H3K36me2', 'H3K36me3', 'H3K4ac',
            'H3K4me', 'H3K4me2', 'H3K4me3', 'H3K56ac', 'H3K79me',
            'H3K79me3', 'H3K9ac', 'H3S10ph', 'H4K12ac', 'H4K16ac', 'H4K20me', 'H4K5ac',
            'H4K8ac', 'H4R3me', 'H4R3me2s', 'HTZ1'
        ]

        rhea_identifiers = {'H2AK5ac': None, 'H2AS129ph': None, 'H3K14ac': None, 'H3K18ac': None, 'H3K23ac': None,
                            'H3K27ac': None, 'H3K36me': 'RHEA-COMP:9786', 'H3K36me2': 'RHEA-COMP:9787',
                            'H3K36me3': 'RHEA-COMP:15536', 'H3K4ac': None,
                            'H3K4me': 'RHEA-COMP:15543', 'H3K4me2': 'RHEA-COMP:15540', 'H3K4me3': 'RHEA-COMP:15537',
                            'H3K56ac': None, 'H3K79me': 'RHEA-COMP:15550',
                            'H3K79me3': ' RHEA-COMP:15552', 'H3K9ac': None, 'H3S10ph': None, 'H4K12ac': None,
                            'H4K16ac': None, 'H4K20me': 'RHEA-COMP:15555', 'H4K5ac': None,
                            'H4K8ac': None, 'H4R3me': None, 'H4R3me2s': None, 'HTZ1': None}

        # Will continue to work on this mapping.
        # Get descendants of GO term "histone modifying activity" (GO:0140993)
        HisModDescendants = rq.get(
            f"https://www.ebi.ac.uk/QuickGO/services/ontology/go/terms/GO:0140993/descendants").json()
        descendants = str(HisModDescendants['results'][0]['descendants']).replace("'", "").replace(" ", "").replace(
            "[", "").replace("]", "")
        descendantNames = rq.get(f"https://www.ebi.ac.uk/QuickGO/services/ontology/go/terms/{descendants}").json()
        descendant_dict = {}
        for result in descendantNames['results']:
            descendant_dict.update({result['name']: result['id']})

        histonePTM2GO = {'ptm': [], 'predicate': [], 'GOid': [], 'GOname': []}
        for ptm in histonePTMs:
            if 'ac' in ptm:
                mod = ['acetyl']
                notmod = []
                ptmloc = ptm.replace('ac', '')
            elif 'me2s' in ptm:
                mod = ['methyl', 'dimethyl']
                notmod = ['monomethyl', 'trimethyl']
                ptmloc = ptm.replace('me2s', '')
            elif 'me2' in ptm:
                mod = ['methyl', 'dimethyl']
                notmod = ['monomethyl', 'trimethyl']
                ptmloc = ptm.replace('me2', '')
            elif 'me3' in ptm:
                mod = ['methyl', 'trimethyl']
                notmod = ['monomethyl', 'dimethyl']
                ptmloc = ptm.replace('me3', '')
            elif 'me' in ptm:
                mod = ['methyl']
                notmod = ['dimethyl', 'trimethyl']
                ptmloc = ptm.replace('me', '')
            elif 'ph' in ptm:
                mod = ['phosph', 'kinase']
                notmod = []
                ptmloc = ptm.replace('ph', '')
            else:
                continue

            if 'H2A' in ptm or 'H2B' in ptm:
                query_process = f"histone {ptmloc[0:3]}-{ptmloc[3:]}"
                query_activity = f"histone {ptmloc[0:3]}{ptmloc[3:]}"
            else:
                query_process = f"histone {ptmloc[0:2]}-{ptmloc[2:]}"
                query_activity = f"histone {ptmloc[0:2]}{ptmloc[2:]}"

            pred_dict = {
                "CTD:affects_abundance_of": ["regulation"],
                "CTD:increases_abundance_of": ["positive regulation"],
                "CTD:decreases_abundance_of": ["negative regulation", " de"],
            }
            for name in descendant_dict.keys():
                if query_process in name or query_activity in name:
                    if any(x in name for x in mod):
                        if not any(x in name for x in notmod):
                            histonePTM2GO['ptm'] = histonePTM2GO['ptm'] + ["HisPTM:" + ptm]
                            histonePTM2GO['GOname'] = histonePTM2GO['GOname'] + [name]
                            histonePTM2GO['GOid'] = histonePTM2GO['GOid'] + [descendant_dict[name]]
                            if any(x in name for x in pred_dict["CTD:decreases_abundance_of"]):
                                histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + [
                                    "CTD:decreases_abundance_of"]
                            elif any(x in name for x in pred_dict["CTD:affects_abundance_of"]):
                                if any(x in name for x in pred_dict["CTD:increases_abundance_of"]):
                                    histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + [
                                        "CTD:increases_abundance_of"]
                                else:
                                    histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + [
                                        "CTD:affects_abundance_of"]
                            else:
                                histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + [
                                    "CTD:increases_abundance_of"]

        self.logger.debug('Histone Modifications Mapped to GO Terms!')
        csv_fname = f"HistonePTM2GO.csv"
        histonePTM2GO_df = pd.DataFrame.from_dict(histonePTM2GO)
        histonePTM2GO_df.to_csv(os.path.join(output_directory, csv_fname), encoding="utf-8-sig", index=False)
        for chr in chromosome_lengths.keys():
            m = int(chromosome_lengths[chr])
            for i in range(m):  # Create loci nodes for chromosomes
                if i != 0 and i % n == 0:
                    for ptm in histonePTMs:
                        data['hisPTMid'].append(
                            "BinHisPTM:" + chr + "(" + str(i - (n - 1)) + "-" + str(i) + ")" + ";" + ptm)
                        data['chromosomeID'].append(str(chr))
                        data['start'].append(i - (n - 1))
                        data['end'].append(i)
                        data['loci'].append(f"{str(chr)}({i - (n - 1)}-{i})")
                        data['histoneMod'].append(ptm)

                # Handles the tail end of chromosomes.
                if i == m - 1:
                    for ptm in histonePTMs:
                        data['hisPTMid'].append(
                            "BinHisPTM:" + chr + "(" + str(((m // 9) * 9) + 1) + "-" + str(m) + ")" + ";" + ptm)
                        data['chromosomeID'].append(str(chr))
                        data['start'].append(((m // 9) * 9) + 1)
                        data['end'].append(m)
                        data['loci'].append(f"{str(chr)}({((m // 9) * 9) + 1}-{m})")
                        data['histoneMod'].append(ptm)
        genomelocidf = pd.DataFrame(data)
        self.logger.debug('Histone Modifications Loci Collected!')
        genomelocidf.to_csv(os.path.join(output_directory, HISTONE_LOCI_FILE), encoding="utf-8-sig", index=False)

        if not generate_gene_mapping:
            return

        allgenesdf = pd.read_csv(os.path.join(output_directory, SGD_ALL_GENES_FILE))
        chrome_dict = {}
        for uc in chromosome_lengths.keys():
            chrome_dict.update({uc: allgenesdf.loc[(allgenesdf['chromosome.primaryIdentifier'] == uc)]})

        mapped_genes = []
        just_windows = genomelocidf[['loci', 'chromosomeID', 'start', 'end']]
        just_windows = just_windows.drop_duplicates().reset_index(drop=True)
        total = len(just_windows.index)
        for idx, row in just_windows.iterrows():
            if (idx % 10000) == 0:
                self.logger.debug(f"{idx} of {total}")
            gene = chrome_dict[row['chromosomeID']].loc[
                (row['end'] >= chrome_dict[row['chromosomeID']]['chromosomeLocation.start']) & (
                            row['start'] <= chrome_dict[row['chromosomeID']]['chromosomeLocation.end'])]

            gene = gene['primaryIdentifier'].values[:]
            if len(gene) < 1:
                gene = "None"

            mapped_genes = mapped_genes + [gene]

        just_windows['mapped_genes'] = mapped_genes
        just_windows = just_windows[just_windows.mapped_genes.isin(["None"]) == False]
        just_windows = just_windows.explode('mapped_genes')
        genomelocidf = genomelocidf.merge(just_windows, how='inner', on=['chromosomeID', 'start', 'end', 'loci'])

        self.logger.debug(f"Histone Modifications Mapping Complete!")
        csv_f3name = f"HistoneMod2Gene.csv"
        genomelocidf.to_csv(os.path.join(output_directory, csv_f3name), encoding="utf-8-sig", index=False)
