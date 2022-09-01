import os
import enum

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, ORIGINAL_KNOWLEDGE_SOURCE


# Maps Experimental Condition affects Nucleosome edge.
class EXPNUC_EDGEUMAN(enum.IntEnum):
    NUC_ID = 0
    CHR = 1
    CENTER = 2
    COVERAGE_RPM = 3
    GENE = 4
    ACC = 5
    GENE_POS = 6
    H2AK5AC_0 = 7
    H2AK5AC_4 = 8
    H2AK5AC_8 = 9
    H2AK5AC_15 = 10
    H2AK5AC_30 = 11
    H2AK5AC_60 = 12
    H2AS129PH_0 = 13
    H2AS129PH_4 = 14
    H2AS129PH_8 = 15
    H2AS129PH_15 = 16
    H2AS129PH_30 = 17
    H2AS129PH_60 = 18
    H3K14AC_0 = 19
    H3K14AC_4 = 20
    H3K14AC_8 = 21
    H3K14AC_15 = 22
    H3K14AC_30 = 23
    H3K14AC_60 = 24
    H3K18AC_0 = 25
    H3K18AC_4 = 26
    H3K18AC_8 = 27
    H3K18AC_15 = 28
    H3K18AC_30 = 29
    H3K18AC_60 = 30
    H3K23AC_0 = 31
    H3K23AC_4 = 32
    H3K23AC_8 = 33
    H3K23AC_15 = 34
    H3K23AC_30 = 35
    H3K23AC_60 = 36
    H3K27AC_0 = 37
    H3K27AC_4 = 38
    H3K27AC_8 = 39
    H3K27AC_15 = 40
    H3K27AC_30 = 41
    H3K27AC_60 = 42
    H3K36ME_0 = 43
    H3K36ME_4 = 44
    H3K36ME_8 = 45
    H3K36ME_15 = 46
    H3K36ME_30 = 47
    H3K36ME_60 = 48
    H3K36ME2_0 = 49
    H3K36ME2_4 = 50
    H3K36ME2_8 = 51
    H3K36ME2_15 = 52
    H3K36ME2_30 = 53
    H3K36ME2_60 = 54
    H3K36ME3_0 = 55
    H3K36ME3_4 = 56
    H3K36ME3_8 = 57
    H3K36ME3_15 = 58
    H3K36ME3_30 = 59
    H3K36ME3_60 = 60
    H3K4AC_0 = 61
    H3K4AC_4 = 62
    H3K4AC_8 = 63
    H3K4AC_15 = 64
    H3K4AC_30 = 65
    H3K4AC_60 = 66
    H3K4ME_0 = 67
    H3K4ME_4 = 68
    H3K4ME_8 = 69
    H3K4ME_15 = 70
    H3K4ME_30 = 71
    H3K4ME_60 = 72
    H3K4ME2_0 = 73
    H3K4ME2_4 = 74
    H3K4ME2_8 = 75
    H3K4ME2_15 = 76
    H3K4ME2_30 = 77
    H3K4ME2_60 = 78
    H3K4ME3_0 = 79
    H3K4ME3_4 = 80
    H3K4ME3_8 = 81
    H3K4ME3_15 = 82
    H3K4ME3_30 = 83
    H3K4ME3_60 = 84
    H3K56AC_0 = 85
    H3K56AC_4 = 86
    H3K56AC_8 = 87
    H3K56AC_15 = 88
    H3K56AC_30 = 89
    H3K56AC_60 = 90
    H3K79ME_0 = 91
    H3K79ME_4 = 92
    H3K79ME_8 = 93
    H3K79ME_15 = 94
    H3K79ME_30 = 95
    H3K79ME_60 = 96
    H3K79ME3_0 = 97
    H3K79ME3_4 = 98
    H3K79ME3_8 = 99
    H3K79ME3_15 = 100
    H3K79ME3_30 = 101
    H3K79ME3_60 = 102
    H3K9AC_0 = 103
    H3K9AC_4 = 104
    H3K9AC_8 = 105
    H3K9AC_15 = 106
    H3K9AC_30 = 107
    H3K9AC_60 = 108
    H3S10PH_0 = 109
    H3S10PH_4 = 110
    H3S10PH_8 = 111
    H3S10PH_15 = 112
    H3S10PH_30 = 113
    H3S10PH_60 = 114
    H4K12AC_0 = 115
    H4K12AC_4 = 116
    H4K12AC_8 = 117
    H4K12AC_15 = 118
    H4K12AC_30 = 119
    H4K12AC_60 = 120
    H4K16AC_0 = 121
    H4K16AC_4 = 122
    H4K16AC_8 = 123
    H4K16AC_15 = 124
    H4K16AC_30 = 125
    H4K16AC_60 = 126
    H4K20ME_0 = 127
    H4K20ME_4 = 128
    H4K20ME_8 = 129
    H4K20ME_15 = 130
    H4K20ME_30 = 131
    H4K20ME_60 = 132
    H4K5AC_0 = 133
    H4K5AC_4 = 134
    H4K5AC_8 = 135
    H4K5AC_15 = 136
    H4K5AC_30 = 137
    H4K5AC_60 = 138
    H4K8AC_0 = 139
    H4K8AC_4 = 140
    H4K8AC_8 = 141
    H4K8AC_15 = 142
    H4K8AC_30 = 143
    H4K8AC_60 = 144
    H4R3ME_0 = 145
    H4R3ME_4 = 146
    H4R3ME_8 = 147
    H4R3ME_15 = 148
    H4R3ME_30 = 149
    H4R3ME_60 = 150
    H4R3ME2S_0 = 151
    H4R3ME2S_4 = 152
    H4R3ME2S_8 = 153
    H4R3ME2S_15 = 154
    H4R3ME2S_30 = 155
    H4R3ME2S_60 = 156
    HTZ1_0 = 157
    HTZ1_4 = 158
    HTZ1_8 = 159
    HTZ1_15 = 160
    HTZ1_30 = 161
    HTZ1_60 = 162
    SGD_ID = 163

# Maps Nucleosomes located_on Gene edge
class NUCGENE_EDGEUMAN(enum.IntEnum):
    NUCLEOSOME = 1
    GENE = 6

#mmod_id,hypothetical_nucleosome,relationship,cosine_distance
"""
class NUCGENE_EDGECOSDIST(enum.IntEnum):
    DRUG_ID = 0
    VERBAL_SCENT = 1
    PREDICATE = 2
    DISTANCE = 3
"""


##############
# Class: Nucleosome Mapping to Gene Data loader
#
# By: Jon-Michael Beasley
# Date: 07/31/2022
# Desc: Class that loads/parses the unique data for handling GSE61888 nucleosomes.
##############
class YeastNucleosomeLoaderGSE61888(SourceDataLoader):

    source_id: str = 'YeastNucleosomesGSE61888'
    provenance_id: str = 'yeast_nucleosomes_GSE61888'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.cos_dist_threshold = 1.0
        #self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        #self.experimental_conditions_list_file_name = "experimental_conditions_list.csv"
        self.nucleosome_list_file_name = "/Data_services/parsers/yeast/src/SGD_Data_Storage/GSE61888_nucs_normed_updated.csv"
        #self.sgd_gene_list_file_name = "/SGD_Data_Storage/SDGAllGenes.csv"
        #self.experimental_conditions_to_nucleosomes_edges_file_name = "experimental_conditions_mapped_to_nucleosomes.csv"
        #self.nucleosome_to_gene_edges_file_name = "Yeast_Nucleosomes_Mapped_to_Genes.csv"
        

        self.data_files = [
            #self.experimental_conditions_list_file_name,
            self.nucleosome_list_file_name
            #self.sgd_gene_list_file_name
            #self.experimental_conditions_to_nucleosomes_edges_file_name,
            #self.nucleosome_to_gene_edges_file_name,
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'yeast_v1'

    def get_data(self) -> int:
        """
        Gets the yeast data.
        """
    #    main(self.data_path)
        #data_puller = GetData()
        #for source in self.data_files:
        #    source_url = f"{self.yeast_data_url}{source}"
        #    data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        #This file is a list of nuclesomes with histone PTM enrichement from "GSE61888 High resolution chromatin dynamics during a yeast stress response"
        nucleosome_list_file: str = (self.nucleosome_list_file_name)
        with open(nucleosome_list_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"NUC:GSE61888_{str(line[EXPNUC_EDGEUMAN.NUC_ID.value])}", # subject id
                                  lambda line: None,  # object id
                                  lambda line: None,  # predicate extractor
                                  lambda line: {'categories': ['nucleosome','biolink:GenomicEntity'],
                                                'name':f"GSE61888: Nucleosome_{str(line[EXPNUC_EDGEUMAN.NUC_ID.value])}",
                                                'chromosomeLocation': f"chr{str(line[EXPNUC_EDGEUMAN.CHR.value])}: {str(line[EXPNUC_EDGEUMAN.CENTER.value])}",
                                                'genePosition': line[EXPNUC_EDGEUMAN.GENE_POS.value],
                                                'referenceLink': "https://pubmed.ncbi.nlm.nih.gov/25801168/"},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        #Experimental conditions to nucleosomes edges. Edges contain nucleosome occupancy and histone PTMs/Variants as properties.
        #In this case, handles only "GSE61888 High resolution chromatin dynamics during a yeast stress response" data
        experimental_conditions_to_nucleosome_edges_file: str = (self.nucleosome_list_file_name)
        with open(experimental_conditions_to_nucleosome_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: "PUBCHEM.COMPOUND:5353800", #subject id #In this case, it is set as "Diamide", the stressor used in GSE61888
                                  lambda line: f"NUC:GSE61888_{str(line[EXPNUC_EDGEUMAN.NUC_ID.value])}",  # object id
                                  lambda line: "biolink:affects_molecular_modification_of",  # predicate extractor
                                  lambda line: None,  # subject props
                                  lambda line: None,  # object props
                                  lambda line: {'occupancy(rpm)':float(line[EXPNUC_EDGEUMAN.COVERAGE_RPM.value]),
                                                'epigeneticModifications(log2FC)': {
                                                    'H2AK5ac': [float(line[EXPNUC_EDGEUMAN.H2AK5AC_0.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_4.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_8.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_15.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_30.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_60.value])],
                                                    'H2AS129ph': [float(line[EXPNUC_EDGEUMAN.H2AS129PH_0.value]),float(line[EXPNUC_EDGEUMAN.H2AS129PH_4.value]),float(line[EXPNUC_EDGEUMAN.H2AS129PH_8.value]),float(line[EXPNUC_EDGEUMAN.H2AS129PH_15.value]),float(line[EXPNUC_EDGEUMAN.H2AS129PH_30.value]),float(line[EXPNUC_EDGEUMAN.H2AS129PH_60.value])],
                                                    'H3K14ac': [float(line[EXPNUC_EDGEUMAN.H3K14AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K14AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K14AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K14AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K14AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K14AC_60.value])],
                                                    'H3K18ac': [float(line[EXPNUC_EDGEUMAN.H3K18AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K18AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K18AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K18AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K18AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K18AC_60.value])],
                                                    'H3K23ac': [float(line[EXPNUC_EDGEUMAN.H3K23AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K23AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K23AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K23AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K23AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K23AC_60.value])],
                                                    'H3K27ac': [float(line[EXPNUC_EDGEUMAN.H3K27AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K27AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K27AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K27AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K27AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K27AC_60.value])],
                                                    'H3K36me': [float(line[EXPNUC_EDGEUMAN.H3K36ME_0.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME_4.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME_8.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME_15.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME_30.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME_60.value])],
                                                    'H3K36me2': [float(line[EXPNUC_EDGEUMAN.H3K36ME2_0.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME2_4.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME2_8.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME2_15.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME2_30.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME2_60.value])],
                                                    'H3K36me3': [float(line[EXPNUC_EDGEUMAN.H3K36ME3_0.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME3_4.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME3_8.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME3_15.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME3_30.value]),float(line[EXPNUC_EDGEUMAN.H3K36ME3_60.value])],
                                                    'H3K4ac': [float(line[EXPNUC_EDGEUMAN.H3K4AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K4AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K4AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K4AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K4AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K4AC_60.value])],
                                                    'H3K4me': [float(line[EXPNUC_EDGEUMAN.H3K4ME_0.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME_4.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME_8.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME_15.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME_30.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME_60.value])],
                                                    'H3K4me2': [float(line[EXPNUC_EDGEUMAN.H3K4ME2_0.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME2_4.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME2_8.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME2_15.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME2_30.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME2_60.value])],
                                                    'H3K4me3': [float(line[EXPNUC_EDGEUMAN.H3K4ME3_0.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME3_4.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME3_8.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME3_15.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME3_30.value]),float(line[EXPNUC_EDGEUMAN.H3K4ME3_60.value])],
                                                    'H3K56ac': [float(line[EXPNUC_EDGEUMAN.H3K56AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K56AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K56AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K56AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K56AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K56AC_60.value])],
                                                    'H3K79me': [float(line[EXPNUC_EDGEUMAN.H3K79ME_0.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME_4.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME_8.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME_15.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME_30.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME_60.value])],
                                                    'H3K79me3': [float(line[EXPNUC_EDGEUMAN.H3K79ME3_0.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME3_4.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME3_8.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME3_15.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME3_30.value]),float(line[EXPNUC_EDGEUMAN.H3K79ME3_60.value])],
                                                    'H3K9ac': [float(line[EXPNUC_EDGEUMAN.H3K9AC_0.value]),float(line[EXPNUC_EDGEUMAN.H3K9AC_4.value]),float(line[EXPNUC_EDGEUMAN.H3K9AC_8.value]),float(line[EXPNUC_EDGEUMAN.H3K9AC_15.value]),float(line[EXPNUC_EDGEUMAN.H3K9AC_30.value]),float(line[EXPNUC_EDGEUMAN.H3K9AC_60.value])],
                                                    'H3S10ph': [float(line[EXPNUC_EDGEUMAN.H3S10PH_0.value]),float(line[EXPNUC_EDGEUMAN.H3S10PH_4.value]),float(line[EXPNUC_EDGEUMAN.H3S10PH_8.value]),float(line[EXPNUC_EDGEUMAN.H3S10PH_15.value]),float(line[EXPNUC_EDGEUMAN.H3S10PH_30.value]),float(line[EXPNUC_EDGEUMAN.H3S10PH_60.value])],
                                                    'H4K12ac': [float(line[EXPNUC_EDGEUMAN.H4K12AC_0.value]),float(line[EXPNUC_EDGEUMAN.H4K12AC_4.value]),float(line[EXPNUC_EDGEUMAN.H4K12AC_8.value]),float(line[EXPNUC_EDGEUMAN.H4K12AC_15.value]),float(line[EXPNUC_EDGEUMAN.H4K12AC_30.value]),float(line[EXPNUC_EDGEUMAN.H4K12AC_60.value])],
                                                    'H4K16ac': [float(line[EXPNUC_EDGEUMAN.H4K16AC_0.value]),float(line[EXPNUC_EDGEUMAN.H4K16AC_4.value]),float(line[EXPNUC_EDGEUMAN.H4K16AC_8.value]),float(line[EXPNUC_EDGEUMAN.H4K16AC_15.value]),float(line[EXPNUC_EDGEUMAN.H4K16AC_30.value]),float(line[EXPNUC_EDGEUMAN.H4K16AC_60.value])],
                                                    'H4K20me': [float(line[EXPNUC_EDGEUMAN.H4K20ME_0.value]),float(line[EXPNUC_EDGEUMAN.H4K20ME_4.value]),float(line[EXPNUC_EDGEUMAN.H4K20ME_8.value]),float(line[EXPNUC_EDGEUMAN.H4K20ME_15.value]),float(line[EXPNUC_EDGEUMAN.H4K20ME_30.value]),float(line[EXPNUC_EDGEUMAN.H4K20ME_60.value])],
                                                    'H4K5ac': [float(line[EXPNUC_EDGEUMAN.H4K5AC_0.value]),float(line[EXPNUC_EDGEUMAN.H4K5AC_4.value]),float(line[EXPNUC_EDGEUMAN.H4K5AC_8.value]),float(line[EXPNUC_EDGEUMAN.H4K5AC_15.value]),float(line[EXPNUC_EDGEUMAN.H4K5AC_30.value]),float(line[EXPNUC_EDGEUMAN.H4K5AC_60.value])],
                                                    'H4K8ac': [float(line[EXPNUC_EDGEUMAN.H4K8AC_0.value]),float(line[EXPNUC_EDGEUMAN.H4K8AC_4.value]),float(line[EXPNUC_EDGEUMAN.H4K8AC_8.value]),float(line[EXPNUC_EDGEUMAN.H4K8AC_15.value]),float(line[EXPNUC_EDGEUMAN.H4K8AC_30.value]),float(line[EXPNUC_EDGEUMAN.H4K8AC_60.value])],
                                                    'H4R3me': [float(line[EXPNUC_EDGEUMAN.H4R3ME_0.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME_4.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME_8.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME_15.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME_30.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME_60.value])],
                                                    'H4R3me2s': [float(line[EXPNUC_EDGEUMAN.H4R3ME2S_0.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME2S_4.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME2S_8.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME2S_15.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME2S_30.value]),float(line[EXPNUC_EDGEUMAN.H4R3ME2S_60.value])],
                                                    'HTZ1': [float(line[EXPNUC_EDGEUMAN.HTZ1_0.value]),float(line[EXPNUC_EDGEUMAN.HTZ1_4.value]),float(line[EXPNUC_EDGEUMAN.HTZ1_8.value]),float(line[EXPNUC_EDGEUMAN.HTZ1_15.value]),float(line[EXPNUC_EDGEUMAN.HTZ1_30.value]),float(line[EXPNUC_EDGEUMAN.HTZ1_60.value])]
                                                }}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        nucleosome_to_gene_edges_file: str = os.path.join(self.nucleosome_list_file_name)
        with open(nucleosome_to_gene_edges_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"NUC:GSE61888_{str(line[EXPNUC_EDGEUMAN.NUC_ID.value])}", #subject id
                                  lambda line: line[EXPNUC_EDGEUMAN.SGD_ID.value],  # object id
                                  lambda line: 'biolink:located_in',  # predicate extractor
                                  lambda line: None,  # subject props
                                  lambda line: None,  # object props
                                  lambda line: {}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        
        #Goes through the file and only yields the rows in which the cosine distance is above a predefined threshold.
        """
        def cos_dist_filter(infile):
            #Header
            yield next(infile)
            for line in infile:
               if(float(line.split(',')[SOREDGECOSDIST.DISTANCE.value])<=self.cos_dist_threshold): yield line

                 
        sor_vsd_cos_dist_edges_file: str = os.path.join(self.data_path, self.sor_vsd_cos_dist_edges_file_name)
        with open(sor_vsd_cos_dist_edges_file, 'r') as fp:
            extractor.csv_extract(cos_dist_filter(fp),
                                  lambda line: line[SOREDGECOSDIST.DRUG_ID.value], #subject id
                                  lambda line: "SCENT:" + line[SOREDGECOSDIST.VERBAL_SCENT.value].replace(' ','_'),  # object id
                                  lambda line: line[SOREDGECOSDIST.PREDICATE.value],  # predicate extractor
                                  lambda line: {'categories': ['odorant','biolink:ChemicalEntity']},  # subject props
                                  lambda line: {'categories': ['verbal_scent_descriptor'],"name":line[SOREDGECOSDIST.VERBAL_SCENT.value]},  # object props
                                  lambda line: {'cosine_distance':float(line[SOREDGECOSDIST.DISTANCE.value])}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        """
        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

