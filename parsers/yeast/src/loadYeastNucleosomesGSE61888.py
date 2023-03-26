import gzip
import os
import enum
import gzip
import pandas as pd
import numpy as np

from parsers.yeast.src.collectSGDdata import createLociWindows
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import AGGREGATOR_KNOWLEDGE_SOURCES, PRIMARY_KNOWLEDGE_SOURCE


# Maps Experimental Condition affects Nucleosome edge.
class EXPNUC_EDGEUMAN(enum.IntEnum):
    HISMODID = 0
    CHR_ID = 1
    START = 2
    END = 3
    HISMOD = 4
    NUC_ID = 5
    CHR = 6
    CENTER = 7
    COVERAGE_RPM = 8
    GENE = 9
    ACC = 10
    GENE_POS = 11
    H2AK5AC_0 = 12
    H2AK5AC_4 = 13
    H2AK5AC_8 = 14
    H2AK5AC_15 = 15
    H2AK5AC_30 = 16
    H2AK5AC_60 = 17
    H2AS129PH_0 = 18
    H2AS129PH_4 = 19
    H2AS129PH_8 = 20
    H2AS129PH_15 = 21
    H2AS129PH_30 = 22
    H2AS129PH_60 = 23
    H3K14AC_0 = 24
    H3K14AC_4 = 25
    H3K14AC_8 = 26
    H3K14AC_15 = 27
    H3K14AC_30 = 28
    H3K14AC_60 = 29
    H3K18AC_0 = 30
    H3K18AC_4 = 31
    H3K18AC_8 = 32
    H3K18AC_15 = 33
    H3K18AC_30 = 34
    H3K18AC_60 = 35
    H3K23AC_0 = 36
    H3K23AC_4 = 37
    H3K23AC_8 = 38
    H3K23AC_15 = 39
    H3K23AC_30 = 40
    H3K23AC_60 = 41
    H3K27AC_0 = 42
    H3K27AC_4 = 43
    H3K27AC_8 = 44
    H3K27AC_15 = 45
    H3K27AC_30 = 46
    H3K27AC_60 = 47
    H3K36ME_0 = 48
    H3K36ME_4 = 49
    H3K36ME_8 = 50
    H3K36ME_15 = 51
    H3K36ME_30 = 52
    H3K36ME_60 = 53
    H3K36ME2_0 = 54
    H3K36ME2_4 = 55
    H3K36ME2_8 = 56
    H3K36ME2_15 = 57
    H3K36ME2_30 = 58
    H3K36ME2_60 = 59
    H3K36ME3_0 = 60
    H3K36ME3_4 = 61
    H3K36ME3_8 = 62
    H3K36ME3_15 = 63
    H3K36ME3_30 = 64
    H3K36ME3_60 = 65
    H3K4AC_0 = 66
    H3K4AC_4 = 67
    H3K4AC_8 = 68
    H3K4AC_15 = 69
    H3K4AC_30 = 70
    H3K4AC_60 = 71
    H3K4ME_0 = 72
    H3K4ME_4 = 73
    H3K4ME_8 = 74
    H3K4ME_15 = 75
    H3K4ME_30 = 76
    H3K4ME_60 = 77
    H3K4ME2_0 = 78
    H3K4ME2_4 = 79
    H3K4ME2_8 = 80
    H3K4ME2_15 = 81
    H3K4ME2_30 = 82
    H3K4ME2_60 = 83
    H3K4ME3_0 = 84
    H3K4ME3_4 = 85
    H3K4ME3_8 = 86
    H3K4ME3_15 = 87
    H3K4ME3_30 = 88
    H3K4ME3_60 = 89
    H3K56AC_0 = 90
    H3K56AC_4 = 91
    H3K56AC_8 = 92
    H3K56AC_15 = 93
    H3K56AC_30 = 94
    H3K56AC_60 = 95
    H3K79ME_0 = 96
    H3K79ME_4 = 97
    H3K79ME_8 = 98
    H3K79ME_15 = 99
    H3K79ME_30 = 100
    H3K79ME_60 = 101
    H3K79ME3_0 = 102
    H3K79ME3_4 = 103
    H3K79ME3_8 = 104
    H3K79ME3_15 = 105
    H3K79ME3_30 = 106
    H3K79ME3_60 = 107
    H3K9AC_0 = 108
    H3K9AC_4 = 109
    H3K9AC_8 = 110
    H3K9AC_15 = 111
    H3K9AC_30 = 112
    H3K9AC_60 = 113
    H3S10PH_0 = 114
    H3S10PH_4 = 115
    H3S10PH_8 = 116
    H3S10PH_15 = 117
    H3S10PH_30 = 118
    H3S10PH_60 = 119
    H4K12AC_0 = 120
    H4K12AC_4 = 121
    H4K12AC_8 = 122
    H4K12AC_15 = 123
    H4K12AC_30 = 124
    H4K12AC_60 = 125
    H4K16AC_0 = 126
    H4K16AC_4 = 127
    H4K16AC_8 = 128
    H4K16AC_15 = 129
    H4K16AC_30 = 130
    H4K16AC_60 = 131
    H4K20ME_0 = 132
    H4K20ME_4 = 133
    H4K20ME_8 = 134
    H4K20ME_15 = 135
    H4K20ME_30 = 136
    H4K20ME_60 = 137
    H4K5AC_0 = 138
    H4K5AC_4 = 139
    H4K5AC_8 = 140
    H4K5AC_15 = 141
    H4K5AC_30 = 142
    H4K5AC_60 = 143
    H4K8AC_0 = 144
    H4K8AC_4 = 145
    H4K8AC_8 = 146
    H4K8AC_15 = 147
    H4K8AC_30 = 148
    H4K8AC_60 = 149
    H4R3ME_0 = 150
    H4R3ME_4 = 151
    H4R3ME_8 = 152
    H4R3ME_15 = 153
    H4R3ME_30 = 154
    H4R3ME_60 = 155
    H4R3ME2S_0 = 156
    H4R3ME2S_4 = 157
    H4R3ME2S_8 = 158
    H4R3ME2S_15 = 159
    H4R3ME2S_30 = 160
    H4R3ME2S_60 = 161
    HTZ1_0 = 162
    HTZ1_4 = 163
    HTZ1_8 = 164
    HTZ1_15 = 165
    HTZ1_30 = 166
    HTZ1_60 = 167

    

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
class YeastGSE61888Loader(SourceDataLoader):

    source_id: str = 'YeastGSE61888'
    provenance_id: str = 'infores:Yeast'


    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.cos_dist_threshold = 1.0

        self.yeast_data_url = 'https://stars.renci.org/var/data_services/yeast/'

        self.binned_histone_mods_file = f"{self.data_path}/../../../YeastSGDInfo/yeast_v1/source/Res150HistoneModLoci.csv"
        self.GSE61888_ChIPseq = "GSE61888_nucs_normed.csv"
        
        self.data_files = [
            self.GSE61888_ChIPseq
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return 'yeast_v1_5'

    def int_to_Roman(self, num):
        val = [
            1000, 900, 500, 400,
            100, 90, 50, 40,
            10, 9, 5, 4,
            1
            ]
        syb = [
            "M", "CM", "D", "CD",
            "C", "XC", "L", "XL",
            "X", "IX", "V", "IV",
            "I"
            ]
        roman_num = ''
        i = 0
        while  num > 0:
            for _ in range(num // val[i]):
                roman_num += syb[i]
                num -= val[i]
            i += 1
        return roman_num

    def get_data(self) -> int:
        """
        Gets the GEO datasets.
        """
        binned_histones = pd.read_csv(self.binned_histone_mods_file)
        
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.yeast_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
            dataset = pd.read_csv(source_url)
            chromes = []
            for x in dataset['chr'].tolist():
                if x != None:
                    chrome = 'chr'+self.int_to_Roman(int(x))
                else:
                    chrome = ""
                chromes.append(chrome)
            dataset['chr'] = chromes
            print("----Mapping GEO Datasets to Binned Histone Modifications----")
            chrome_dict = {}
            unique_chromes = dataset['chr'].unique()

            for uc in unique_chromes:
                chrome_dict.update({uc:binned_histones.loc[(binned_histones['chromosomeID'] == uc)]})
            dataset_bins = []
            total = len(dataset.index)
            for idx,row in dataset.iterrows():
                if (idx%10000)==0:
                    print(f"{idx} of {total}")
                
                binned = chrome_dict[row['chr']].loc[(row['center'] >= chrome_dict[row['chr']]['start']) & (row['center'] <= chrome_dict[row['chr']]['end'])]

                try:
                    binned = binned['loci'].values[0]
                    if len(binned) < 1:
                        binned = "None"
                except:
                    binned = "None"
                dataset_bins = dataset_bins + [binned]

            dataset['loci'] = dataset_bins
            dataset = dataset[dataset.loci.isin(["None"]) == False]
            print(os.path.join(self.data_path,source))
            dataset.to_csv(os.path.join(self.data_path,source), encoding="utf-8-sig", index=False)
            mergedf = dataset.merge(binned_histones,how='inner',on='loci')
            for col in reversed(binned_histones.columns):
                inserted = mergedf[col]
                mergedf = mergedf.drop(columns=[col])
                mergedf.insert(loc=0, column=col, value=inserted)
            mergedf = mergedf.drop(columns=['loci'])
            print(f"Histone Modifications Mapping Complete!")
            csv_f3name = "HistoneMod2GSE61888.csv"
            mergedf.to_csv(os.path.join(self.data_path,csv_f3name), encoding="utf-8-sig", index=False)
            print(os.path.join(self.data_path,csv_f3name))
    
        return True
    
    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        #Experimental conditions to nucleosomes edges. Edges contain nucleosome occupancy and histone PTMs/Variants as properties.
        #In this case, handles only "GSE61888 High resolution chromatin dynamics during a yeast stress response" data
        histone_mods_2_stressor_file: str = os.path.join(self.data_path, "HistoneMod2GSE61888.csv")
        with open(histone_mods_2_stressor_file, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: "PUBCHEM.COMPOUND:5353800", #subject id #In this case, it is set as "Diamide", the stressor used in GSE61888
                                  lambda line: line[EXPNUC_EDGEUMAN.HISMODID.value],  # object id
                                  lambda line: "biolink:affects_molecular_modification_of",  # predicate extractor
                                  lambda line: {},  # subject props
                                  lambda line: {},  # object props
                                  lambda line: {'dataset':'GSE61888',
                                                'dataComment': "Occupancy represented as coverage measured in reads per million (rpm). \
                                                    Histone modifications measured as log2FC over unmodified state and measured at 0,4,8,15,30 and 60 minutes after diamide exposure.",
                                                'coverage':float(line[EXPNUC_EDGEUMAN.COVERAGE_RPM.value]),
                                                f"{line[EXPNUC_EDGEUMAN.HISMOD.value]}TimeSeries":[val for key,val in 
                                                {'H2AK5ac': [float(line[EXPNUC_EDGEUMAN.H2AK5AC_0.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_4.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_8.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_15.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_30.value]),float(line[EXPNUC_EDGEUMAN.H2AK5AC_60.value])],
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
                                                }.items() if line[EXPNUC_EDGEUMAN.HISMOD.value] == key],
                                                PRIMARY_KNOWLEDGE_SOURCE: "WeinerEpigenomics"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
       
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata

