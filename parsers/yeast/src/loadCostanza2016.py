import os
import enum
import pandas as pd

from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, PUBLICATIONS
from Common.prefixes import PUBMED
from intermine.webservice import Service


# Costanza 2016 Yeast Genetic Interactions
class COSTANZA_GENEINTERACTIONS(enum.IntEnum):
    GENE1 = 0
    GENE2 = 21
    EVIDENCEPMID = 8
    PREDICATE = 14
    PVALUE = 17
    SGASCORE = 18
    GENE1ALLELE = 19
    GENE2ALLELE = 20


##############
# Class: Mapping Costanza 2016 Genetic Interaction Data to Phenotypes
#
# By: Jon-Michael Beasley
# Date: 05/08/2023
##############
class Costanza2016Loader(SourceDataLoader):

    source_id: str = 'Costanza2016'
    provenance_id: str = 'infores:CostanzaGeneticInteractions'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.costanza_genetic_interactions_file_name = "Costanza2016GeneticInteractions.csv"
        
        self.data_files = [
            self.costanza_genetic_interactions_file_name
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
        # Collects all data for complexes with GO Term annotations in SGD.
        self.logger.debug(
            "---------------------------------------------------\nCollecting all Costanza 2016 dataset of yeast genetic interactions...\n---------------------------------------------------\n")
        service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
        query = service.new_query("Gene")
        query.add_constraint("interactions.participant2", "Gene")
        query.add_view(
            "primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
            "interactions.details.annotationType", "interactions.details.phenotype",
            "interactions.details.role1",
            "interactions.details.experiment.publication.pubMedId",
            "interactions.details.experiment.publication.citation",
            "interactions.details.experiment.publication.title",
            "interactions.details.experiment.publication.journal",
            "interactions.participant2.symbol",
            "interactions.participant2.secondaryIdentifier",
            "interactions.details.experiment.interactionDetectionMethods.identifier",
            "interactions.details.experiment.name",
            "interactions.details.relationshipType",
            "interactions.alleleinteractions.pvalue",
            "interactions.alleleinteractions.sgaScore",
            "interactions.alleleinteractions.allele1.name",
            "interactions.alleleinteractions.allele2.name",
            "interactions.participant2.primaryIdentifier"
        )
        query.add_constraint("interactions.details.experiment.publication", "LOOKUP", "27708008", code="A")
        query.outerjoin("interactions.alleleinteractions")

        view = [
            "primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
            "interactions.details.annotationType", "interactions.details.phenotype",
            "interactions.details.role1",
            "interactions.details.experiment.publication.pubMedId",
            "interactions.details.experiment.publication.citation",
            "interactions.details.experiment.publication.title",
            "interactions.details.experiment.publication.journal",
            "interactions.participant2.symbol",
            "interactions.participant2.secondaryIdentifier",
            "interactions.details.experiment.interactionDetectionMethods.identifier",
            "interactions.details.experiment.name",
            "interactions.details.relationshipType",
            "interactions.alleleinteractions.pvalue",
            "interactions.alleleinteractions.sgaScore",
            "interactions.alleleinteractions.allele1.name",
            "interactions.alleleinteractions.allele2.name",
            "interactions.participant2.primaryIdentifier"
        ]

        data = dict.fromkeys(view, [])
        total = query.size()
        idx = 0
        for row in query.rows():

            """
            if (idx % 10000) == 0:
                print(f"{idx} of {total}")
                #####
                if idx != 0:
                    break
                #####
            """
            for col in range(len(view)):
                key = view[col]
                if key == "primaryIdentifier":
                    value = "SGD:" + str(row[key])
                elif key == "interactions.participant2.primaryIdentifier":
                    value = "SGD:" + str(row[key])
                elif key == "interactions.details.experiment.interactionDetectionMethods.identifier":
                    if str(row[
                               "interactions.details.experiment.interactionDetectionMethods.identifier"]) == "Negative Genetic":
                        value = "biolink:negatively_correlated_with"
                    elif str(row[
                                 "interactions.details.experiment.interactionDetectionMethods.identifier"]) == "Positive Genetic":
                        value = "biolink:positively_correlated_with"
                else:
                    value = str(row[key])
                data[key] = data[key] + [value]
            idx += 1
        Costanza2016GeneticInteractions = pd.DataFrame(data)
        Costanza2016GeneticInteractions.fillna("?", inplace=True)
        Costanza2016GeneticInteractions.to_csv(os.path.join(self.data_path, self.costanza_genetic_interactions_file_name), encoding="utf-8-sig", index=False)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor()

        # Costanza Genetic Interactions Parser. Add edges between "fitness" and the yeast genotype.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: "APO:0000216",  # object id # In this case, APO:0000216 is "fitness"
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.PREDICATE.value],  # predicate extractor
                                  lambda line: {
                                                    'name': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                    'categories': ['biolink:Genotype'],
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                    'gene2_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value]
                                                }, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'p-value': line[COSTANZA_GENEINTERACTIONS.PVALUE.value],
                                                    'sgaScore': line[COSTANZA_GENEINTERACTIONS.SGASCORE.value],
                                                    PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"
                                                }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        # Costanza Genetic Interactions Parser. Genotype to Gene 1 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                    PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"

                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            
        # Costanza Genetic Interactions Parser. Genotype to Gene 2 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2.value].replace("SGD:",""),  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE2.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {
                                                    'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                    PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                    PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"

                                  }, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)        
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata