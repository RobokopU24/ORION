import os
import enum
import csv
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
    INTERACTION_DETECTION_METHOD = 14
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
    parsing_version = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.negatively_correlated_with = 'biolink:negatively_correlated_with'
        self.positively_correlated_with = 'biolink:positively_correlated_with'

        self.yeastmine_url = "https://yeastmine.yeastgenome.org/yeastmine/service"

        self.costanza_genetic_interactions_file_name = "Costanza2016GeneticInteractions.csv"
        
        self.data_files = [
            self.costanza_genetic_interactions_file_name
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        # TODO - this is actually possible with https://yeastmine.yeastgenome.org/yeastmine/service/version/release
        return 'yeast_v1'

    def get_data(self) -> int:
        # Collects all data for complexes with GO Term annotations in SGD.
        self.logger.debug(
            "---------------------------------------------------\nCollecting all Costanza 2016 dataset of yeast genetic interactions...\n---------------------------------------------------\n")
        service = Service(self.yeastmine_url)
        query = service.new_query("Gene")
        query.add_constraint("interactions.participant2", "Gene")

        # NOTE - CAUTION - if these fields changed the indexes in COSTANZA_GENEINTERACTIONS must be changed as well
        fields = [
            "primaryIdentifier",
            "secondaryIdentifier",
            "symbol",
            "name",
            "sgdAlias",
            "interactions.details.annotationType",
            "interactions.details.phenotype",
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

        query.add_view(" ".join(fields))
        query.add_constraint("interactions.details.experiment.publication", "LOOKUP", "27708008", code="A")
        query.outerjoin("interactions.alleleinteractions")

        output_file_path = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(output_file_path, 'w', newline='') as csvfile:
            output_writer = csv.DictWriter(csvfile, fieldnames=fields)
            output_writer.writeheader()
            for row in query.rows():
                output_writer.writerow({key: row[key] for key in fields})

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """

        extractor = Extractor(self.output_file_writer)

        # Costanza Genetic Interactions Parser. Add edges between "fitness" and the yeast genotype.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"SGD:{line[COSTANZA_GENEINTERACTIONS.GENE1.value]}-{line[COSTANZA_GENEINTERACTIONS.GENE2.value]}",  # subject id
                                  lambda line: "APO:0000216",  # object id # In this case, APO:0000216 is "fitness"
                                  lambda line: self.get_costanza_predicate(line),  # predicate extractor
                                  lambda line: {'name': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value]+"-"+line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                'categories': ['biolink:Genotype'],
                                                'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                'gene2_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value]
                                                }, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {'p-value': line[COSTANZA_GENEINTERACTIONS.PVALUE.value],
                                                'sgaScore': line[COSTANZA_GENEINTERACTIONS.SGASCORE.value],
                                                PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)

        # Costanza Genetic Interactions Parser. Genotype to Gene 1 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"SGD:{line[COSTANZA_GENEINTERACTIONS.GENE1.value]}-{line[COSTANZA_GENEINTERACTIONS.GENE2.value]}",  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE1.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {'gene1_allele': line[COSTANZA_GENEINTERACTIONS.GENE1ALLELE.value],
                                                PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
            
        # Costanza Genetic Interactions Parser. Genotype to Gene 2 edge.
        costanza_genetic_interactions: str = os.path.join(self.data_path, self.costanza_genetic_interactions_file_name)
        with open(costanza_genetic_interactions, 'r') as fp:
            extractor.csv_extract(fp,
                                  lambda line: f"SGD:{line[COSTANZA_GENEINTERACTIONS.GENE1.value]}-{line[COSTANZA_GENEINTERACTIONS.GENE2.value]}",  # subject id
                                  lambda line: line[COSTANZA_GENEINTERACTIONS.GENE2.value],  # object id
                                  lambda line: "biolink:has_part",  # predicate extractor
                                  lambda line: {}, # subject props
                                  lambda line: {}, # object props
                                  lambda line: {'gene2_allele': line[COSTANZA_GENEINTERACTIONS.GENE2ALLELE.value],
                                                PUBLICATIONS: [f'{PUBMED}:{line[COSTANZA_GENEINTERACTIONS.EVIDENCEPMID.value]}'],
                                                PRIMARY_KNOWLEDGE_SOURCE: "CostanzaGeneticInteractions"}, #edgeprops
                                  comment_character=None,
                                  delim=',',
                                  has_header_row=True)
        return extractor.load_metadata

    def get_costanza_predicate(self, row):
        if row[COSTANZA_GENEINTERACTIONS.INTERACTION_DETECTION_METHOD.value] == 'Negative Genetic':
            return self.negatively_correlated_with
        elif row[COSTANZA_GENEINTERACTIONS.INTERACTION_DETECTION_METHOD.value] == 'Positive Genetic':
            return self.positively_correlated_with
        else:
            self.logger.warning('Unknown INTERACTION_DETECTION_METHOD could not be converted to a predicate.')
            return None
