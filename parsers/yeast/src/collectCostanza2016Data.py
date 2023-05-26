import intermine
import pandas as pd
import os
import requests as rq
import csv
import numpy as np

def main(source_data_path):

    path = source_data_path
    SGDCostanza2016GeneticInteractions(path)

def SGDCostanza2016GeneticInteractions(data_directory):
    #Collects all data for complexes with GO Term annotations in SGD.
    print("---------------------------------------------------\nCollecting all Costanza 2016 dataset of yeast genetic interactions...\n---------------------------------------------------\n")
    from intermine.webservice import Service
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

    view= [
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
    total=query.size()
    idx=0
    for row in query.rows():
        if (idx%10000)==0:
            print(f"{idx} of {total}")
            #####
            if idx != 0:
                break
            #####
        for col in range(len(view)):
            key = view[col]
            if key == "primaryIdentifier":
                value = "SGD:" + str(row[key])
            elif key == "interactions.participant2.primaryIdentifier":
                value = "SGD:" + str(row[key])
            elif key == "interactions.details.experiment.interactionDetectionMethods.identifier":
                if str(row["interactions.details.experiment.interactionDetectionMethods.identifier"]) == "Negative Genetic":
                    value = "biolink:negatively_correlated_with"
                elif str(row["interactions.details.experiment.interactionDetectionMethods.identifier"]) == "Positive Genetic":
                    value = "biolink:positively_correlated_with"
            else:
                value = str(row[key])
            data[key] = data[key] + [value]
        idx+=1
    Costanza2016GeneticInteractions = pd.DataFrame(data)
    Costanza2016GeneticInteractions.fillna("?",inplace=True)
    print('SGD Costanza2016GeneticInteractions Data Collected!')
    csv_fname = 'Costanza2016GeneticInteractions.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    Costanza2016GeneticInteractions.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)
    
if __name__ == "__main__":
    main("Data_services_storage/YeastCostanza2016Data")