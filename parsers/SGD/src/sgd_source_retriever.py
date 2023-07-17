import pandas as pd
import os
import requests as rq
import csv

def retrieve_sgd_files(download_destination_path: str):
    path = download_destination_path
    SGDAllGenes(path)
    SGDComplex2GOTerm(path)
    SGDGene2GOTerm(path)
    SGDGene2Phenotype(path)
    SGDGene2Pathway(path)
    SGDGene2Complex(path)


def SGDGene2GOTerm(data_directory):
    # Collects all GO Term data for all genes in SGD
    print(
        "---------------------------------------------------\nCollecting all GO Annotation data for all genes on SGD...\n---------------------------------------------------\n")
    view = ["primaryIdentifier", "secondaryIdentifier", "symbol", "featureType",
            "qualifier", "goAnnotation.ontologyTerm.identifier",
            "goAnnotation.ontologyTerm.name", "goAnnotation.ontologyTerm.namespace",
            "goAnnotation.evidence.code.code", "goAnnotation.qualifier",
            "goAnnotation.evidence.code.withText", "goAnnotation.annotationExtension",
            "goAnnotation.evidence.code.annotType",
            "goAnnotation.evidence.publications.pubMedId",
            "goAnnotation.evidence.publications.citation"]

    # Request all gene2GOTerm data.
    rqgene2goterm = rq.get(
        f"https://yeastmine.yeastgenome.org/yeastmine/service/template/results?name=Gene_GO&constraint1=Gene&op1=LOOKUP&value1=**&extra1=&format=csv")

    # Parse as CSV object.
    lines = rqgene2goterm.text.splitlines()
    reader = csv.reader(lines)

    # Save Result
    storage_dir = data_directory
    csv_fname = 'SGDGene2GOTerm.csv'
    with open(os.path.join(storage_dir, csv_fname), 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    gene2gotermdf = pd.read_csv(os.path.join(storage_dir, csv_fname))
    gene2gotermdf.columns = view
    gene2gotermdf[view[0]] = gene2gotermdf[view[0]].apply(lambda x: "SGD:" + str(x))
    gene2gotermdf.fillna("?", inplace=True)
    print('SGD gene2goterm Data Collected!')
    print(os.path.join(storage_dir, csv_fname))
    # gene2gotermdf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)
    gene2gotermdf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)


def SGDGene2Phenotype(data_directory):
    # Gets all APO ids for phenotypes and saves as csv
    print(
        "---------------------------------------------------\nCollecting all phenotype data for all genes on SGD...\n---------------------------------------------------\n")
    print("Collecting all phenotype observable APO ids..."
          )
    r = rq.get(url="http://ontologies.berkeleybop.org/apo.obo")

    response = r.text

    names = [x.replace("name: ", "") for x in response.split("\n") if "name:" in x]
    identifiers = [y.replace("id: ", "") for y in response.split("\n") if "id:" in y and "_id" not in y]
    references = ["https://www.yeastgenome.org/observable/" + z for z in identifiers]

    apo_dict = {'phenotypes.observable': names,
                'identifier': identifiers,
                'reference': references}

    apodf = pd.DataFrame(data=apo_dict)
    print('APO IDs Collected!')
    csv_fname = 'yeast_phenotype_APO_identifiers.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir, csv_fname))
    apodf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)

    # Collects all phenotype data for all genes in SGD
    view = ["primaryIdentifier", "secondaryIdentifier", "symbol", "sgdAlias",
            "qualifier", "phenotypes.experimentType", "phenotypes.mutantType",
            "phenotypes.observable", "phenotypes.qualifier", "phenotypes.allele",
            "phenotypes.alleleDescription", "phenotypes.strainBackground",
            "phenotypes.chemical", "phenotypes.condition", "phenotypes.details",
            "phenotypes.reporter", "phenotypes.publications.pubMedId",
            "phenotypes.publications.citation"]

    # Request all gene2phenotype data.
    rqgene2phenotype = rq.get(
        "https://yeastmine.yeastgenome.org/yeastmine/service/template/results?name=Phenotype_Tab_New&format=csv")

    # Parse as CSV object.
    lines = rqgene2phenotype.text.splitlines()
    reader = csv.reader(lines)

    # Save Result
    csv_fname = 'SGDGene2Phenotype.csv'
    with open(os.path.join(storage_dir, csv_fname), 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    print("SGD gene2phenotype Data Collected!")

    # Join SGD data to APO identifiers and save file in current directory
    print("Joining APO IDs and SGD links to gene2phenotype table and saving...")
    gene2phenotypedf = pd.read_csv(os.path.join(storage_dir, csv_fname))

    gene2phenotypedf.columns = view
    gene2phenotypedf[view[0]] = gene2phenotypedf[view[0]].apply(lambda x: "SGD:" + str(x))
    inner_join_df = pd.merge(gene2phenotypedf, apodf, on='phenotypes.observable', how='inner')
    inner_join_df.fillna("?", inplace=True)
    print("APO IDs Assigned to SGD Phenotype Observables!")
    csv_fname = 'SGDGene2Phenotype.csv'
    print(os.path.join(storage_dir, csv_fname))
    inner_join_df.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)


def SGDGene2Pathway(data_directory):
    # Collects all pathway data for all genes in SGD
    print(
        "---------------------------------------------------\nCollecting all pathway data for all genes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Gene")
    query.add_view("primaryIdentifier", "organism.shortName", "pathways.identifier", "pathways.name")

    view = ["primaryIdentifier", "organism.shortName", "pathways.identifier", "pathways.name", "pathways.link"]
    data = dict.fromkeys(view, [])
    url = "https://pathway.yeastgenome.org/YEAST/new-image?type=PATHWAY&detail-level=2&object="

    for row in query.rows():
        for col in range(len(view)):
            key = view[col]
            if key != "pathways.link":
                if key == "pathways.identifier":
                    value = "PTWY:" + str(row[key])
                elif key == "primaryIdentifier":
                    value = "SGD:" + str(row[key])
                else:
                    value = row[key]
            elif key == "pathways.link":
                value = url + str(row[view[2]])
            data[key] = data[key] + [value]
    gene2pathwaydf = pd.DataFrame(data)
    gene2pathwaydf.fillna("?", inplace=True)
    print('SGD gene2pathway Data Collected!')
    csv_fname = 'SGDGene2Pathway.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir, csv_fname))
    gene2pathwaydf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)


def SGDAllGenes(data_directory):
    # Collects all genes in SGD.
    print(
        "---------------------------------------------------\nCollecting all genes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Gene")
    query.add_view("primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
                   "briefDescription", "chromosome.primaryIdentifier",
                   "chromosomeLocation.start", "chromosomeLocation.end",
                   "chromosomeLocation.strand", "organism.shortName", "featureType")

    view = ["primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
            "briefDescription", "chromosome.primaryIdentifier",
            "chromosomeLocation.start", "chromosomeLocation.end",
            "chromosomeLocation.strand", "organism.shortName", "featureType", "gene.link"]

    data = dict.fromkeys(view, [])
    url = "https://www.yeastgenome.org/locus/"

    for row in query.rows():
        for col in range(len(view)):
            key = view[col]
            if key != "gene.link":
                if key == "primaryIdentifier":
                    value = "SGD:" + str(row[key])
                else:
                    value = row[key]
            elif key == "gene.link":
                value = url + str(row[view[0]])
            data[key] = data[key] + [value]
    genesdf = pd.DataFrame(data)
    genesdf.fillna("?", inplace=True)
    print('SGD Gene Data Collected!')
    csv_fname = 'SGDAllGenes.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir, csv_fname))
    genesdf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)


def SGDGene2Complex(data_directory):
    # Collects all data for genes involved in protein complexes in SGD.
    print(
        "---------------------------------------------------\nCollecting all protein complex data for all genes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Complex")
    query.add_constraint("allInteractors.participant", "Protein")
    query.add_view("name", "function", "systematicName", "allInteractors.participant.symbol",
                   "allInteractors.participant.secondaryIdentifier",
                   "allInteractors.biologicalRole", "allInteractors.stoichiometry",
                   "allInteractors.type", "identifier", "properties", "accession",
                   "allInteractors.participant.genes.primaryIdentifier")

    view = ["name", "function", "systematicName", "allInteractors.participant.symbol",
            "allInteractors.participant.secondaryIdentifier",
            "allInteractors.biologicalRole", "allInteractors.stoichiometry",
            "allInteractors.type", "identifier", "properties", "accession",
            "allInteractors.participant.genes.primaryIdentifier", "complex.link"]

    data = dict.fromkeys(view, [])
    url = "https://www.yeastgenome.org/complex/"

    for row in query.rows():
        for col in range(len(view)):
            key = view[col]
            if key != "complex.link":
                if key == "identifier":
                    value = "EBI:" + str(row[key])
                elif key == "allInteractors.participant.genes.primaryIdentifier":
                    value = "SGD:" + str(row[key])
                else:
                    value = row[key]
            elif key == "complex.link":
                value = url + str(row[view[10]])
            data[key] = data[key] + [value]
    gene2complexdf = pd.DataFrame(data)
    gene2complexdf.fillna("?", inplace=True)
    print('SGD Gene2Complex Data Collected!')
    csv_fname = 'SGDGene2Complex.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir, csv_fname))
    gene2complexdf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)


def SGDComplex2GOTerm(data_directory):
    # Collects all data for complexes with GO Term annotations in SGD.
    print(
        "---------------------------------------------------\nCollecting all GO Terms for all protein complexes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Complex")
    query.add_view("accession", "goAnnotation.ontologyTerm.identifier",
                   "goAnnotation.ontologyTerm.namespace")

    view = ["accession", "goAnnotation.ontologyTerm.identifier",
            "goAnnotation.ontologyTerm.namespace", "goAnnotation.qualifier"]

    data = dict.fromkeys(view, [])

    for row in query.rows():
        for col in view:

            if col == "accession":
                value = "CPX:" + str(row[col])
            elif col == "goAnnotation.qualifier":
                if str(row["goAnnotation.ontologyTerm.namespace"]) == "molecular_function":
                    value = "biolink:enables"
                elif str(row["goAnnotation.ontologyTerm.namespace"]) == "biological_process":
                    value = "biolink:actively_involved_in"
                elif str(row["goAnnotation.ontologyTerm.namespace"]) == "cellular_component":
                    value = "biolink:located_in"
            else:
                value = str(row[col])
            data[col] = data[col] + [value]
    complex2gotermdf = pd.DataFrame(data)
    complex2gotermdf.fillna("?", inplace=True)
    print('SGD Complex2GOTerm Data Collected!')
    csv_fname = 'SGDComplex2GOTerm.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir, csv_fname))
    complex2gotermdf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)