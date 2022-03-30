from operator import ge
import intermine
import pandas as pd
import os
import requests as rq
import json
import csv
import sys

def main():
    checking_input = True
    while checking_input == True:
        args = input(f"Please specify one or more of the following files to download from SGD: \n\n\
GenomeLoci, AllGenes, Gene2GOTerm, Gene2Phenotype, Gene2Pathway, Gene2Complex. Otherwise type 'Everything' to download all files. Include 'Exit' to stop.\n\n\
Which file(s) would you like? ")
        run = False
        if "GenomeLoci" in args:
            res = input("Set resolution of genome loci: ")
            createLociWindows(res)
            run = True
        else:
            pass
        if "AllGenes" in args:
            SGDAllGenes()
            run = True
        else:
            pass
        if "Gene2GOTerm" in args:
            SGDGene2GOTerm()
            run = True
        else:
            pass
        if "Gene2Phenotype" in args:
            SGDGene2Phenotype()
            run = True
        else:
            pass
        if "Gene2Pathway" in args:
            SGDGene2Pathway()
            run = True
        else:
            pass
        if "Gene2Complex" in args:
            SGDGene2Complex()
            run = True
        else:
            pass
        if "Everything" in args:
            res = input("Set resolution of genome loci: ")
            createLociWindows(res)
            SGDAllGenes()
            SGDGene2GOTerm()
            SGDGene2Phenotype()
            SGDGene2Pathway()
            SGDGene2Complex()
            run = True
        else:
            pass
        if "Exit" in args:
            break
        else:
            pass
        if run == False:
            print("Sorry, input not recognized...\n")
            continue
        else:
            continue

def SGDGene2GOTerm():
    #Collects all GO Term data for all genes in SGD
    print("---------------------------------------------------\nCollecting all GO Annotation data for all genes on SGD...\n---------------------------------------------------\n")
    view =["primaryIdentifier", "secondaryIdentifier", "symbol", "featureType",
        "qualifier", "goAnnotation.ontologyTerm.identifier",
        "goAnnotation.ontologyTerm.name", "goAnnotation.ontologyTerm.namespace",
        "goAnnotation.evidence.code.code", "goAnnotation.qualifier",
        "goAnnotation.evidence.code.withText", "goAnnotation.annotationExtension",
        "goAnnotation.evidence.code.annotType",
        "goAnnotation.evidence.publications.pubMedId",
        "goAnnotation.evidence.publications.citation"]

    #Request all gene2GOTerm data.
    rqgene2goterm= rq.get(f"https://yeastmine.yeastgenome.org/yeastmine/service/template/results?name=Gene_GO&constraint1=Gene&op1=LOOKUP&value1=**&extra1=&format=csv")

    # Parse as CSV object.
    lines = rqgene2goterm.text.splitlines()
    reader = csv.reader(lines)

    # Save Result
    with open('//Data_services/parsers/yeast/src/SGDGene2GOTerm.csv', 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    gene2gotermdf = pd.read_csv('//Data_services/parsers/yeast/src/SGDGene2GOTerm.csv')
    gene2gotermdf.columns = view
    gene2gotermdf[view[0]] = gene2gotermdf[view[0]].apply(lambda x: "SGD:"+str(x))
    gene2gotermdf.fillna("?",inplace=True)
    print('SGD gene2goterm Data Collected!')
    csv_fname = 'SGDGene2GOTerm.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    gene2gotermdf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

def SGDGene2Phenotype():
    #Gets all APO ids for phenotypes and saves as csv
    print("---------------------------------------------------\nCollecting all phenotype data for all genes on SGD...\n---------------------------------------------------\n")
    print("Collecting all phenotype observable APO ids..."
)
    r = rq.get(url="http://ontologies.berkeleybop.org/apo.obo")

    response = r.text

    names = [x.replace("name: ", "") for x in response.split("\n") if "name:" in x]
    identifiers = [y.replace("id: ", "") for y in response.split("\n") if "id:" in y and "_id" not in y]
    references = ["https://www.yeastgenome.org/observable/" + z for z in identifiers]

    apo_dict = {'phenotypes.observable':names,
        'identifier':identifiers,
        'reference': references}
        
    apodf = pd.DataFrame(data=apo_dict)
    print('APO IDs Collected!')
    csv_fname = 'yeast_phenotype_APO_identifiers.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    apodf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

    #Collects all phenotype data for all genes in SGD
    view =["primaryIdentifier", "secondaryIdentifier", "symbol", "sgdAlias",
        "qualifier", "phenotypes.experimentType", "phenotypes.mutantType",
        "phenotypes.observable", "phenotypes.qualifier", "phenotypes.allele",
        "phenotypes.alleleDescription", "phenotypes.strainBackground",
        "phenotypes.chemical", "phenotypes.condition", "phenotypes.details",
        "phenotypes.reporter", "phenotypes.publications.pubMedId",
        "phenotypes.publications.citation"]

    #Request all gene2phenotype data.
    rqgene2phenotype = rq.get("https://yeastmine.yeastgenome.org/yeastmine/service/template/results?name=Phenotype_Tab_New&format=csv")

    # Parse as CSV object.
    lines = rqgene2phenotype.text.splitlines()
    reader = csv.reader(lines)

    # Save Result
    with open('//Data_services/parsers/yeast/src/SGDGene2Phenotype.csv', 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    print("SGD gene2phenotype Data Collected!")

    #Join SGD data to APO identifiers and save file in current directory
    print("Joining APO IDs and SGD links to gene2phenotype table and saving...")
    gene2phenotypedf = pd.read_csv('//Data_services/parsers/yeast/src/SGDGene2Phenotype.csv')

    gene2phenotypedf.columns = view
    gene2phenotypedf[view[0]] = gene2phenotypedf[view[0]].apply(lambda x: "SGD:"+str(x))
    inner_join_df= pd.merge(gene2phenotypedf, apodf, on='phenotypes.observable', how='inner')
    inner_join_df.fillna("?",inplace=True)
    print("APO IDs Assigned to SGD Phenotype Observables!")
    csv_fname = 'SGDGene2Phenotype.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    inner_join_df.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

def SGDGene2Pathway():
    #Collects all pathway data for all genes in SGD
    print("---------------------------------------------------\nCollecting all pathway data for all genes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Gene")
    query.add_view("primaryIdentifier","organism.shortName", "pathways.identifier", "pathways.name")

    view =["primaryIdentifier","organism.shortName", "pathways.identifier", "pathways.name", "pathways.link"]
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
    gene2pathwaydf.fillna("?",inplace=True)
    print('SGD gene2pathway Data Collected!')
    csv_fname = 'SGDGene2Pathway.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    gene2pathwaydf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

def SGDAllGenes():
    #Collects all genes in SGD.
    print("---------------------------------------------------\nCollecting all genes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Gene")
    query.add_view("primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
        "briefDescription", "chromosome.primaryIdentifier",
        "chromosomeLocation.start", "chromosomeLocation.end",
        "chromosomeLocation.strand", "organism.shortName", "featureType")

    view =["primaryIdentifier", "secondaryIdentifier", "symbol", "name", "sgdAlias",
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
    genesdf.fillna("?",inplace=True)
    print('SGD Gene Data Collected!')
    csv_fname = 'SGDAllGenes.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    genesdf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

def SGDGene2Complex():
    #Collects all data for genes involved in protein complexes in SGD.
    print("---------------------------------------------------\nCollecting all protein complex data for all genes on SGD...\n---------------------------------------------------\n")
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
    gene2complexdf.fillna("?",inplace=True)
    print('SGD Gene2Complex Data Collected!')
    csv_fname = 'SGDGene2Complex.csv'
    print(os.path.join(os.getcwd(),csv_fname))
    gene2complexdf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

def createLociWindows(resolution):
    #Creates sliding windows of hypothetical genome locations. 
    n = int(resolution) #Sets sliding window resolution.
    print(f"---------------------------------------------------\nCreating sliding window of resolution {n} of yeast genome loci...\n---------------------------------------------------\n")

    data = {'lociID':[],'chromosomeID':[],'start':[],'end':[]}

    #Reference: https://wiki.yeastgenome.org/index.php/Systematic_Sequencing_Table
    chromosome_lengths = {'chrI':230218, 'chrII':813184, 'chrIII':316620, 
    'chrIV':1531933, 'chrV':576874, 'chrVI':270161, 'chrVII':1090940, 
    'chrVIII':562643, 'chrIX':439888, 'chrX':745751, 'chrXI':666816, 
    'chrXII':1078177, 'chrXIII': 924431, 'chrXIV':784333, 'chrXV':1091291, 
    'chrXVI':948066, 'chrmt':85779}

    for chr in chromosome_lengths.keys():
        m = int(chromosome_lengths[chr])
        for i in range(m-1): #Create loci nodes for chromosomes
            if i!= 0 and i % n == 0:
                data['chromosomeID'].append(str(chr))
                data['start'].append(i-(n-1))
                data['end'].append(i)
                data['lociID'].append("LOC:" + chr + "-" + str(i-(n-1)) + "-" + str(i))
            
            #Handles the tail end of chromosomes.
            if i == m-1:
                data['chromosomeID'].append(str(chr))
                data['start'].append(((m//9)*9)+1)
                data['end'].append(m)
                data['lociID'].append("LOC:" + chr + "-" + str(((m//9)*9)+1) + "-" + str(m))

    genomelocidf = pd.DataFrame(data)
    print('Genome Loci Collected!')
    csv_fname = f"Res{n}GenomeLoci.csv"
    print(os.path.join(os.getcwd(),csv_fname))
    genomelocidf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)

if __name__ == "__main__":
    main()