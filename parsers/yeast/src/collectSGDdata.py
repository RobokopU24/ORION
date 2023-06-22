import intermine
import pandas as pd
import os
import requests as rq
import csv
import numpy as np

def main(resolution, source_data_path):

    res = resolution
    path = source_data_path
    SGDAllGenes(path)
    createLociWindows(res, path)
    SGDComplex2GOTerm(path)
    SGDGene2GOTerm(path)
    SGDGene2Phenotype(path)
    SGDGene2Pathway(path)
    SGDGene2Complex(path)

def SGDGene2GOTerm(data_directory):
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
    storage_dir = data_directory
    csv_fname = 'SGDGene2GOTerm.csv'
    with open(os.path.join(storage_dir,csv_fname), 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    gene2gotermdf = pd.read_csv(os.path.join(storage_dir,csv_fname))
    gene2gotermdf.columns = view
    gene2gotermdf[view[0]] = gene2gotermdf[view[0]].apply(lambda x: "SGD:"+str(x))
    gene2gotermdf.fillna("?",inplace=True)
    print('SGD gene2goterm Data Collected!')
    print(os.path.join(storage_dir,csv_fname))
    #gene2gotermdf.to_csv(f"//Data_services/parsers/yeast/src/{csv_fname}", encoding="utf-8-sig", index=False)
    gene2gotermdf.to_csv(os.path.join(storage_dir, csv_fname), encoding="utf-8-sig", index=False)

def SGDGene2Phenotype(data_directory):
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
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    apodf.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

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
    csv_fname = 'SGDGene2Phenotype.csv'
    with open(os.path.join(storage_dir,csv_fname), 'w', encoding="utf-8-sig", newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            datawriter.writerow(row)
    print("SGD gene2phenotype Data Collected!")

    #Join SGD data to APO identifiers and save file in current directory
    print("Joining APO IDs and SGD links to gene2phenotype table and saving...")
    gene2phenotypedf = pd.read_csv(os.path.join(storage_dir,csv_fname))

    gene2phenotypedf.columns = view
    gene2phenotypedf[view[0]] = gene2phenotypedf[view[0]].apply(lambda x: "SGD:"+str(x))
    inner_join_df= pd.merge(gene2phenotypedf, apodf, on='phenotypes.observable', how='inner')
    inner_join_df.fillna("?",inplace=True)
    print("APO IDs Assigned to SGD Phenotype Observables!")
    csv_fname = 'SGDGene2Phenotype.csv'
    print(os.path.join(storage_dir,csv_fname))
    inner_join_df.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

def SGDGene2Pathway(data_directory):
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
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    gene2pathwaydf.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

def SGDAllGenes(data_directory):
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
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    genesdf.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

def SGDGene2Complex(data_directory):
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
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    gene2complexdf.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

def SGDComplex2GOTerm(data_directory):
    #Collects all data for complexes with GO Term annotations in SGD.
    print("---------------------------------------------------\nCollecting all GO Terms for all protein complexes on SGD...\n---------------------------------------------------\n")
    from intermine.webservice import Service
    service = Service("https://yeastmine.yeastgenome.org/yeastmine/service")
    query = service.new_query("Complex")
    query.add_view("accession", "goAnnotation.ontologyTerm.identifier",
    "goAnnotation.ontologyTerm.namespace")

    view = ["accession", "goAnnotation.ontologyTerm.identifier",
    "goAnnotation.ontologyTerm.namespace","goAnnotation.qualifier"]

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
    complex2gotermdf.fillna("?",inplace=True)
    print('SGD Complex2GOTerm Data Collected!')
    csv_fname = 'SGDComplex2GOTerm.csv'
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_fname))
    complex2gotermdf.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)

def createLociWindows(resolution, data_directory):
    #Creates sliding windows of hypothetical genome locations of all Histone PTMs. 
    n = int(resolution) #Sets sliding window resolution.
    print(f"---------------------------------------------------\nCreating sliding window of resolution {n} of yeast genome loci...\n---------------------------------------------------\n")

    data = {'hisPTMid':[],'chromosomeID':[],'start':[],'end':[],'loci':[],'histoneMod':[]}

    #Reference: https://wiki.yeastgenome.org/index.php/Systematic_Sequencing_Table

    chromosome_lengths = {'chrI':230218, 'chrII':813184, 'chrIII':316620, 
    'chrIV':1531933, 'chrV':576874, 'chrVI':270161, 'chrVII':1090940, 
    'chrVIII':562643, 'chrIX':439888, 'chrX':745751, 'chrXI':666816, 
    'chrXII':1078177, 'chrXIII': 924431, 'chrXIV':784333, 'chrXV':1091291, 
    'chrXVI':948066, 'chrmt':85779}

    histonePTMs = [
                'H2AK5ac','H2AS129ph','H3K14ac','H3K18ac','H3K23ac',
                'H3K27ac','H3K36me','H3K36me2','H3K36me3','H3K4ac',
                'H3K4me','H3K4me2','H3K4me3','H3K56ac','H3K79me',
                'H3K79me3','H3K9ac','H3S10ph','H4K12ac','H4K16ac','H4K20me','H4K5ac',
                'H4K8ac','H4R3me','H4R3me2s','HTZ1'
    ]
    
    rhea_identifiers = {'H2AK5ac':None,'H2AS129ph':None,'H3K14ac':None,'H3K18ac':None,'H3K23ac':None,
                'H3K27ac':None,'H3K36me':'RHEA-COMP:9786','H3K36me2':'RHEA-COMP:9787','H3K36me3':'RHEA-COMP:15536','H3K4ac':None,
                'H3K4me':'RHEA-COMP:15543','H3K4me2':'RHEA-COMP:15540','H3K4me3':'RHEA-COMP:15537','H3K56ac':None,'H3K79me':'RHEA-COMP:15550',
                'H3K79me3':' RHEA-COMP:15552','H3K9ac':None,'H3S10ph':None,'H4K12ac':None,'H4K16ac':None,'H4K20me':'RHEA-COMP:15555','H4K5ac':None,
                'H4K8ac':None,'H4R3me':None,'H4R3me2s':None,'HTZ1':None}
    
    # Will continue to work on this mapping.
    #Get descendants of GO term "histone modification" (GO:0016570)
    HisModDescendants = rq.get(f"https://www.ebi.ac.uk/QuickGO/services/ontology/go/terms/{'GO:0016570'}/descendants").json()
    descendants = str(HisModDescendants['results'][0]['descendants']).replace("'","").replace(" ","").replace("[","").replace("]","")
    descendantNames = rq.get(f"https://www.ebi.ac.uk/QuickGO/services/ontology/go/terms/{descendants}").json()
    descendant_dict = {}
    for result in descendantNames['results']:
        descendant_dict.update({result['name']:result['id']})

    histonePTM2GO={'ptm':[],'predicate':[],'GOid':[],'GOname':[]}
    for ptm in histonePTMs:
        if 'ac' in ptm:
            mod = ['acetyl']
            notmod = []
            ptmloc = ptm.replace('ac','')
        elif 'me2s' in ptm:
            mod = ['methyl','dimethyl']
            notmod = ['monomethyl','trimethyl']
            ptmloc = ptm.replace('me2s','')
        elif 'me2' in ptm:
            mod = ['methyl','dimethyl']
            notmod = ['monomethyl','trimethyl']
            ptmloc = ptm.replace('me2','')
        elif 'me3' in ptm:
            mod = ['methyl','trimethyl']
            notmod = ['monomethyl','dimethyl']
            ptmloc = ptm.replace('me3','') 
        elif 'me' in ptm:
            mod = ['methyl']
            notmod = ['dimethyl','trimethyl']
            ptmloc = ptm.replace('me','') 
        elif 'ph' in ptm:
            mod = ['phosph','kinase']
            notmod = []
            ptmloc = ptm.replace('ph','')
        else:
            continue
        
        if 'H2A' in ptm or 'H2B' in ptm:
            query_process = f"histone {ptmloc[0:3]}-{ptmloc[3:]}"
            query_activity = f"histone {ptmloc[0:3]}{ptmloc[3:]}"
        else:
            query_process = f"histone {ptmloc[0:2]}-{ptmloc[2:]}"
            query_activity = f"histone {ptmloc[0:2]}{ptmloc[2:]}"

        pred_dict = {
            "CTD:affects_abundance_of":["regulation"],
            "CTD:increases_abundance_of":["positive regulation"],
            "CTD:decreases_abundance_of":["negative regulation"," de"],
        }
        for name in descendant_dict.keys():
            if query_process in name or query_activity in name:
                if any(x in name for x in mod):
                    if not any(x in name for x in notmod):
                        histonePTM2GO['ptm'] = histonePTM2GO['ptm'] + ["HisPTM:"+ptm]
                        histonePTM2GO['GOname'] = histonePTM2GO['GOname'] + [name]
                        histonePTM2GO['GOid'] = histonePTM2GO['GOid'] + [descendant_dict[name]]
                        if any(x in name for x in pred_dict["CTD:decreases_abundance_of"]):
                            histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + ["CTD:decreases_abundance_of"]
                        elif any(x in name for x in pred_dict["CTD:affects_abundance_of"]):
                            if any(x in name for x in pred_dict["CTD:increases_abundance_of"]):
                                histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + ["CTD:increases_abundance_of"]
                            else:
                                histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + ["CTD:affects_abundance_of"]
                        else:
                            histonePTM2GO['predicate'] = histonePTM2GO['predicate'] + ["CTD:increases_abundance_of"]
                            
    print('Histone Modifications Mapped to GO Terms!')
    csv_fname = f"HistonePTM2GO.csv"
    storage_dir = data_directory
    histonePTM2GO_df = pd.DataFrame.from_dict(histonePTM2GO)
    histonePTM2GO_df.to_csv(os.path.join(storage_dir,csv_fname), encoding="utf-8-sig", index=False)
    print(os.path.join(storage_dir,csv_fname))

    for chr in chromosome_lengths.keys():
        m = int(chromosome_lengths[chr])
        for i in range(m): #Create loci nodes for chromosomes
            if i!= 0 and i % n == 0:
                for ptm in histonePTMs:
                    data['hisPTMid'].append("BinHisPTM:" + chr + "(" + str(i-(n-1)) + "-" + str(i) + ")" + ";" + ptm)
                    data['chromosomeID'].append(str(chr))
                    data['start'].append(i-(n-1))
                    data['end'].append(i)
                    data['loci'].append(f"{str(chr)}({i-(n-1)}-{i})")
                    data['histoneMod'].append(ptm)
            
            #Handles the tail end of chromosomes.
            if i == m-1:
                for ptm in histonePTMs:
                    data['hisPTMid'].append("BinHisPTM:" + chr + "(" + str(((m//9)*9)+1) + "-" + str(m) + ")" + ";" + ptm)
                    data['chromosomeID'].append(str(chr))
                    data['start'].append(((m//9)*9)+1)
                    data['end'].append(m)
                    data['loci'].append(f"{str(chr)}({((m//9)*9)+1}-{m})")
                    data['histoneMod'].append(ptm)
    genomelocidf = pd.DataFrame(data)
    print('Histone Modifications Loci Collected!')
    csv_f1name = f"Res{n}HistoneModLoci.csv"
    storage_dir = data_directory
    print(os.path.join(storage_dir,csv_f1name))
    genomelocidf.to_csv(os.path.join(storage_dir,csv_f1name), encoding="utf-8-sig", index=False)
    
    allgenesdf = pd.read_csv(data_directory+'/SGDAllGenes.csv')
    chrome_dict = {}
    for uc in chromosome_lengths.keys():
        chrome_dict.update({uc:allgenesdf.loc[(allgenesdf['chromosome.primaryIdentifier'] == uc)]})

    mapped_genes = []
    just_windows = genomelocidf[['loci','chromosomeID','start','end']]
    just_windows = just_windows.drop_duplicates().reset_index(drop=True)
    total = len(just_windows.index)
    for idx,row in just_windows.iterrows():
        if (idx%10000)==0:
            print(f"{idx} of {total}")
        gene = chrome_dict[row['chromosomeID']].loc[(row['end'] >= chrome_dict[row['chromosomeID']]['chromosomeLocation.start']) & (row['start'] <= chrome_dict[row['chromosomeID']]['chromosomeLocation.end'])]
        
        gene = gene['primaryIdentifier'].values[:]
        if len(gene)<1:
            gene = "None"
            
        mapped_genes = mapped_genes + [gene]
        
    just_windows['mapped_genes'] = mapped_genes
    just_windows = just_windows[just_windows.mapped_genes.isin(["None"]) == False]
    just_windows = just_windows.explode('mapped_genes')
    genomelocidf = genomelocidf.merge(just_windows,how='inner',on=['chromosomeID','start','end','loci'])

    print(f"Histone Modifications Mapping Complete!")
    csv_f3name = f"HistoneMod2Gene.csv"
    print(os.path.join(storage_dir,csv_f3name))
    genomelocidf.to_csv(os.path.join(storage_dir,csv_f3name), encoding="utf-8-sig", index=False)
    
if __name__ == "__main__":
    main(150,"Data_services_storage/YeastSGDInfo")