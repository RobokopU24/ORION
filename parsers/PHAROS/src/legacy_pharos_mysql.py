import os
import mysql.connector
import logging
from orion.utils import LoggingUtil, GetData, NodeNormUtils, EdgeNormUtils
from pathlib import Path


# create a logger
logger = LoggingUtil.init_logging("ORION.PHAROS.PHAROSLoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

class PharosMySQL():
    def __init__(self, context):
        self.db  = mysql.connector.connect(user='tcrd', host=self.url, database='tcrd540', buffered = True)

    def gene_get_disease(self, gene_node):
        identifiers = gene_node.get_synonyms_by_prefix('HGNC')
        predicate = LabeledID(identifier='WD:P2293', label='gene_involved')
        resolved_edge_nodes = []
        for hgnc in identifiers:
            query = f"select distinct d.did,d.name from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and d.dtype <> 'Expression Atlas' and x.value = '{hgnc}'"
            cursor = self.db.cursor(dictionary = True, buffered = True)
            cursor.execute(query)
            for result in cursor:
                did = result['did']
                label = result['name']
                # query was returning None for result['did'] on HGNC:1884
                if did == None:
                    continue                
                pattern = re.compile('^C\d+$') # pattern for umls local id
                if pattern.match(did):
                    did = f'UMLS:{did}'
                if did.startswith('Orphanet:'):
                    dparts = did.split(':')
                    did = 'ORPHANET:' + dparts[1]
                disease_node = KNode(did, type=node_types.DISEASE, name=label)
                edge = self.create_edge(gene_node, disease_node, 'pharos.gene_get_disease',hgnc,predicate)
                resolved_edge_nodes.append( (edge,disease_node) )
        return resolved_edge_nodes

    def grab_edge_props(self,result):
        if result['pred'] is not None and len(result['pred']) > 1:
            rel = Text.snakify(result['pred']).lower()
        else:
            rel = 'interacts_with'
        predicate = LabeledID(identifier=f'GAMMA:{rel}', label=rel)
        if 'pubmed_ids' in result and result['pubmed_ids'] is not None:
            pmids = [ f'PMID:{r}' for r in result['pubmed_ids'].split('|')]
        else:
            pmids = []
        props = {}
        if result['affinity'] is not None:
            props['affinity'] = float(result['affinity'])
            props['affinity_parameter'] = result['affinity_parameter']
        return predicate, pmids, props

    def g2d(self,hgnc,query,chembls,resolved_edge_nodes,gene_node):
        prefixmap={'ChEMBL':'CHEMBL.COMPOUND', 'Guide to Pharmacology':'gtpo'}
        cursor = self.db.cursor(dictionary = True, buffered = True)
        cursor.execute(query)
        for result in cursor:
            label = result['drug']
            chemblid = f"{prefixmap[result['id_src']]}:{result['cid']}"
            predicate,pmids,props = self.grab_edge_props(result)
            if chemblid not in chembls:
                chembls.add(chemblid)
                drug_node = KNode(chemblid, type=node_types.CHEMICAL_SUBSTANCE, name=label)
                edge = self.create_edge(drug_node,gene_node, 'pharos.gene_get_drug',hgnc,predicate,publications=pmids,properties=props)
                resolved_edge_nodes.append( (edge,drug_node) )

    def gene_get_drug(self, gene_node):
        """ Get a drug from a gene. """
        resolved_edge_nodes = []
        identifiers = gene_node.get_synonyms_by_prefix('HGNC')
        chembls = set()
        for hgnc in identifiers:
            #Note that there are nulls in at least cmpd_chemblid (?!?!)
            #Have not fully checked all the other columns/tables.
            query1= \
            f"""SELECT DISTINCT da.drug, da.cmpd_chemblid AS cid, 'ChEMBL' AS id_src, 
            da.act_value AS affinity, da.act_type AS affinity_parameter, da.action_type AS pred
            FROM xref x, drug_activity da  
            WHERE  x.protein_id = da.target_id 
            AND da.cmpd_chemblid IS NOT NULL
            AND x.xtype='HGNC' 
            AND x.value = '{hgnc}';"""
            self.g2d(hgnc,query1,chembls,resolved_edge_nodes,gene_node)
            query2=\
            f"""SELECT DISTINCT da.cmpd_name_in_src as drug, da.cmpd_id_in_src as cid, catype AS id_src,
            da.act_value AS affinity, da.act_type as affinity_parameter, da.act_type AS pred,
            da.pubmed_ids AS pubmed_ids
            FROM xref x, cmpd_activity da  
            WHERE  x.protein_id = da.target_id 
            AND x.xtype='HGNC' 
            AND x.value = '{hgnc}';"""
            self.g2d(hgnc,query2,chembls,resolved_edge_nodes,gene_node)
        return resolved_edge_nodes

    def d2g(self, drug_node, query, resolved_edge_nodes, chembl,hgncs):
        """ Get a gene from a drug. """
        cursor = self.db.cursor(dictionary = True, buffered = True)
        cursor.execute(query)
        for result in cursor:
            label = result['sym']
            hgnc = result['value']
            if hgnc not in hgncs:
                hgncs.add(hgnc)
                predicate,pmids,props = self.grab_edge_props(result)
                gene_node = KNode(hgnc, type=node_types.GENE, name=label)
                edge = self.create_edge(drug_node,gene_node, 'pharos.drug_get_gene',chembl,predicate,publications=pmids,properties=props)
                resolved_edge_nodes.append( (edge,gene_node) )

    def drug_get_gene(self, drug_node):
        """ Get a gene from a drug. """
        resolved_edge_nodes = []
        identifiers = drug_node.get_synonyms_by_prefix('CHEMBL.COMPOUND')
        hgncs = set()
        for chembl in identifiers:
            query=f"""SELECT DISTINCT x.value, p.sym,
            da.act_value AS affinity, da.act_type AS affinity_parameter, da.action_type AS pred
            FROM xref x, drug_activity da, protein p 
            WHERE da.target_id = x.protein_id 
            AND da.cmpd_chemblid='{Text.un_curie(chembl)}' 
            AND x.xtype='HGNC' 
            AND da.target_id = p.id;"""
            self.d2g(drug_node, query, resolved_edge_nodes, chembl,hgncs)
            query=f"""SELECT DISTINCT x.value, p.sym, 
            da.act_value AS affinity, da.act_type as affinity_parameter, da.act_type AS pred,
            da.pubmed_ids AS pubmed_ids
            FROM xref x, cmpd_activity da, protein p
            WHERE da.target_id = x.protein_id 
            AND da.cmpd_id_in_src='{Text.un_curie(chembl)}' 
            AND x.xtype='HGNC' 
            AND da.target_id = p.id;"""
            self.d2g(drug_node, query, resolved_edge_nodes, chembl,hgncs)
        return resolved_edge_nodes


#select distinct x.value  from disease d join xref x on x.protein_id = d.target_id where x.xtype = 'HGNC' and d.did='DOID:5572' order by x.value;
    def disease_get_gene(self, disease_node):
        """ Get a gene from a pharos disease id."""
        resolved_edge_nodes = []
        hgncs = set()
        # WD:P2293 gene assoc with condition.
        # domain is gene and range is disease or phenotype for this relationship
        predicate = LabeledID(identifier='WD:P2293', label='gene_involved')
        #Pharos contains multiple kinds of disease identifiers in its disease table:
        # For OMIM identifiers, they can have either prefix OMIM or MIM
        # UMLS doen't have any prefixes.... :(
        pharos_predicates = {'DOID':('DOID',),'UMLS':(None,),'MESH':('MESH',),'OMIM':('OMIM','MIM'),'ORPHANET':('Orphanet',)}
        for ppred,dbpreds in pharos_predicates.items():
            pharos_candidates = [Text.un_curie(x) for x in disease_node.get_synonyms_by_prefix(ppred)]
            for dbpred in dbpreds:
                if dbpred is None:
                    pharos_ids = pharos_candidates
                else:
                    pharos_ids = [f'{dbpred}:{x}' for x in pharos_candidates]
                    for pharos_id in pharos_ids:
                        cursor = self.db.cursor(dictionary = True, buffered = True)
                        query = f"select distinct x.value, p.sym  from disease d join xref x on x.protein_id = d.target_id join protein p on d.target_id = p.id where x.xtype = 'HGNC' and d.dtype <> 'Expression Atlas' and d.did='{pharos_id}';"
                        cursor.execute(query)
                        for result in cursor:
                            label = result['sym']
                            hgnc = result['value']
                            if hgnc not in hgncs:
                                hgncs.add(hgnc)
                                gene_node = KNode(hgnc, type=node_types.GENE, name=label)
                                edge = self.create_edge(gene_node, disease_node, 'pharos.disease_get_gene', pharos_id, predicate)
                                resolved_edge_nodes.append((edge, gene_node))
        return resolved_edge_nodes
