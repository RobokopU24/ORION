import os
import argparse
import logging
import requests
import re
import xml.etree.cElementTree as E_Tree

from bs4 import BeautifulSoup
from zipfile import ZipFile
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: HMDB metabolites loader
#
# By: Phil Owen
# Date: 3/31/2021
# Desc: Class that loads/parses the HMDB data.
##############
class HMDBLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path: str = os.environ['DATA_SERVICES_STORAGE']
        self.data_file: str = 'hmdb_metabolites.zip'
        self.test_mode: bool = test_mode
        self.source_id: str = 'HMDB'
        self.source_db: str = 'Human Metabolome Database'
        self.provenance_id: str = 'infores:hmdb'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.HMDB.HMDBLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        # init the return
        ret_val: str = 'Not found'

        # load the web page for CTD
        html_page: requests.Response = requests.get('https://hmdb.ca/downloads')

        # get the html into a parsable object
        resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

        # init the search text
        search_text = 'Current Version '

        # find the version div area
        div_tag = resp.find('a', string=re.compile('Current Version'))

        # did we find version data
        if len(div_tag) > 0:
            ret_val = div_tag.text.split(search_text)[1].strip('() ')

        # return to the caller
        return ret_val

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=None)

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'],
                                       object_id=edge['object'],
                                       relation=edge['relation'],
                                       original_knowledge_source=self.provenance_id,
                                       edge_properties=edge['properties'])

    def get_hmdb_data(self) -> int:
        """
        Gets the hmdb data.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            file_count: int = gd.pull_via_http('https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip', self.data_path)
        else:
            file_count: int = 1

        # return the file count to the caller
        return file_count

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        parses the HMDB data file gathered from https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return the parsed metadata stats
        """

        self.logger.info(f'HMDBloader - Start of HMDB data processing.')

        # get the data to process
        file_count = self.get_hmdb_data()

        # init the return
        load_metadata: dict = {}

        # did we get the archive
        if file_count == 1:
            self.logger.debug(f'{self.data_file} archive retrieved. Parsing HMDB data.')

            # parse the data
            load_metadata = self.parse_data_file(self.data_path, self.data_file)

            self.logger.info(f'HMDBLoader - {self.data_file} Processing complete.')

            # write out the data
            self.write_to_file(nodes_output_file_path, edges_output_file_path)

            self.logger.info(f'HMDBLoader - Processing complete.')
        else:
            self.logger.error(f'Error: Retrieving HMDB archive failed.')

        # remove the intermediate data
        os.remove(os.path.join(self.data_path, 'hmdb_metabolites.zip'))

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, data_file_path: str, data_file_name: str) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :param data_file_path: the path to the HMDB zip file
        :param data_file_name: the name of the HMDB zip file
        :return: ret_val: record counts
        """
        # get the path to the data file
        infile_path: str = os.path.join(data_file_path, data_file_name)

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        with ZipFile(infile_path) as zf:
            # open the taxon file indexes and the uniref data file
            with zf.open('hmdb_metabolites.xml', 'r') as fp:
                # get all the xml elements
                xml_records: list = self.read_xml_file(fp, 'metabolite')

                # loop through the elements
                for record in xml_records:
                    # increment the counter
                    record_counter += 1

                    # convert the xml text into an object
                    el: E_Tree.Element = E_Tree.fromstring(record)

                    # get the metabolite element
                    metabolite_accession: E_Tree.Element = el.find('accession')

                    # did we get a good value
                    if metabolite_accession is not None and metabolite_accession.text is not None:
                        # create a valid curie for the metabolite id
                        metabolite_id = 'HMDB:' + metabolite_accession.text

                        # get the metabolite name element
                        metabolite_name: E_Tree.Element = el.find('name')

                        # did we get a good value
                        if metabolite_name is not None and metabolite_name.text is not None:
                            # get the nodes and edges for the pathways
                            pathway_success: bool = self.get_pathways(el, metabolite_id)

                            # get nodes and edges for the diseases
                            disease_success: bool = self.get_diseases(el, metabolite_id)

                            # get the nodes and edges for genes
                            gene_success: bool = self.get_genes(el, metabolite_id)

                            # did we get something created
                            if pathway_success or disease_success or gene_success:
                                # create a metabolite node and add it to the list
                                self.final_node_list.append({'id': metabolite_id, 'name': metabolite_name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")})
                            else:
                                # increment the counter
                                skipped_record_counter += 1

                                self.logger.debug(f'Metabolite {metabolite_id} record skipped due to no pathway, disease or gene data.')
                        else:
                            # increment the counter
                            skipped_record_counter += 1

                            self.logger.debug(f'Metabolite {metabolite_id} record skipped due to invalid metabolite name.')
                    else:
                        # increment the counter
                        skipped_record_counter += 1

                        self.logger.debug(f'Record skipped due to invalid metabolite id: {record}')

        self.logger.debug(f'Parsing XML data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

    def get_genes(self, el, metabolite_id) -> bool:
        """
        This method creates the gene nodes and gene to metabolite edges.

        note that there are 2 potential edge directions (legacy records shown):
             It is unknown (to me) why these would have different provided_by's as the subject/object types are the same.
          - metabolite to enzyme
            "provided_by": "hmdb.metabolite_to_enzyme",
            "subject": "CHEBI:16040", (chemical compound, ie. metabolite)
            "object": "NCBIGene:29974", (protein, ie. gene)
            "relation": "RO:0002434",
            "publications": []

          - enzyme to metabolite
            "provided_by": "hmdb.enzyme_to_metabolite",
            "subject": "CHEBI:84764", (chemical compound, ie. metabolite)
            "object": "NCBIGene:53947", (protein, ie. gene)
            "relation": "RO:0002434",
            "publications": []

        :param el: the root of this xml fragment
        :param metabolite_id: the metabolite id
        :return: found flag
        """
        # init the return
        ret_val: bool = False

        # get all the proteins
        proteins: list = el.find('protein_associations').findall('protein')

        # did we get any records
        if len(proteins) > 0:
            # for all the proteins listed
            for p in proteins:
                # get the protein id (gene)
                protein: E_Tree.Element = p.find('uniprot_id')

                # did we get a value
                if protein is not None and protein.text is not None:
                    # get the type of protein (gene type)
                    protein_type: E_Tree.Element = p.find('protein_type')

                    # was the protein type found
                    if protein_type is not None and protein_type.text is not None:
                        # we got at least something
                        ret_val = True

                        # create the gene id
                        gene_id = 'UniProtKB:' + protein.text

                        # what type of protein is this
                        if protein_type.text.startswith('Enzyme'):
                            # create the edge data
                            props: dict = {'provided_by': 'hmdb.enzyme_to_metabolite'}
                            subject_id: str = gene_id
                            object_id: str = metabolite_id
                            relation: str = 'CTD:affects_abundance_of'
                        # else it must be a transport?
                        else:
                            # create the edge data
                            props: dict = {'provided_by': 'hmdb.metabolite_to_enzyme'}
                            subject_id: str = metabolite_id
                            object_id: str = gene_id
                            relation: str = 'CTD:increases_transport_of'

                        # get the name element
                        el_name: E_Tree.Element = p.find('name')

                        # was the name found (optional)
                        if el_name is not None and el_name.text is not None:
                            name: str = el_name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                        else:
                            name: str = ''

                        # create a node and add it to the list
                        self.final_node_list.append({'id': object_id, 'name': name})

                        # create an edge and add it to the list
                        self.final_edge_list.append({'subject': subject_id, 'relation': relation, 'object': object_id, 'properties': props})
                    else:
                        self.logger.debug(f'no protein type for {metabolite_id}')
                else:
                    self.logger.debug(f'no proteins for {metabolite_id}')
        else:
            self.logger.debug(f'No proteins for {metabolite_id}')

        # return pass or fail
        return ret_val

    def get_diseases(self, el, metabolite_id) -> bool:
        """
        This method creates disease nodes and disease to metabolite edges.

        note: that there are 2 potential edge directions (modified legacy records shown below)
              It is unknown (to me) why these would have different provided_by's as the subject/object types are the same.

         - hmdb.metabolite_to_disease
              "provided_by": "hmdb.metabolite_to_disease",
              "subject": "CHEBI:16742", (chemical compound, ie. the metabolite)
              "object": "UMLS:C4324375", (disease, ie. the OMIM value)
              "relation": "SEMMEDDB:ASSOCIATED_WITH",
              "publications": []

         - disease_to_hmdb.metabolite
              "provided_by": "hmdb.disease_to_metabolite",
              "subject": "CHEBI:16742", (chemical compound, ie. the metabolite)
              "object": "MONDO:0005335", (disease, ie. the OMIM value)
              "relation": "SEMMEDDB:ASSOCIATED_WITH",
              "publications": []

        :param el: the root of this xml fragment
        :param metabolite_id: the metabolite id (edge subject)
        :return: found flag
        """
        # init the return
        ret_val: bool = False

        # get all the diseases
        diseases: list = el.find('diseases').findall('disease')

        # did we get any diseases
        if len(diseases) > 0:
            # for each disease
            for d in diseases:
                # get the omim id
                object_id: E_Tree.Element = d.find('omim_id')

                # did we get a value
                if object_id is not None and object_id.text is not None:
                    # we got at least something
                    ret_val = True

                    # get the name
                    name: E_Tree.Element = d.find('name')

                    # was the name found (optional)
                    if name is not None and name.text is not None:
                        name: str = name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                    else:
                        name: str = ''

                    # get all the pubmed ids
                    references: list = d.find('references').findall('reference')

                    # did we get some good data
                    if references is not None and len(references) > 0:
                        # storage for the pubmed ids
                        pmids: list = []

                        # for each reference get the pubmed id
                        for r in references:
                            # get the pubmed id
                            pmid: E_Tree.Element = r.find('pubmed_id')

                            # was it found
                            if pmid is not None and pmid.text is not None:
                                # save it in the list
                                pmids.append('PMID:' + pmid.text)

                        # create the edge property data
                        props: dict = {'provided_by': 'hmdb.metabolite_to_disease'}

                        # if we found any pubmed ids add them to the properties (optional)
                        if len(pmids) > 0:
                            props.update({'publications': pmids})

                        # create a node and add it to the list
                        self.final_node_list.append({'id': 'OMIM:' + object_id.text, 'name': name})

                        # create an edge and add it to the list
                        self.final_edge_list.append({'subject': metabolite_id, 'relation': 'RO:0002610', 'object': 'OMIM:' + object_id.text, 'properties': props})
                else:
                    self.logger.debug(f'no omim id for {metabolite_id}')
        else:
            self.logger.debug(f'No diseases for {metabolite_id}')

        # return pass or fail
        return ret_val

    def get_pathways(self, el, metabolite_id) -> bool:
        """
        This method creates pathway nodes and pathway to metabolite edges.

        note that there is one edge direction (modified legacy record shown below):
              "provided_by": "hmdb.metabolite_to_pathway",
              "subject": "CHEBI:80603", (chemical compound, ie. the metabolite)
              "object": "SMPDB:SMP0000627", (SMP pathway)
              "relation": "RO:0000056",
              "publications": []

        :param el: the root of this xml fragment
        :param metabolite_id: the metabolite id (edge subject)
        :return: found flag
        """
        # init the return
        ret_val: bool = False

        # get the pathways
        pathways: list = el.find('biological_properties').find('pathways').findall('pathway')

        # did we get any pathways
        if len(pathways) > 0:
            # for each pathway
            for p in pathways:
                # get the pathway id
                smpdb_id: E_Tree.Element = p.find('smpdb_id')

                # did we get a good value
                if id is not None and smpdb_id.text is not None:
                    # get the pathway curie ID
                    object_id: str = self.smpdb_to_curie(smpdb_id.text)

                    # did we get an id. a valid curie here is 16 characters long (SMPDB:SMP1234567)
                    if len(object_id) == 16:
                        # indicate that we created at least something
                        ret_val = True

                        # get the name
                        name_el: E_Tree.Element = p.find('name')

                        # did we get a good value (optional)
                        if name_el is not None and name_el.text is not None:
                            name: str = name_el.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                        else:
                            name: str = ''

                        # create a node and add it to the list
                        self.final_node_list.append({'id': object_id, 'name': name})

                        # create an edge and add it to the list
                        self.final_edge_list.append({'subject': metabolite_id, 'relation': 'RO:0000056', 'object': object_id, 'properties': {'provided_by': 'HMDB.metabolite_to_pathway'}})
                    else:
                        self.logger.debug(f'invalid smpdb for {metabolite_id}')
                else:
                    self.logger.debug(f'no smpdb for {metabolite_id}')
        else:
            self.logger.debug(f'No pathways for {metabolite_id}')

        # return pass or fail
        return ret_val

    def read_xml_file(self, fp, element) -> list:
        """
        Read the xml file and capture the metabolite elements.

        TODO filter out more items that arent used

        """
        # create the target xml fragment search tag
        start_tag: str = f'<{element}>'
        end_tag: str = f'</{element}>'

        # flag to indicate we have identified a new xml fragment
        tag_found: bool = False

        # init the list of xml fragments
        xml_fragments: list = []

        # init the line of xml text captured
        xml_line: str = ''

        # init a record counter
        counter: int = 0

        # for every line in the file
        for line in fp:
            # convert to string and remove the unprintable characters
            line = line.decode('utf-8')

            # are we starting a new one
            if start_tag in line:
                tag_found = True
                counter += 1
                if counter % 25000 == 0:
                    self.logger.debug(f'Loaded {counter} metabolites...')

            # collect the line
            if tag_found:
                xml_line += line

            # did we read the end of the element
            if end_tag in line:
                # save the element in the list
                xml_fragments.append(xml_line)

                # reset the start flag
                tag_found = False

                # rest the captured line
                xml_line = ''

            # TODO debugging only
            # if len(xml_fragments) > 10:
            #     break

        self.logger.debug(f'Loaded a total of {counter} metabolites.')

        # return the list of capture xml elements
        return xml_fragments

    @staticmethod
    def smpdb_to_curie(smp_id: str) -> str:
        """
        returns a valid smpdb curie from what is passed in ('SMP00123')

        :param smp_id: the smp id
        :return: the corrected curie
        """
        # init the return
        ret_val: str = ''

        # get the integer part
        smp_numeric: str = smp_id.lstrip('SMP')

        # was there a integer
        if smp_numeric.isdigit():
            ret_val = 'SMPDB:SMP' + '0'*(7-len(smp_numeric)) + smp_numeric

        # return to the caller
        return ret_val


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load HMDB data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the HMDB data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    HMDB_data_dir: str = args['data_dir']

    # get a reference to the processor
    hmdb = HMDBLoader()

    # load the data files and create KGX output
    hmdb.load(HMDB_data_dir, HMDB_data_dir)
