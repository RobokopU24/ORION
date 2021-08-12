import os
import argparse
import pandas as pd
import logging
import datetime

from xml.etree import ElementTree as ETree
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: UniRef similarities loader
#
# By: Phil Owen
# Date: 5/13/2020
# Desc: Class that loads the UniRef similarities data and creates KGX files for importing into a Neo4j graph.
##############
class UniRefSimLoader(SourceDataLoader):
    # UniProtKB viral organism column type for nodes.dmp
    TYPE_VIRUS: str = '9'

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
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.data_file = 'Uniref50/90/100.xml'
        self.test_mode = test_mode
        self.source_id = 'UniRef'
        self.source_db = 'UniProt UniRef gene similarity data'
        self.provenance_id = 'infores:uniref'
        self.nodes_output_file_path = ''
        self.edges_output_file_path = ''

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.ViralProteome.UniRefSimLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def write_to_file_x(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
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
                file_writer.write_node(node['id'], node_name=node['name'], node_types=node['category'], node_properties=node['properties'])

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'],
                                       object_id=edge['object'],
                                       relation=edge['relation'],
                                       original_knowledge_source=self.provenance_id,
                                       edge_properties=edge['properties'])

            self.final_node_list.clear()
            self.final_edge_list.clear()

    def get_uniref_data(self) -> set:
        """
        Gets the UniRef data.

        """

        # get a reference to the get data util class
        gd: GetData = GetData(self.logger.level)

        # are we in test mode
        if not self.test_mode:
            # get the list of taxa
            target_taxon_set: set = gd.get_ncbi_taxon_id_set(self.data_path, self.TYPE_VIRUS)
        else:
            # create a test set of target taxa
            target_taxon_set: set = {'654924', '2219562', '10493', '160691', '2219561', ''}

        # return the file count to the caller
        return target_taxon_set

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        parses the UniRef data files gathered from ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/ to
        create standard KGX files to import the data into a graph database

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return the parsed metadata stats
        """

        self.logger.info(f'UniRefSimLoader - Start of UniRef data processing.')

        self.nodes_output_file_path = nodes_output_file_path
        self.edges_output_file_path = edges_output_file_path

        # declare the name of the taxon index file
        taxon_index_file = 'taxon_file_indexes.txt'

        # declare the list of uniref input file names
        in_file_names: list = ['UniRef50']  # , 'UniRef90', 'UniRef100'

        # get the list of taxons to process
        target_taxon_set = self.get_uniref_data()

        final_record_count: int = 0
        final_skipped_count: int = 0

        # for each UniRef file to process
        for f in in_file_names:
            self.logger.debug(f'Processing {f}.')

            # add the file extension to the raw data
            if self.test_mode:
                full_file = f + '.test.xml'
            else:
                full_file = f + '.xml'

            # read the file and make the list
            records, skipped = self.parse_data_file(os.path.join(self.data_path, full_file), os.path.join(self.data_path, f'{f}_{taxon_index_file}'), target_taxon_set)

            # add to the final counts
            final_record_count += records
            final_skipped_count += skipped

            self.logger.info(f'UniRefSimLoader - {f} Processing complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    def parse_data_file(self, uniref_infile_path: str, index_file_path: str, target_taxa: set) -> (int, int):
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        The parsing uses and entry index file to read the uniref entry data elements on the fly rather.

        :param uniref_infile_path: the name of the uniref file to process
        :param index_file_path: the name of the uniref entry index file
        :param target_taxa: the set of target virus taxon ids
        :return: ret_val: record counts
        """
        # init the array that will contain nodes to write to KGX files
        node_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        self.logger.debug(f'Parsing XML data file start.')

        # open the taxon file indexes and the uniref data file
        with open(index_file_path, 'r') as index_fp, open(uniref_infile_path, 'rb+') as uniref_fp:
            # for each taxon index
            for line in index_fp:
                # increment the node counter
                record_counter += 1

                # output a status indicator
                if record_counter % 500000 == 0:
                    self.logger.debug(f'Completed {record_counter} taxa.')
                    # write out what we have
                    self.get_edge_list(node_list)
                    self.get_node_list(node_list)

                    self.write_to_file_x(self.nodes_output_file_path, self.edges_output_file_path)
                    node_list.clear()

                # start looking a bit before the location grep found
                taxon_index = int(line.split(':')[0]) - 150

                # get the next entry element
                entry_element: str = self.get_entry_element(taxon_index, uniref_fp)

                # did we get something back
                if entry_element != '':
                    # call to get an entry and enter it into the node list
                    good_record: bool = self.capture_entry_data(entry_element, node_list, target_taxa)

                    # was the record parsed properly
                    if not good_record:
                        # increment the node counter
                        skipped_record_counter += 1
                else:
                    # increment the node counter
                    skipped_record_counter += 1

                    self.logger.error(f'Error: Entry node for {line} at line number {record_counter} invalid.')

                # TODO: remove after testing
                if record_counter > 1000:
                    break

        # save any remainders
        if len(node_list) > 0:
            # sort the node list
            # node_list = sorted(node_list, key=lambda x: (x['grp'], x['node_num']))

            # write out what we have
            self.get_edge_list(node_list)
            self.get_node_list(node_list)
            self.write_to_file_x(self.nodes_output_file_path, self.edges_output_file_path)

        self.logger.debug(f'Parsing XML data file complete. {record_counter} taxa processed.')

        return record_counter, skipped_record_counter

    @staticmethod
    def get_entry_element(taxon_index: int, uniref_fp) -> str:
        """
        Looks for and returns the entry node at the byte location passed

        :param taxon_index: the index into the uniref file to start looking
        :param uniref_fp:  the file pointer to the uniref file
        :return: the text of the uniref entry node
        """

        # init the return
        entry_node_text: str = ''

        # backup up to 500 characters max to look for the "<entry>" start
        for i in range(0, 500):
            # goto the last checked position
            uniref_fp.seek(taxon_index)

            # read 6 characters to see if it is the start of the entry
            uniref_line = uniref_fp.read(6)

            # did we find the entry start
            if uniref_line.decode("utf-8") == "<entry":
                # go back to the start of the 'entry'
                uniref_fp.seek(taxon_index)

                # start writing out data until we see the end of the entry
                while True:
                    # get the line form the uniref file
                    uniref_line = uniref_fp.readline().decode("utf-8")

                    # no need to save the DNA sequence data
                    if uniref_line.startswith('  <seq'):
                        continue

                    # write out the line
                    entry_node_text += uniref_line

                    # did we find the end of the entry
                    if uniref_line.startswith('</entr'):
                        break
                break
            else:
                # move up a character and recheck
                taxon_index -= 1

        # return the entry node text to the caller
        return entry_node_text

    def capture_entry_data(self, entry_element: str, node_list: list, in_taxon_set: set) -> bool:
        """
        Loads the xml string and parses it to create graph nodes.

        :param entry_element: the text of the XML entry node
        :param node_list:
        :param in_taxon_set: the list of taxa that we are interested in capturing
        :return:
        """

        # init the return
        good_record: bool = False

        # load up the entry element
        root = ETree.fromstring(entry_element)

        # set the entry name
        entry_name = root.attrib['id'].replace('_', ':')

        # set the group id and similarity bin name
        grp: str = entry_name.split(':')[1]
        similarity_bin: str = entry_name.split(':')[0]

        # create local storage for the nodes will conditionally add to main node list later
        tmp_node_list: list = []

        # init the node counter
        node_counter: int = 0

        # init the flag to indicate we did something
        virus_capture: bool = False

        # declare some default categories for genes and taxons
        default_taxon_category: str = 'biolink:OrganismTaxon|biolink:OntologyClass|biolink:NamedThing'
        default_gene_category: str = ''  # 'biolink:Gene|biolink:GeneOrGeneProduct|biolink:MacromolecularMachine|biolink:GenomicEntity|biolink:MolecularEntity|biolink:BiologicalEntity|biolink:NamedThing'

        # loop through the child elements of the entry
        for entry_child in root:
            """
            Entry XML elements: UniRef node "UniRef###_accession" (gene_family) and "common taxon ID" (creates 1 node pair)
            Ex. (node number type 0, 1): UniRef100_Q6GZX4, NCBITaxon:10493

            Representative member XML elements: "UniProtKB accession" (gene) and "NCBI taxonomy" (creates 1 node pair)
            Ex. (node number type 2):  UniProt:Q6GZX4, NCBITaxon:654924

            Member XML elements: "UniProtKB accession" (gene) and "NCBI taxonomy" (creates N node pairs)
            Ex. (node number type 3+):  UniProt:A0A0F6NZX8, NCBITaxon:10493...
            """

            try:
                if entry_child.attrib['type'] == 'common taxon ID':
                    # we found a virus to capture
                    virus_capture = True

                    # save nodes for UniRef ID (UniRef###_accession) and UniRef taxon nodes (common taxon ID) for the entry
                    tmp_node_list.append({'grp': grp, 'node_num': node_counter, 'id': entry_name, 'name': entry_name, 'category': default_gene_category, 'similarity_bin': similarity_bin})

                    # get the taxon id
                    taxon_id = 'NCBITaxon:' + entry_child.attrib['value']

                    # save the taxon node
                    tmp_node_list.append({'grp': grp, 'node_num': node_counter + 1, 'id': taxon_id, 'name': 'NCBITaxon:' + entry_child.attrib['value'],
                                          'category': default_taxon_category, 'similarity_bin': similarity_bin})

                    # increment the node counter
                    node_counter += 2
            except KeyError:
                pass

            # get the similar members that are related to the entry. there could be a large number of these
            if virus_capture and (entry_child.tag == 'member' or entry_child.tag == 'representativeMember'):
                # loop through the members
                for member in iter(entry_child):
                    # look for the DB reference node.
                    if member.tag == 'dbReference':
                        # logger.debug(f"\t\tCluster dbReference\" element member: \"{member.attrib['type']}\" is {member.attrib['id']}.")
                        member_uniprotkb_id: str = member.attrib['id']

                        # init node data with the node grouping mechanism
                        member_props: dict = {'grp': grp}

                        # init the uniprot accession first found flag
                        found_uniprot_access: bool = False

                        # loop through the member properties
                        for db_ref_prop in member:
                            # get the needed DB reference properties for the similar member
                            if db_ref_prop.tag == 'property' and db_ref_prop.attrib['type'] in {'UniProtKB accession', 'source organism', 'NCBI taxonomy', 'protein name'}:
                                if db_ref_prop.attrib['type'] == 'UniProtKB accession':
                                    if not found_uniprot_access:
                                        found_uniprot_access = True
                                        # logger.debug(f"\t\t\tdbReference property: \"{db_ref_prop.attrib['type']}\" is {db_ref_prop.attrib['value']}")
                                        member_props.update({'id': member_uniprotkb_id, db_ref_prop.attrib['type']: db_ref_prop.attrib['value']})
                                else:
                                    # logger.debug(f"\t\t\tdbReference property: \"{db_ref_prop.attrib['type']}\" is {db_ref_prop.attrib['value']}")
                                    member_props.update({'id': member_uniprotkb_id, db_ref_prop.attrib['type']: db_ref_prop.attrib['value']})

                        try:
                            # is this a virus taxon
                            if member_props['NCBI taxonomy'] in in_taxon_set:
                                # insure all member elements are there before we add the nodes
                                ncbi_taxon: str = 'NCBITaxon:' + member_props['NCBI taxonomy']
                                uniprot: str = 'UniProtKB:' + member_props['UniProtKB accession']
                                source_organ: str = member_props["source organism"]
                                protein_name: str = member_props["protein name"]

                                # add the member Uniprot KB accession node
                                tmp_node_list.append({'grp': grp, 'node_num': node_counter, 'id': uniprot, 'name': protein_name,
                                                      'category': default_gene_category, 'equivalent_identifiers': uniprot, 'similarity_bin': similarity_bin})

                                # add the member NCBI taxon node
                                tmp_node_list.append({'grp': grp, 'node_num': node_counter, 'id': ncbi_taxon, 'name': source_organ,
                                                      'category': default_taxon_category, 'similarity_bin': similarity_bin})

                                # make ready for the next member node pair
                                node_counter += 1

                        except KeyError:
                            pass

            # did we get at least 3 node pairs (entry node pair, rep member node pair, at least 1 cluster member pair)
            if len(tmp_node_list) >= 6:
                good_record = True
                node_list.extend(tmp_node_list)
            # else:
            #     logger.debug(f'\nEntry {entry_element} disqualified.\n')

            # if not virus_capture:
            #     logger.debug(f'{grp} not captured.')

        # return to the caller
        return good_record

    def get_node_list(self, node_list):
        """
        gets the data collected from the UniRef file node list

        :return:
        """

        self.logger.debug(f'Loading data frame with {len(node_list)} nodes.')

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'similarity_bin'])

        # reshape the data frame and remove all node duplicates.
        new_df = df.drop(['grp', 'node_num'], axis=1)
        new_df = new_df.drop_duplicates(keep='first')

        self.logger.debug(f'{len(new_df.index)} nodes found.')

        # write out the unique nodes
        for item in new_df.iterrows():
            # get the properties for the node. ncbitaxons geet a taxon property
            if item[1]['id'].startswith('NCBITaxon'):
                props = {'similarity_bin': item[1]['similarity_bin'], 'taxon': item[1]['id']}
            else:
                props = {'similarity_bin': item[1]['similarity_bin']}

            # save the node
            self.final_node_list.append({'id': item[1]['id'], 'name': item[1]['name'], 'category': [], 'properties': props})

    def get_edge_list(self, node_list):
        """
        writes edges for the node list passed

        :return: nothing
        """

        self.logger.debug(f'Creating edges for {len(node_list)} nodes.')

        # init group detection
        cur_group_name: str = ''
        first: bool = True
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # iterate through node groups and create the edge records.
        while node_idx < node_count:
            # logger.debug(f'Working index: {node_idx}')

            # if its the first time in prime the pump
            if first:
                cur_group_name = node_list[node_idx]['grp']
                first = False

            # init variables for each group
            similarity_bin: str = ''
            gene_family_node_id: str = ''
            rep_member_node_id: str = ''

            # for each entry member in the group
            while node_list[node_idx]['grp'] == cur_group_name:
                """
                Entry nodes UniRef ID (UniRef###_accession) and UniRef taxon nodes (common taxon ID)
                Ex. (node number 0 and 1): (gene_family UniRef100_Q6GZX4)-[in_taxon]-(NCBITaxon:10493)

                For all member node pairs (representative or cluster) where node number N starts at 2...
                    Member ID (UniProtKB accession) and UniRef ID (UniRef###_accession)
                    Ex. (node number N and 0): (gene UniProt:A0A0F6NZX8)-[part of]-(UniRef100_Q6GZX4)
                    Member ID (UniProtKB accession) and taxon ID (NCBI taxonomy)
                    Ex. (node number N+1 and 1): (gene UniProt:A0A0F6NZX8)-[in_taxon]-(NCBITaxon:10493)

                (Optional) Combination Member ID (UniProtKB accession) to Member ID (UniProtKB accession)
                Ex. (node number X and Y): (gene UniProt:Q6GZX4)-[SO:similar_to]-(gene UniProt:A0A0F6NZX8)            
                """

                props = {'similarity_bin': similarity_bin}

                # get the UniRef entry ID and similarity bin
                if node_list[node_idx]['node_num'] == 0:
                    gene_family_node_id = node_list[node_idx]['id']
                    similarity_bin = node_list[node_idx]['similarity_bin']
                    # set the default properties
                # get the UniRef entry common taxon ID and create the UniRef ID to taxon edge
                elif node_list[node_idx]['node_num'] == 1 and gene_family_node_id != '':
                    self.final_edge_list.append({"predicate": "", "subject": f"{gene_family_node_id}", "relation": "RO:0002162", "object": f"{node_list[node_idx]['id']}", 'properties': props})
                # get the member node edges
                elif similarity_bin != '' and gene_family_node_id != '':
                    self.final_edge_list.append({"subject": node_list[node_idx]['id'], "relation": "BFO:0000050", "object": gene_family_node_id, 'properties': props})
                    self.final_edge_list.append({"subject": node_list[node_idx]['id'], "relation": "RO:0002162", "object": node_list[node_idx + 1]['id'], 'properties': props})

                    # this node is the representative UniProtKB ID node
                    if node_list[node_idx]['node_num'] == 2:
                        rep_member_node_id: str = node_list[node_idx]['id']

                    # add the spoke edge if it isn't a reflection of itself
                    if rep_member_node_id != node_list[node_idx]['id']:
                        self.final_edge_list.append({"subject": rep_member_node_id, "relation": "RO:HOM0000000", "object": node_list[node_idx]['id'], 'properties': props})

                    # increment the node counter pairing
                    node_idx += 1
                else:
                    self.logger.error('Missing data elements similarity_bin or gene_family_node_id')

                # increment the node counter
                node_idx += 1

                # insure we dont overrun the list
                if node_idx >= node_count:
                    break

            # insure we dont overrun the list
            if node_idx >= node_count:
                break

            # save the new group name
            cur_group_name = node_list[node_idx]['grp']

        self.logger.debug(f'{node_idx} Entry member edges created.')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniRef data files and create KGX import files.')

    # command line should be like: python loadUniRef2.py -d /projects/stars/Data_services/UniRef_data -f uniref50,uniref90,uniref100
    ap.add_argument('-r', '--data_dir', required=True, help='The location of the UniRef data files')
    ap.add_argument('-f', '--UniRef_files', required=True, help='Name(s) of input UniRef files (comma delimited)')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    # data_dir = 'E:/Data_services/UniRef_data'
    UniRef_data_dir: str = args['data_dir']

    # create the file list
    # file_list: list = ['uniref50']  # 'uniref100', 'uniref90', 'uniref50'
    file_list: list = args['UniRef_files'].split(',')

    # get a reference to the processor
    vp = UniRefSimLoader()

    # load the data files and create KGX output
    vp.load(UniRef_data_dir, UniRef_data_dir)
