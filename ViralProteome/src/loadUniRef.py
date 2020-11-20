import os
import argparse
import hashlib
import pandas as pd
import requests
import json
import logging

from io import TextIOBase
from xml.etree import ElementTree as ETree
from Common.utils import LoggingUtil, GetData, EdgeNormUtils
from pathlib import Path


##############
# Class: UniRef similarities loader
#
# By: Phil Owen
# Date: 5/13/2020
# Desc: Class that loads the UniRef similarities data and creates KGX files for importing into a Neo4j graph.
##############
class UniRefSimLoader:
    # UniProtKB viral organism column type for nodes.dmp
    TYPE_VIRUS: str = '9'

    # storage for cached node and edge normalizations
    cached_node_norms: dict = {}
    cached_edge_norms: dict = {}

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.ViralProteome.UniRefSimLoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    def load(self, data_dir: str, in_file_names: list, taxon_index_file: str, output_mode: str = 'json', test_mode: bool = False):
        """
        parses the UniRef data files gathered from ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/ to
        create standard KGX files to import thr data into a graph database

        :param data_dir: the directory of the input data files
        :param in_file_names: the UniRef file to work
        :param taxon_index_file: the list of UniRef virus file indexes
        :param output_mode: the output mode (tsv or json)
        :param test_mode: debug mode flag to indicate use of smaller input files
        :return
        """

        self.logger.info(f'UniRefSimLoader - Start of UniRef data processing.')

        # get a reference to the get data util class
        gd = GetData(self.logger.level)

        # are we in test mode
        if not test_mode:
            # get the list of taxa
            target_taxon_set: set = gd.get_ncbi_taxon_id_set(data_dir, self.TYPE_VIRUS)
        else:
            # create a test set of target taxa
            target_taxon_set = {'654924', '2219562', '10493', '160691', '2219561', ''}

        # for each UniRef file to process
        for f in in_file_names:
            self.logger.debug(f'Processing {f}.')

            # process the file
            with open(os.path.join(data_dir, f'{f}_Virus_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_dir, f'{f}_Virus_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
                # depending on the output mode, write out the node and edge data headers
                if output_mode == 'json':
                    out_node_f.write('{"nodes":[\n')
                    out_edge_f.write('{"edges":[\n')
                else:
                    out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                    out_edge_f.write(f'id\tpredicate\tsubject\trelation\tedge_label\tobject\tsource_database\n')

                # add the file extension
                if test_mode:
                    full_file = f + '.test.xml'
                else:
                    full_file = f + '.xml'

                # read the file and make the list
                self.parse_data_file(os.path.join(data_dir, full_file), os.path.join(data_dir, f'{f}_{taxon_index_file}'), target_taxon_set, out_node_f, out_edge_f, output_mode)

                self.logger.info(f'UniRefSimLoader - {f} Processing complete.')

        # output the normalization failures
        gd.format_normalization_failures(self.get_name(), self.node_norm_failures, self.edge_norm_failures)

    def parse_data_file(self, uniref_infile_path: str, index_file_path: str, target_taxa: set, out_node_f, out_edge_f, output_mode):
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        The parsing uses and entry index file to read the uniref entry data elements on the fly rather.

        :param uniref_infile_path: the name of the uniref file to process
        :param index_file_path: the name of the uniref entry index file
        :param target_taxa: the set of target virus taxon ids
        :param out_node_f: the node file pointer
        :param out_edge_f: the edge file pointer
        :param output_mode: the output mode (tsv or json)
        :return: ret_val: the node list
        """
        # init the array that will contain nodes to write to KGX files
        node_list: list = []

        # uniref entry index counter
        index_counter: int = 0

        self.logger.debug(f'Parsing XML data file start.')

        # open the taxon file indexes and the uniref data file
        with open(index_file_path, 'r') as index_fp, open(uniref_infile_path, 'rb+') as uniref_fp:
            # for each taxon index
            for line in index_fp:
                # increment the node counter
                index_counter += 1

                # output a status indicator
                if index_counter % 500000 == 0:
                    self.logger.debug(f'Completed {index_counter} taxa.')

                # start looking a bit before the location grep found
                taxon_index = int(line.split(':')[0]) - 150

                # get the next entry element
                entry_element: str = self.get_entry_element(taxon_index, uniref_fp)

                # did we get something back
                if entry_element != '':
                    # call to get an entry and enter it into the node list
                    self.capture_entry_data(entry_element, node_list, target_taxa)
                else:
                    self.logger.error(f'Error: Entry node for {line} at line number {index_counter} invalid.')

        # save any remainders
        if len(node_list) > 0:
            # normalize the group of entries on the data frame.
            self.normalize_node_data(node_list)

            # write out what we have
            self.write_out_data(node_list, out_node_f, out_edge_f, output_mode)

        # finish off the json if we have to
        if output_mode == 'json':
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

        self.logger.debug(f'Parsing XML data file complete. {index_counter} taxa processed.')

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the taxon node.
        the data comes in as a node list and we will normalize the only the taxon nodes.

        :param node_list: A list with items to normalize
        :return:
        """

        # loop through the list and only save the NCBI taxa nodes
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify taxa that has not been node normed
        tmp_normalize: set = set()

        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # is this a NCBI taxon
            if node_list[node_idx]['id'].startswith('N'):
                # check to see if this one needs normalization data from the website
                if not node_list[node_idx]['id'] in self.cached_node_norms:
                    tmp_normalize.add(node_list[node_idx]['id'])

            # go to the next element
            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 2500

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique nodes will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block {start_index} to {end_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**self.cached_node_norms, **rvs}

                    # save the merged list
                    self.cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.warning(f'Warning: Response code: {resp.status_code} block {start_index} to {end_index}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        self.cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx = 0

        # for each row in the slice add the new id and name
        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # is this a NCBI taxon
            if node_list[node_idx]['id'].startswith('N'):
                rv = node_list[node_idx]

                # did we find a normalized value
                if self.cached_node_norms[rv['id']] is not None:
                    # find the name and replace it with label
                    if 'label' in self.cached_node_norms[rv['id']]['id']:
                        node_list[node_idx]['name'] = self.cached_node_norms[rv['id']]['id']['label']

                    if 'type' in self.cached_node_norms[rv['id']]:
                        node_list[node_idx]['category'] = '|'.join(self.cached_node_norms[rv['id']]['type'])

                    # get the equivalent identifiers
                    if 'equivalent_identifiers' in self.cached_node_norms[rv['id']] and len(self.cached_node_norms[rv['id']]['equivalent_identifiers']) > 0:
                        node_list[node_idx]['equivalent_identifiers'] = '|'.join(list((item['identifier']) for item in self.cached_node_norms[rv['id']]['equivalent_identifiers']))

                    # find the id and replace it with the normalized value
                    node_list[node_idx]['id'] = self.cached_node_norms[rv['id']]['id']['identifier']
                else:
                    # self.logger.debug(f"{rv['id']} has no normalized value")
                    self.node_norm_failures.append(rv['id'])

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list

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

    @staticmethod
    def capture_entry_data(entry_element: str, node_list: list, in_taxon_set: set):
        """
        Loads the xml string and parses it to create graph nodes.

        :param entry_element: the text of the XML entry node
        :param node_list: the main list that will contain all nodes
        :param in_taxon_set: the list of taxa that we are interested in capturing
        :return:
        """

        # load up the entry element
        root = ETree.fromstring(entry_element)

        # set the entry name, group id and similarity bin name
        entry_name = root.attrib['id'].replace('_', ':')
        grp: str = entry_name.split(':')[1]
        similarity_bin: str = entry_name.split(':')[0]

        # create local storage for the nodes will conditionally add to main node list later
        tmp_node_list: list = []

        # init the node counter
        node_counter: int = 0

        # init the flag to indicate we did something
        virus_capture: bool = False

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
                    tmp_node_list.append({'grp': grp, 'node_num': node_counter, 'id': entry_name, 'name': entry_name, 'category': 'gene_family|named_thing|biological_entity|molecular_entity', 'equivalent_identifiers': entry_name,
                                          'similarity_bin': similarity_bin})

                    tmp_node_list.append({'grp': grp, 'node_num': node_counter + 1, 'id': 'NCBITaxon:' + entry_child.attrib['value'], 'name': 'NCBITaxon:' + entry_child.attrib['value'], 'category': '',
                                          'equivalent_identifiers': 'NCBITaxon:' + entry_child.attrib['value'], 'similarity_bin': similarity_bin})

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
                                                      'category': 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing', 'equivalent_identifiers': uniprot, 'similarity_bin': similarity_bin})

                                # add the member NCBI taxon node
                                tmp_node_list.append({'grp': grp, 'node_num': node_counter, 'id': ncbi_taxon, 'name': source_organ, 'category': 'organism_taxon|named_thing|ontology_class', 'equivalent_identifiers': ncbi_taxon,
                                                      'similarity_bin': similarity_bin})

                                # make ready for the next member node pair
                                node_counter += 1

                        except KeyError:
                            pass

        # did we get at least 3 node pairs (entry node pair, rep member node pair, at least 1 cluster member pair)
        if len(tmp_node_list) >= 6:
            node_list.extend(tmp_node_list)
        # else:
        #     logger.debug(f'\nEntry {entry_element} disqualified.\n')

        # if not virus_capture:
        #     logger.debug(f'{grp} not captured.')

    def write_out_data(self, node_list, out_node_f: TextIOBase, out_edge_f: TextIOBase, output_mode: str):
        """
        writes out the data collected from the UniRef file node list to KGX node and edge files

        :param node_list: the list of nodes create edges and to write out to file
        :param out_node_f: the node file
        :param out_edge_f: the edge file
        :param output_mode: the output mode (tsv or json)
        :return:
        """

        self.logger.debug(f'Loading data frame with {len(node_list)} nodes.')

        # write out the edges
        self.write_edge_data(out_edge_f, node_list, output_mode)

        # create a data frame with the node list
        df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers', 'similarity_bin'])

        # reshape the data frame and remove all node duplicates.
        new_df = df.drop(['grp', 'node_num'], axis=1)
        new_df = new_df.drop_duplicates(keep='first')

        self.logger.debug(f'{len(new_df.index)} nodes found.')

        # init a set for the node de-duplication
        final_node_set: set = set()

        # write out the unique nodes
        for item in new_df.iterrows():
            if output_mode == 'json':
                # turn these into json
                category = json.dumps(item[1]['category'].split('|'))
                identifiers = json.dumps(item[1]['equivalent_identifiers'].split('|'))

                # save the node
                final_node_set.add(f'{{"id":"{item[1]["id"]}", "name":"{item[1]["name"]}", "category":{category}, "equivalent_identifiers":{identifiers}}}')
            else:
                final_node_set.add(f"{item[1]['id']}\t{item[1]['name']}\t{item[1]['category']}\t{item[1]['equivalent_identifiers']}")

        # write out the node data
        if output_mode == 'json':
            out_node_f.write(',\n'.join(final_node_set))
        else:
            out_node_f.write('\n'.join(final_node_set))

        self.logger.debug('Writing out to data file complete.')

    def write_edge_data(self, out_edge_f: TextIOBase, node_list: list, output_mode: str):
        """
        writes edges for the node list passed

        :param out_edge_f: the edge file
        :param node_list: node storage data frame
        :param output_mode: the output mode (tsv or json)
        :return: nothing
        """

        self.logger.debug(f'Creating edges for {len(node_list)} nodes.')

        # init group detection
        cur_group_name: str = ''
        first: bool = True
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list for edges to normalize
        edge_list: list = []

        # get the edge normalizer
        en = EdgeNormUtils(self.logger.level)

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

                # get the UniRef entry ID and similarity bin
                if node_list[node_idx]['node_num'] == 0:
                    gene_family_node_id = node_list[node_idx]['id']
                    similarity_bin = node_list[node_idx]['similarity_bin']
                # get the UniRef entry common taxon ID and create the UniRef ID to taxon edge
                elif node_list[node_idx]['node_num'] == 1 and gene_family_node_id != '':
                    edge_list.append({"predicate": "biolink:in_taxon", "subject": f"{gene_family_node_id}", "relation": "RO:0002162", "object": f"{node_list[node_idx]['id']}", "edge_label": "in_taxon", "source_database": f"{similarity_bin}"})
                # get the member node edges
                elif similarity_bin != '' and gene_family_node_id != '':
                    edge_list.append({"predicate": "biolink:part_of", "subject": f"{node_list[node_idx]['id']}", "relation": "BFO:0000050", "object": f"{gene_family_node_id}", "edge_label": "part_of", "source_database": f"{similarity_bin}"})
                    edge_list.append({"predicate": "biolink:in_taxon", "subject": f"{node_list[node_idx]['id']}", "relation": "RO:0002162", "object": f"{node_list[node_idx + 1]['id']}", "edge_label": "in_taxon", "source_database": f"{similarity_bin}"})

                    # this node is the representative UniProtKB ID node
                    if node_list[node_idx]['node_num'] == 2:
                        rep_member_node_id: str = node_list[node_idx]['id']

                    # add the spoke edge if it isn't a reflection of itself
                    if rep_member_node_id != node_list[node_idx]['id']:
                        edge_list.append({"predicate": "biolink:similar_to", "subject": f"{rep_member_node_id}", "relation": "RO:HOM0000000", "object": f"{node_list[node_idx]['id']}", "edge_label": "similar_to", "source_database": f"{similarity_bin}"})

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

        # normalize the edges
        en.normalize_edge_data(edge_list, self.cached_edge_norms)

        # create an edge set to remove duplicates
        edge_set: set = set()

        # write out all the edges
        for item in edge_list:
            # create the record ID
            record_id: str = item["subject"] + item["relation"] + item["edge_label"] + item["object"]

            # depending on the output mode save the edge data
            if output_mode == 'json':
                edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "predicate":"{item["predicate"]}", "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "source_database":"{item["source_database"]}"}}')
            else:
                edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["predicate"]}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["object"]}\t{item["source_database"]}')

        # write out the edge data
        if output_mode == 'json':
            out_edge_f.write(',\n'.join(edge_set))
        else:
            out_edge_f.write('\n'.join(edge_set))

        # empty out the edge list and set
        edge_list.clear()
        edge_set.clear()

        self.logger.debug(f'{node_idx} Entry member edges created.')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniRef data files and create KGX import files.')

    # command line should be like: python loadUniRef2.py -d /projects/stars/Data_services/UniRef_data -f uniref50,uniref90,uniref100
    ap.add_argument('-r', '--data_dir', required=True, help='The location of the UniRef data files')
    ap.add_argument('-f', '--UniRef_files', required=True, help='Name(s) of input UniRef files (comma delimited)')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    # data_dir = 'E:/Data_services/UniRef_data'
    UniRef_data_dir: str = args['data_dir']
    out_mode = args['out_mode']

    # create the file list
    # file_list: list = ['uniref50']  # 'uniref100', 'uniref90', 'uniref50'
    file_list: list = args['UniRef_files'].split(',')

    # get a reference to the processor
    vp = UniRefSimLoader()

    # load the data files and create KGX output
    vp.load(UniRef_data_dir, file_list, 'taxon_file_indexes.txt', output_mode=out_mode)
