import os
import argparse
import pyoxigraph
from gzip import GzipFile

from zipfile import ZipFile
from collections import defaultdict
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode



##############
# Class: Chebi-Properties loader
#
# By: Olawumi
# Date: 10/19/2022
# Desc: Class that loads/parses the Chebi-Properties data.
##############
class ChebiSuplementLoader(SourceDataLoader):

    # Setting the class level variables for the source ID and provenance
    source_id: str = 'ChebiProperties'
    provenance_id: str = 'infores:chebi-properties'
    

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url: str = 'https://ftp.ebi.ac.uk/pub/databases/chebi/Flat_file_tab_delimited/'
        self.data_file: str = 'compounds.tsv.gz'
        self.relation: str = 'relation.tsv'
        self.data_files = [self.data_file,
                            self.relation]
    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """
        #Get the version for the compounds dataset
        file_url = f'{self.data_url}{self.data_file}'
        gd = GetData(self.logger.level)
        latest_source_version = gd.get_http_file_modified_date(file_url)
        return latest_source_version

    def get_data(self) -> bool:
        """
        Gets the chebi-properties data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)
        for dt_file in self.data_files:
            gd.pull_via_http(f'{self.data_url}{dt_file}',
                                           self.data_path)
        return True

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """
        def fixname(n):
            return f'CHEBI_ROLE:{"_".join(n.split())}'
        
        def update_ancestors(ancestors,parent,iia):
            kids = iia[parent]
            for kid in kids:
                ancestors[kid].append(parent)
                ancestors[kid] += ancestors[parent]
                update_ancestors(ancestors,kid,iia)

        def get_ancestors(iia):
            ancestors = defaultdict(list)
            role = 'CHEBI:50906'
            update_ancestors(ancestors,role,iia)
            print('CHEBI:50904 Ancestors', ancestors['CHEBI:50904'])
            return ancestors

        def read_roles():
            """This format is not completely obvious, but the triple is (FINAL_ID)-[type]->(INIT_ID)."""
            roles = defaultdict(list)
            invert_is_a = defaultdict(list)
            relation_file_path = os.path.join(self.data_path, f'{self.relation}')
            with open(relation_file_path,'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    if x[1] == 'has_role':
                        roles[f'CHEBI:{x[3]}'].append(f'CHEBI:{x[2]}')
                    elif x[1] == 'is_a':
                        child = f'CHEBI:{x[3]}'
                        parent = f'CHEBI:{x[2]}'
                        invert_is_a[parent].append(child)
            #Now include parents
            ancestors = get_ancestors(invert_is_a)
            for node,noderoles in roles.items():
                if node == 'CHEBI:64663':
                    print('hi checking CHEBI:64663 ')
                restroles= []
                for role in noderoles:
                    moreroles=ancestors[role]
                    restroles += moreroles
                roles[node] += restroles
            return roles

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0
        names = {}
        chebi_roles = read_roles()

        archive_file_path = os.path.join(self.data_path, f'{self.data_file}')
        with GzipFile(archive_file_path) as zf:
            for bytesline in zf:

                # increment the record counter
                record_counter += 1

                if self.test_mode and record_counter == 2000:
                    break

                lines = bytesline.decode('utf-8')
                line = lines.strip().split('\t')
                cid = line[2]
                cname = line[5]
                names[cid] = cname

        Chebi_nodes = []
        for node_id, node in names.items():
            if node == 'NAME':
                continue
            outnode = {'id': node_id, 'name': names[node_id],
                    'rest_roles': [eid for eid in chebi_roles[node_id]]}
            rolenames = [fixname(names[x]) for x in chebi_roles[node_id]]
            for rn in rolenames:
                if rn in ['CHEBI_ROLE:role', 'CHEBI_ROLE:biological_role', 'CHEBI_ROLE:chemical_role', 'CHEBI_ROLE:application']:
                    continue
                outnode[rn] = True
            Chebi_nodes.append(outnode)
        
     
        for chebi_node in Chebi_nodes:
            # make sure the node list is not null
            if chebi_node['name']!= 'null':
                chebi_props = {k: chebi_node[k] for k in set(list(chebi_node.keys())) - set(['name', 'id'])}
                node = kgxnode(chebi_node['id'], chebi_node['name'], nodeprops=chebi_props)
                self.final_node_list.append(node)
            else:
                skipped_record_counter += 1
        
        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        # return the split file names so they can be removed if desired
        return load_metadata


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load Chebi-Properties data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the Chebi-Properties data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    ch_data_dir: str = args['data_dir']

    # get a reference to the processor
    ch_ldr = ChebiSuplementLoader()

    # load the data files and create KGX output
    ch_ldr.load(ch_data_dir + '/nodes.jsonl')
