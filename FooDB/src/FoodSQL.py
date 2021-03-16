import sqlite3
import os
import pandas as pd
from pandas import DataFrame
import tarfile


class FoodSQL:
    def __init__(self, data_path):
        self.data_path = data_path
        self.conn = sqlite3.connect(os.path.join(self.data_path, 'FooDB.db'))
        self.c = self.conn.cursor()

    def create_db(self, reload=True):
        if reload:
            read_compound = pd.read_csv(os.path.join(self.data_path, 'Compound.csv'), index_col='id', low_memory=False)
            read_compound.to_sql('compound', self.conn, if_exists='replace', index=True, index_label='id')  # Insert the values from the csv file into the table 'CLIENTS'

            read_content = pd.read_csv(os.path.join(self.data_path, 'Content.csv'), index_col=['food_id'], low_memory=False)
            read_content.to_sql('content', self.conn, if_exists='replace', index=True, index_label='food_id')  # Insert the values from the csv file into the table 'CLIENTS'

            read_food = pd.read_csv(os.path.join(self.data_path, 'Food.csv'), index_col='id', low_memory=False)
            read_food.to_sql('food', self.conn, if_exists='replace', index=True, index_label='id')  # Insert the values from the csv file into the table 'CLIENTS'

            read_nutrient = pd.read_csv(os.path.join(self.data_path, 'Nutrient.csv'), index_col='id', low_memory=False)
            read_nutrient.to_sql('nutrient', self.conn, if_exists='replace', index=True, index_label='id')  # Insert the values from the csv file into the table 'CLIENTS'

    def lookup_food(self):
        sql = 'select f.id as food_id, ifnull(f.name_scientific, ifnull(f.name, NULL)) as food_name, \
                    f.ncbi_taxonomy_id as ncbi_taxonomy_id, cn.id as content_id, cn.orig_unit as content_unit, \
                    cn.orig_max as content_max, cn.source_id as content_source_id, cn.source_type as content_source_type, \
                    cm.name as compound_name, cm.moldb_inchikey as inchikey, cm.moldb_smiles as smiles \
                from food f \
                join content cn on cn.food_id = f.id \
                join compound cm on cm.id=cn.source_id \
                where f.ncbi_taxonomy_id is not NULL \
                    and f.ncbi_taxonomy_id <> \'\' \
                    and food_name is not NULL \
                order by f.id'

        ret_val = self.c.execute(sql)

        return ret_val , {'food_id': 0, 'food_name': 1, 'ncbi_taxonomy_id': 2, 'content_id': 3, 'content_unit': 4, 'content_max': 5, 'content_source_id': 6, 'content_source_type': 7, 'compound_name': 8, 'inchikey': 9, 'smiles': 10}
