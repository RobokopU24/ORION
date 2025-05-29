import os
import zipfile

import polars as pl
import yaml
from yaml import SafeLoader

from Common.kgxmodel import kgxnode, kgxedge
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import *
from Common.prefixes import PUBCHEM_COMPOUND
from Common.utils import GetData


class MetabolomicsWorkbenchLoader(SourceDataLoader):

    source_id: str = 'MetabolomicsWorkbench'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_url = 'https://cfde-drc.s3.amazonaws.com/Metabolomics/KG%20Assertions'
        self.data_file = "MW.zip"

        with open('/ORION/cfde-config.yml', 'r') as file:
            yaml_data = list(yaml.load_all(file, Loader=SafeLoader))
        self.config = list(filter(lambda x: x["name"] == self.source_id, yaml_data))[0]

    def get_latest_source_version(self) -> str:
        return self.config['version']

    def get_data(self) -> bool:
        data_puller = GetData()
        source_data_url = f'{self.data_url}/{self.get_latest_source_version()}/{self.data_file}'
        data_puller.pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        zip_file = os.path.join(self.data_path, self.data_file)

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(self.data_path)

        nodes = pl.DataFrame(schema={"id": pl.String, "original_id": pl.String, "name": pl.String, "category": pl.List(pl.String)})

        predicate_mapping = dict(self.config['predicate_mapping'])

        for file in self.config["node_files"]:
            if file["node_file"]["secondary_id_column"]:
                tmp_df = pl.scan_csv(os.path.join(self.data_path, file["node_file"]["name"]), has_header=True).select(
                    pl.when(pl.col(file["node_file"]["primary_id_column"]).is_null()).then(pl.col(file["node_file"]["secondary_id_column"])).otherwise(pl.col(file["node_file"]["primary_id_column"])).alias("id"),
                    pl.col("").alias("original_id"),
                    pl.col("label").alias("name"),
                    pl.when(pl.col("type").is_null()).then(pl.lit(file["node_file"]["type"])).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
                )
            else:
                tmp_df = pl.scan_csv(os.path.join(self.data_path, file["node_file"]["name"]), has_header=True).select(
                    pl.col(file["node_file"]["primary_id_column"]).alias("id"),
                    pl.col("").alias("original_id"),
                    pl.col("label").alias("name"),
                    pl.when(pl.col("type").is_null()).then(pl.lit(file["node_file"]["type"])).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
                )
            tmp_df = tmp_df.with_columns(
                pl.when(pl.col("id").str.starts_with("PUBCHEM")).then(pl.col("id").str.replace("PUBCHEM", PUBCHEM_COMPOUND)).otherwise(pl.col("id")).alias("id"),
                pl.col("original_id"),
                pl.col("name"),
                pl.col("category")
            ).collect()
            nodes = pl.concat([nodes, tmp_df], how="vertical")

        node_mapping = dict(zip(nodes["original_id"], nodes["id"]))

        df_missing = nodes.filter(pl.any_horizontal(pl.all().is_null()))
        unmapped_path = os.path.join(self.data_path, "unmapped.jsonl")
        df_missing.write_ndjson(unmapped_path)

        missing_mapping = dict(zip(df_missing["original_id"], df_missing["id"]))

        nodes = nodes.drop_nulls()
        nodes.drop_in_place("original_id")

        for row in nodes.rows(named=True):
            node = kgxnode(identifier=row['id'], name=row['name'], categories=row['category'])
            self.final_node_list.append(node)

        # nodes_path = os.path.join(self.data_path, "source_nodes.jsonl")
        # nodes.write_ndjson(nodes_path)

        edges = pl.scan_csv(os.path.join(self.data_path, self.config['edge_file']), has_header=True).select(
            pl.col("source").alias("subject"),
            pl.col("relation").alias("predicate"),
            pl.col("target").alias("object"),
            pl.lit(self.config['provenance_id']).alias(PRIMARY_KNOWLEDGE_SOURCE),
            pl.lit("data_analysis_pipeline").alias(AGENT_TYPE),
            pl.lit("knowledge_assertion").alias(KNOWLEDGE_LEVEL),
        ).collect()

        edges = edges.with_columns(pl.col("subject").replace(missing_mapping), pl.col("predicate"), pl.col("object").replace(missing_mapping)).drop_nulls()
        edges = edges.with_columns(pl.col("subject").replace(node_mapping), pl.col("predicate").replace(predicate_mapping), pl.col("object").replace(node_mapping))

        for row in edges.rows(named=True):
            edge = kgxedge(subject_id=row['subject'], predicate=row['predicate'], object_id=row['object'], primary_knowledge_source=row[PRIMARY_KNOWLEDGE_SOURCE], edgeprops={ KNOWLEDGE_LEVEL: row[KNOWLEDGE_LEVEL], AGENT_TYPE: row[AGENT_TYPE]})
            self.final_edge_list.append(edge)

        return { 'record_counter': len(edges), 'skipped_record_counter': len(df_missing), 'errors': []}
