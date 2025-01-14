import os
import enum
import zipfile
import pathlib
from collections.abc import Mapping

import polars as pl

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.biolink_constants import *
from Common.prefixes import PUBCHEM_COMPOUND
from Common.utils import GetData




class MetabolomicsWorkbenchLoader(SourceDataLoader):

    source_id: str = 'MetabolomicsWorkbench'
    provenance_id: str = 'infores:metabolomics_workbench'
    parsing_version: str = '2024-05-08'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_url = 'https://cfde-drc.s3.amazonaws.com/Metabolomics/KG%20Assertions'
        self.data_file = "MW.zip"

    def get_latest_source_version(self) -> str:
        latest_version = '2024-05-08'
        return latest_version

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

        anatomy_df = pl.scan_csv(os.path.join(self.data_path, "MW.Anatomy.nodes.csv"), has_header=True).select(
            pl.when(pl.col("UBERON").is_null()).then(pl.col("CHV")).otherwise(pl.col("UBERON")).alias("id"),
            pl.col("").alias("original_id"),
            pl.col("label").alias("name"),
            pl.when(pl.col("type").is_null()).then(pl.lit("Anatomy")).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
        ).collect()
        null_nodes_count = int(anatomy_df.null_count().item(0,0))

        disease_df = pl.scan_csv(os.path.join(self.data_path, "MW.Disease or Phenotype.nodes.csv"), has_header=True).select(
            pl.when(pl.col("MONDO").is_null()).then(pl.col("CHV")).otherwise(pl.col("MONDO")).alias("id"),
            pl.col("").alias("original_id"),
            pl.col("label").alias("name"),
            pl.when(pl.col("type").is_null()).then(pl.lit("Disease or Phenotype")).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
        ).collect()
        null_nodes_count = null_nodes_count + int(disease_df.null_count().item(0,0))

        gene_df = pl.scan_csv(os.path.join(self.data_path, "MW.Gene.nodes.csv"), has_header=True).select(
            pl.when(pl.col("HGNC").is_null()).then(pl.col("OMIM")).otherwise(pl.col("HGNC")).alias("id"),
            pl.col("").alias("original_id"),
            pl.col("label").alias("name"),
            pl.when(pl.col("type").is_null()).then(pl.lit("Gene")).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
        ).collect()
        null_nodes_count = null_nodes_count + int(gene_df.null_count().item(0,0))

        metabolite_df = pl.scan_csv(os.path.join(self.data_path, "MW.Metabolite.nodes.csv"), has_header=True).select(
            pl.when(pl.col("PUBCHEM").is_null()).then(pl.col("PUBMED")).otherwise(pl.col("PUBCHEM")).alias("id"),
            pl.col("").alias("original_id"),
            pl.col("label").alias("name"),
            pl.when(pl.col("type").is_null()).then(pl.lit("Metabolite")).otherwise(pl.col("type")).cast(pl.List(pl.String)).alias("category")
        ).collect()
        null_nodes_count = null_nodes_count + int(metabolite_df.null_count().item(0,0))

        nodes = pl.concat([nodes, anatomy_df, disease_df, gene_df, metabolite_df], how="vertical")
        node_mapping = dict(zip(nodes["original_id"], nodes["id"]))

        df_missing = nodes.filter(pl.any_horizontal(pl.all().is_null()))
        unmapped_path = os.path.join(self.data_path, "unmapped.jsonl")
        df_missing.write_ndjson(unmapped_path)

        missing_mapping = dict(zip(df_missing["original_id"], df_missing["id"]))

        nodes = nodes.drop_nulls()
        nodes.drop_in_place("original_id")

        nodes_path = os.path.join(self.data_path, "source_nodes.jsonl")
        nodes.write_ndjson(nodes_path)

        predicate_mapping = {"produces": "biolink:produces", "causally_influences": "biolink:produces", "correlated_with_condition": "biolink:correlated_with"}

        edges = pl.scan_csv(os.path.join(self.data_path, "MW.edges.csv"), has_header=True).select(
            pl.col("source").alias("subject"),
            pl.col("relation").alias("predicate"),
            pl.col("target").alias("object"),
            pl.lit("infores:mw").alias(PRIMARY_KNOWLEDGE_SOURCE),
            pl.lit("data_analysis_pipeline").alias(AGENT_TYPE),
            pl.lit("knowledge_assertion").alias(KNOWLEDGE_LEVEL),
        ).collect()

        edges = edges.with_columns(pl.col("subject").replace(missing_mapping), pl.col("predicate"), pl.col("object").replace(missing_mapping))
        edges = edges.drop_nulls()

        edges = edges.with_columns(pl.col("subject").replace(node_mapping), pl.col("predicate").replace(predicate_mapping), pl.col("object").replace(node_mapping))
        edges_path = os.path.join(self.data_path, "source_edges.jsonl")
        edges.write_ndjson(edges_path)

        # class GENERICDATACOLS(enum.IntEnum):
        #     SOURCE_ID = 2
        #     SOURCE_LABEL = 3
        #     TARGET_ID = 5
        #     TARGET_LABEL = 6
        #     PREDICATE = 7
        #
        #
        # PREDICATE_MAPPING = {
        #     "in_similarity_relationship_with": "biolink:chemically_similar_to",
        #     "negatively_regulates": "RO:0002212",
        #     "positively_regulates": "RO:0002213"
        # }

        # extractor = Extractor(file_writer=self.output_file_writer)
        # mw_edge_file: str = os.path.join(self.data_path, "MW.edges.csv")
        # with open(mw_edge_file, 'rt') as fp:
        #     extractor.csv_extract(fp,
        #                           lambda line: self.resolve_id(line[GENERICDATACOLS.SOURCE_ID.value]),  # source id
        #                           lambda line: self.resolve_id(line[GENERICDATACOLS.TARGET_ID.value]),  # target id
        #                           lambda line: PREDICATE_MAPPING[line[GENERICDATACOLS.PREDICATE.value]],  # predicate extractor
        #                           lambda line: {},  # subject properties
        #                           lambda line: {},  # object properties
        #                           lambda line: self.get_edge_properties(),  # edge properties
        #                           comment_character='#',
        #                           delim=',',
        #                           has_header_row=True)

        return { 'record_counter': 0, 'skipped_record_counter': null_nodes_count, 'errors': []}

if __name__ == '__main__':

    source_data_dir = str(os.path.join(os.environ.get("ORION_STORAGE"), "MetabolomicsWorkbench", "2024-05-08"))
    loader = MetabolomicsWorkbenchLoader(source_data_dir=source_data_dir)
    loader.get_data()
    # print(loader.parse_data())