import json
import logging
import os
import polars as pl

from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, KNOWLEDGE_LEVEL, AGENT_TYPE
from Common.kgx_file_writer import KGXFileWriter
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import PUBCHEM_COMPOUND
from Common.utils import LoggingUtil


class SourceDataLoader:

    # implementations of parsers should override and increment this whenever they change
    parsing_version = "1.0"

    # implementations of parsers can override this with True to indicate that unconnected nodes should be preserved
    preserve_unconnected_nodes = False

    # implementations of parsers should override this with True when the source data will contain sequence variants
    has_sequence_variants = False

    generator_code = "https://github.com/RobokopU24/ORION"

    # parsers should override all of these attributes:
    source_id = ""
    provenance_id = ""
    description = ""
    source_data_url = ""
    license = ""
    attribution = ""

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """Initialize with the option to run in testing mode."""
        self.test_mode: bool = test_mode

        if source_data_dir:
            self.data_path = os.path.join(source_data_dir, "source")
            if not os.path.exists(self.data_path):
                os.mkdir(self.data_path)
        else:
            self.data_path = os.environ.get("ORION_STORAGE")

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # placeholder for lazy instantiation
        self.output_file_writer: KGXFileWriter = None

        # create a logger
        self.logger = LoggingUtil.init_logging(f"ORION.parsers.{self.get_name()}",
                                               level=logging.INFO,
                                               line_format='medium',
                                               log_file_path=os.environ.get('ORION_LOGS'))

    def get_latest_source_version(self):
        """Determine and return the latest source version ie. a unique identifier associated with the latest version."""
        raise NotImplementedError

    def get_data(self):
        """Download the source data"""
        raise NotImplementedError

    def parse_data(self):
        """Parse the downloaded data into kgx files"""
        raise NotImplementedError

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        :param edges_output_file_path: path to the new nodes output file
        :param nodes_output_file_path: path to the new edges output file
        :return: dict of metadata about the loading
        """
        source_name = self.get_name()
        self.logger.info(f'{source_name}: Processing beginning')

        try:
            # TODO really this step should not be here - there were a few parsers that did not implement fetch/get_data
            # in the same way as the others. Ideally you would never get here if the data was not fetched.
            # So this could be removed after a review that confirms all sources fetch successfully during get_data().
            if self.needs_data_download():
                error_message = f'{source_name}: Error - Retrieving files failed.'
                self.logger.error(error_message)
                raise SourceDataFailedError(error_message)

            # create a KGX file writer, parsers may use this
            self.output_file_writer = KGXFileWriter(nodes_output_file_path,
                                                    edges_output_file_path)

            # parse the data
            load_metadata = self.parse_data()
            if 'errors' in load_metadata and load_metadata['errors']:
                self.logger.error(f'{source_name}: Experienced {len(load_metadata["errors"])} errors while parsing... examples: {load_metadata["errors"][:10]}')
                load_metadata['parsing_error_examples'] = load_metadata.pop('errors')[:10]
            self.logger.info(f'{source_name}: Parsing complete.')

            # if nodes or edges were queued, write them to file
            if self.final_node_list or self.final_edge_list:
                self.logger.info(f'{source_name}: Writing to file...')
                self.write_to_file()

            load_metadata['repeat_nodes'] = self.output_file_writer.repeat_node_count
            load_metadata['source_nodes'] = self.output_file_writer.nodes_written
            load_metadata['source_edges'] = self.output_file_writer.edges_written

        except Exception:
            raise

        finally:
            if self.output_file_writer:
                self.output_file_writer.close()

            # remove the temp data files or do any necessary clean up
            self.clean_up()

        self.logger.info(f'{self.get_name()}: Processing complete')

        return load_metadata

    def needs_data_download(self):
        try:
            # some implementations will have one data_file
            if self.data_file:
                downloaded_data = os.path.join(self.data_path, self.data_file)
                # check if the one file already exists - if it does return false, does not need a download
                if os.path.exists(downloaded_data):
                    return False
                return True
        except AttributeError:
            pass
        try:
            # and some may have many
            if self.data_files:
                # for many files - if any of them do not exist return True to download them
                for data_file_name in self.data_files:
                    downloaded_data = os.path.join(self.data_path, data_file_name)
                    if not os.path.exists(downloaded_data):
                        return True
                return False
        except AttributeError:
            pass

    def clean_up(self):
        # as of now we decided to not remove source data after parsing
        # this function could still be overridden by parsers to remove temporary files or workspace clutter
        pass
        """
        try:
            # some implementations will have one data_file
            if self.data_file:
                file_to_remove = os.path.join(self.data_path, self.data_file)
                if os.path.exists(file_to_remove):
                    os.remove(file_to_remove)
        except AttributeError:
            pass
        try:
            # and some may have many
            if self.data_files:
                for data_file_name in self.data_files:
                    file_to_remove = os.path.join(self.data_path, data_file_name)
                    if os.path.exists(file_to_remove):
                        os.remove(file_to_remove)
        except AttributeError:
            pass
        """

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_source_meta_information(self):
        return {
            'provenance': self.provenance_id,
            'description': self.description,
            'source_data_url': self.source_data_url,
            'license': self.license,
            'attribution': self.attribution
        }

    def write_to_file(self) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files
        """

        # for each node captured
        for node in self.final_node_list:
            # write out the node
            self.output_file_writer.write_kgx_node(node)

        # for each edge captured
        for edge in self.final_edge_list:
            # write out the edge data
            self.output_file_writer.write_kgx_edge(edge)


    def pad_curie(self, curie):
        if curie.startswith("UBERON") or curie.startswith("MONDO"):
            split = curie.split(":")
            prefix = split[0]
            suffix = split[1].zfill(7)
            return f"{prefix}:{suffix}"
        return curie


    def parse_cfde_source(self, config, data_path) -> dict:
        nodes = pl.DataFrame(schema={"id": pl.String, "original_id": pl.String, "name": pl.String, "category": pl.String})

        dfs = [nodes]
        for file in config["node_files"]:
            tmp_df = pl.scan_csv(os.path.join(data_path, file["node_file"]["name"]), has_header=True)
            if "secondary_id_column" in file["node_file"]:
                tmp_df = tmp_df.select(
                    pl.when(pl.col(file["node_file"]["primary_id_column"]).is_null()).then(pl.col(file["node_file"]["secondary_id_column"])).otherwise(pl.col(file["node_file"]["primary_id_column"])).alias("id"),
                    pl.col("").alias("original_id"),
                    pl.col("label").alias("name"),
                    pl.when(pl.col("type").is_null()).then(pl.lit(file["node_file"]["type"])).otherwise(pl.col("type")).alias("category")
                )
            else:
                tmp_df = tmp_df.select(
                    pl.col(file["node_file"]["primary_id_column"]).alias("id"),
                    pl.col("").alias("original_id"),
                    pl.col("label").alias("name"),
                    pl.when(pl.col("type").is_null()).then(pl.lit(file["node_file"]["type"])).otherwise(pl.col("type")).alias("category")
                )
            tmp_df = tmp_df.with_columns(
                pl.when(pl.col("id").str.starts_with("PUBCHEM")).then(pl.col("id").str.replace("PUBCHEM", PUBCHEM_COMPOUND)).otherwise(pl.col("id")).alias("id"),
            ).with_columns(pl.col("id").map_elements(self.pad_curie, return_dtype=pl.String)).collect()
            # tmp_df.write_csv(f'/tmp/{file["node_file"]["type"]}.csv')
            dfs.append(tmp_df)
            
        nodes = pl.concat(dfs, how="vertical")
    
        df_missing = nodes.filter(pl.any_horizontal(pl.all().is_null()))
        unmapped_path = os.path.join(data_path, "unmapped.jsonl")
        df_missing.write_ndjson(unmapped_path)
    
        missing_mapping = dict(zip(df_missing["original_id"], df_missing["id"]))
    
        nodes = nodes.drop_nulls()
        
        node_mapping = dict(zip(nodes["original_id"], nodes["id"]))
        # with open('/tmp/node_mapping.json', 'w') as file:
        #     file.write(json.dumps(node_mapping))
            
        nodes.drop_in_place("original_id")
    
        for row in nodes.rows(named=True):
            node = kgxnode(identifier=row['id'], name=row['name'], categories=row['category'])
            self.final_node_list.append(node)
    
        nodes_path = os.path.join(self.data_path, "nodes.jsonl")
        nodes.write_ndjson(nodes_path)
    
        edges = pl.scan_csv(os.path.join(self.data_path, config['edge_file']), has_header=True).select(
            pl.col("source").alias("subject"),
            pl.col("relation").alias("predicate"),
            pl.col("target").alias("object"),
            pl.lit(config['provenance_id']).alias(PRIMARY_KNOWLEDGE_SOURCE),
            pl.lit("data_analysis_pipeline").alias(AGENT_TYPE),
            pl.lit("knowledge_assertion").alias(KNOWLEDGE_LEVEL),
        ).collect()

        edges = edges.with_columns(pl.col("subject").replace(missing_mapping), pl.col("predicate"), pl.col("object").replace(missing_mapping)).drop_nulls()

        predicate_mapping = dict(config['predicate_mapping'])
        edges = edges.with_columns(pl.col("subject").replace(node_mapping).alias("subject"), pl.col("predicate").replace(predicate_mapping).alias("predicate"), pl.col("object").replace(node_mapping).alias("object"))
        edges = edges.unique(subset=["subject", "predicate", "object", "primary_knowledge_source"])
        
        # print(edges.pivot(on="primary_knowledge_source", index="predicate", values="subject", aggregate_function="count"))
        edges_path = os.path.join(self.data_path, "edges.jsonl")
        edges.write_ndjson(edges_path)

        edges = [kgxedge(subject_id=row['subject'], predicate=row['predicate'], object_id=row['object'], primary_knowledge_source=row[PRIMARY_KNOWLEDGE_SOURCE], edgeprops={ KNOWLEDGE_LEVEL: row[KNOWLEDGE_LEVEL], AGENT_TYPE: row[AGENT_TYPE]}) for row in edges.rows(named=True)]    
        print(len(edges))
        # for row in edges.rows(named=True):
        #     edge = 
        #     # print(f"{edge.subjectid}, {edge.predicate}, {edge.objectid}")
        #     self.final_edge_list.append(edge)
        self.final_edge_list.extend(edges)
        print(len(self.final_edge_list))
        
        return { 'record_counter': len(edges), 'skipped_record_counter': len(df_missing), 'errors': []}

class SourceDataBrokenError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


class SourceDataFailedError(Exception):
    def __init__(self, error_message: str):
        self.error_message = error_message


    
