import os
import json
import requests
from collections import defaultdict
from vectordb import InMemoryExactNNVectorDB
from docarray import BaseDoc, DocList
from docarray.typing import NdArray

from Common.config import CONFIG

OPENAI_API_KEY = CONFIG.get("OPENAI_API_KEY")

FALLBACK_PREDICATE = "biolink:related_to"


class PredicateText(BaseDoc):
    predicate: str = ''
    text: str = ''
    embedding: NdArray[3072]


class PredicateDatabase:
    def __init__(self, logger, workspace_dir='./workspace'):
        self.db = None
        self.logger = logger
        self.workspace_dir = workspace_dir

    def map_biolink_predicates(self, data: dict, output_file=None) -> dict:
        text_maps = defaultdict(set)
        for entry in data:
            fields = list(data[entry].keys())
            mappings = [field for field in fields if "mapping" in field]
            if mappings:
                mapped_term = data[entry][mappings[0]]
            else:
                mapped_term = entry

            text = data[entry]["text"]
            if isinstance(text, str):
                text = [text]
            text_maps[mapped_term].update(text)

        for mapping in text_maps:
            text_maps[mapping] = list(text_maps[mapping])

        if output_file is not None:
            with open(output_file, "w") as outf:
                outf.write(json.dumps(text_maps, indent=2))

        return text_maps

    def openai_embed(self, text) -> list:
        """Given a chunk of text, send it to the openai API for embedding and return the result"""
        model = "text-embedding-3-large"
        payload = {"model": model, "input": text}
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }
        url = "https://api.openai.com/v1/embeddings"
        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code != 200:
                response.raise_for_status()
            j = response.json()
            vector = j["data"][0]["embedding"]
        except Exception as e:
            self.logger.error(f"OpenAI Error: {e}")
            raise e

        return vector

    def load_db_from_json(self, embeddings_file):
        # self.logger.info("Load json")
        with open(embeddings_file, "r") as f:
            embeddings = json.load(f)

        doc_list = []
        for entry in embeddings:
            if len(entry["embedding"]) != 0:
                doc_list.append(
                    PredicateText(
                        predicate=entry["predicate"],
                        text=entry["text"],
                        embedding=entry["embedding"]
                    )
                )

        # self.logger.info("Load vectordb")
        self.db = InMemoryExactNNVectorDB[PredicateText](workspace=self.workspace_dir)
        self.db.index(inputs=DocList[PredicateText](doc_list))
        # self.logger.info("Ready")

    def search(self, query_text):
        embedding = self.openai_embed(query_text)
        if len(embedding) == 0:
            return None

        query = PredicateText(text=query_text, embedding=embedding)
        results = self.db.search(inputs=DocList[PredicateText]([query]), limit=10)

        return results[0].matches[0], results[0].scores[0]


class PredicateMapping:

    def __init__(self,
                 predicate_vectors_file_path: str,
                 logger,
                 predicate_map_cache_file_path: str = None,
                 predicate_score_threshold: float = None,
                 workspace_dir: str = './workspace'):

        self.predicate_map = {}
        self.predicate_map_cache_file_path = predicate_map_cache_file_path
        self.load_cached_predicate_mappings()

        self.predicate_score_threshold = predicate_score_threshold

        self.predicate_database = PredicateDatabase(logger=logger, workspace_dir=workspace_dir)
        self.predicate_database.load_db_from_json(predicate_vectors_file_path)

        self.logger = logger

    def get_mapped_predicate(self, predicate: str):
        if predicate in self.predicate_map:
            mapped_predicate, predicate_mapping_score = self.predicate_map[predicate]
        else:
            # self.logger.info(f'calling openAI for predicate embedding {predicate}')
            predicate_mapping_result, predicate_mapping_score = self.predicate_database.search(predicate)
            mapped_predicate = predicate_mapping_result.predicate
            self.predicate_map[predicate] = mapped_predicate, predicate_mapping_score
        if self.predicate_score_threshold and predicate_mapping_score < self.predicate_score_threshold:
            mapped_predicate = FALLBACK_PREDICATE
        return mapped_predicate

    def load_cached_predicate_mappings(self):
        if self.predicate_map_cache_file_path and os.path.exists(self.predicate_map_cache_file_path):
            with open(self.predicate_map_cache_file_path, "r") as mapped_predicates_file:
                self.predicate_map = json.load(mapped_predicates_file)

    def save_cached_predicate_mappings(self):
        if self.predicate_map_cache_file_path:
            with open(self.predicate_map_cache_file_path, "w") as mapped_predicates_file:
                return json.dump(self.predicate_map,
                                 mapped_predicates_file,
                                 indent=4,
                                 sort_keys=True)


if __name__ == '__main__':
    db = PredicateDatabase()
    db.load_db_from_json("mapped_predicate_vectors.json")

    query = "reduces"
    match, score= db.search(query)
    print(f"query: {query}")
    print("best_match:")
    print(f"\tPredicate: {match.predicate}")
    print(f"\tText: {match.text}")
    print(f"\tScore: {score}")
