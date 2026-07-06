import os
import requests
from collections import defaultdict

from orion.biolink_constants import (
    AGENT_TYPE,
    DATA_PIPELINE,
    KNOWLEDGE_LEVEL,
    LOGICAL_ENTAILMENT,
    PHENOTYPIC_FEATURE,
    SUPPORTING_DATA_SOURCE,
)
from orion.loader_interface import SourceDataLoader
from orion.utils import GetData, GetDataPullError


HOMOLOGOUS_TO_PREDICATE = "biolink:homologous_to"
DEFAULT_EXISTING_HOMOLOGY_PREDICATES = frozenset(
    {
        HOMOLOGOUS_TO_PREDICATE,
        "RO:HOM0000007",
        "homologous_to",
    }
)


def curie_has_prefix(curie: str, prefixes: tuple[str, ...]) -> bool:
    return curie.split(":", 1)[0] in prefixes if ":" in curie else False


def symmetric_pair_key(subject_id: str, object_id: str) -> tuple[str, str]:
    return tuple(sorted((subject_id, object_id)))


def parse_obo_term(term_lines: list[str]) -> dict:
    term = {
        "id": None,
        "is_a": [],
        "relationships": [],
    }
    for line in term_lines:
        if line.startswith("id: "):
            term["id"] = line[4:]
        elif line.startswith("is_a: "):
            term["is_a"].append(line[6:].split()[0])
        elif line.startswith("relationship: "):
            term["relationships"].append(line[14:])
    return term


def iter_obo_terms(obo_file_path: str):
    term_lines = None
    with open(obo_file_path, encoding="utf-8", errors="replace") as obo_file:
        for line in obo_file:
            line = line.rstrip("\n")
            if line == "[Term]":
                if term_lines is not None:
                    yield parse_obo_term(term_lines)
                term_lines = []
            elif line.startswith("["):
                if term_lines is not None:
                    yield parse_obo_term(term_lines)
                term_lines = None
            elif term_lines is not None:
                term_lines.append(line)

    if term_lines is not None:
        yield parse_obo_term(term_lines)


def infer_homology_pairs(
    species_a_by_parent: dict[str, set[str]],
    species_b_by_parent: dict[str, set[str]],
    existing_homology_pairs: set[tuple[str, str]],
) -> tuple[dict[tuple[str, str], list[str]], dict[str, int]]:
    inferred_pairs = defaultdict(list)
    skipped_existing_homology_edges = 0
    duplicate_candidate_edges = 0

    for generic_parent in sorted(set(species_a_by_parent) & set(species_b_by_parent)):
        for species_a_term in sorted(species_a_by_parent[generic_parent]):
            for species_b_term in sorted(species_b_by_parent[generic_parent]):
                pair_key = (species_a_term, species_b_term)
                if symmetric_pair_key(species_a_term, species_b_term) in existing_homology_pairs:
                    skipped_existing_homology_edges += 1
                    continue
                if pair_key in inferred_pairs:
                    duplicate_candidate_edges += 1
                inferred_pairs[pair_key].append(generic_parent)

    return dict(inferred_pairs), {
        "skipped_existing_homology_edges": skipped_existing_homology_edges,
        "duplicate_candidate_edges": duplicate_candidate_edges,
    }


class UPhenoPhenotypeHomologyLoader(SourceDataLoader):
    provenance_id = "infores:upheno"
    parsing_version = "1.0"
    source_data_url = "https://purl.obolibrary.org/obo/upheno.obo"
    license = "https://github.com/obophenotype/upheno/blob/master/LICENSE"
    attribution = "https://github.com/obophenotype/upheno"

    generic_phenotype_prefixes = ("UPHENO",)
    species_a_phenotype_prefixes = ()
    species_b_phenotype_prefixes = ()
    existing_homology_predicates = DEFAULT_EXISTING_HOMOLOGY_PREDICATES

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.data_url = "https://purl.obolibrary.org/obo/upheno.obo"
        self.data_file = "upheno.obo"

    def get_latest_source_version(self):
        response = requests.get("https://api.github.com/repos/obophenotype/upheno/releases/latest", timeout=30)
        response.raise_for_status()
        tag_name = response.json()["tag_name"]
        if not tag_name.startswith("v"):
            raise GetDataPullError(f"Unexpected UPheno release tag format: {tag_name}")
        return tag_name.removeprefix("v")

    def get_data(self):
        GetData().pull_via_http(self.data_url, self.data_path, saved_file_name=self.data_file)
        return True

    def parse_data(self):
        if not self.species_a_phenotype_prefixes or not self.species_b_phenotype_prefixes:
            raise ValueError("species_a_phenotype_prefixes and species_b_phenotype_prefixes must be configured")

        term_counter = 0
        generic_parent_counter = 0
        species_a_by_parent = defaultdict(set)
        species_b_by_parent = defaultdict(set)
        existing_homology_pairs = set()

        obo_file_path = os.path.join(self.data_path, self.data_file)
        for term in iter_obo_terms(obo_file_path):
            term_counter += 1
            term_id = term["id"]
            if not term_id:
                continue

            for generic_parent in term["is_a"]:
                if not curie_has_prefix(generic_parent, self.generic_phenotype_prefixes):
                    continue
                generic_parent_counter += 1
                if curie_has_prefix(term_id, self.species_a_phenotype_prefixes):
                    species_a_by_parent[generic_parent].add(term_id)
                elif curie_has_prefix(term_id, self.species_b_phenotype_prefixes):
                    species_b_by_parent[generic_parent].add(term_id)

            self._index_existing_homology_relationships(term_id, term["relationships"], existing_homology_pairs)

        inferred_pairs, inference_metadata = infer_homology_pairs(
            species_a_by_parent,
            species_b_by_parent,
            existing_homology_pairs,
        )

        for (species_a_term, species_b_term), generic_parents in sorted(inferred_pairs.items()):
            self._write_homology_edge(species_a_term, species_b_term, sorted(generic_parents))

        common_generic_parents = set(species_a_by_parent) & set(species_b_by_parent)
        candidate_edges = sum(
            len(species_a_by_parent[parent]) * len(species_b_by_parent[parent])
            for parent in common_generic_parents
        )
        return {
            "num_source_terms": term_counter,
            "generic_parent_assertions": generic_parent_counter,
            "species_a_terms_with_generic_parents": len({term for terms in species_a_by_parent.values() for term in terms}),
            "species_b_terms_with_generic_parents": len({term for terms in species_b_by_parent.values() for term in terms}),
            "common_generic_parents": len(common_generic_parents),
            "candidate_homology_edges": candidate_edges,
            "duplicate_candidate_edges": inference_metadata["duplicate_candidate_edges"],
            "existing_homology_edges": len(existing_homology_pairs),
            "skipped_existing_homology_edges": inference_metadata["skipped_existing_homology_edges"],
            "inferred_homology_edges": len(inferred_pairs),
        }

    def _index_existing_homology_relationships(
        self,
        term_id: str,
        relationships: list[str],
        existing_homology_pairs: set[tuple[str, str]],
    ) -> None:
        for relationship in relationships:
            relationship_parts = relationship.split()
            if len(relationship_parts) < 2:
                continue
            predicate, object_id = relationship_parts[:2]
            if predicate in self.existing_homology_predicates and self._is_species_pair(term_id, object_id):
                existing_homology_pairs.add(symmetric_pair_key(term_id, object_id))

    def _is_species_pair(self, subject_id: str, object_id: str) -> bool:
        return (
            curie_has_prefix(subject_id, self.species_a_phenotype_prefixes)
            and curie_has_prefix(object_id, self.species_b_phenotype_prefixes)
        ) or (
            curie_has_prefix(subject_id, self.species_b_phenotype_prefixes)
            and curie_has_prefix(object_id, self.species_a_phenotype_prefixes)
        )

    def _write_homology_edge(
        self,
        species_a_term: str,
        species_b_term: str,
        generic_parents: list[str],
    ) -> None:
        self.output_file_writer.write_node(
            node_id=species_a_term,
            node_types=[PHENOTYPIC_FEATURE],
        )
        self.output_file_writer.write_node(
            node_id=species_b_term,
            node_types=[PHENOTYPIC_FEATURE],
        )
        self.output_file_writer.write_edge(
            subject_id=species_a_term,
            object_id=species_b_term,
            predicate=HOMOLOGOUS_TO_PREDICATE,
            primary_knowledge_source=self.provenance_id,
            edge_properties={
                KNOWLEDGE_LEVEL: LOGICAL_ENTAILMENT,
                AGENT_TYPE: DATA_PIPELINE,
                SUPPORTING_DATA_SOURCE: self.provenance_id,
                "upheno_generic_parent": generic_parents,
            },
        )


class UPhenoHumanMousePhenotypeHomologyLoader(UPhenoPhenotypeHomologyLoader):
    source_id = "UPhenoHumanMousePhenotypeHomology"
    description = (
        "Human-to-mouse phenotype homology relationships inferred from HP and MP classes "
        "that directly subclass the same species-independent UPheno phenotype."
    )
    species_a_phenotype_prefixes = ("HP",)
    species_b_phenotype_prefixes = ("MP",)
