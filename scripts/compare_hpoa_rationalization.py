#!/usr/bin/env python3
import argparse
import gzip
import json
from collections import Counter
from pathlib import Path


GENE_PREFIXES = {"HGNC", "MGI", "NCBIGene", "UniProtKB"}
PHENOTYPE_PREFIXES = {"HP"}
GENE_DISEASE_PREDICATES = {
    "biolink:gene_associated_with_condition",
    "biolink:gene_associated_with_disease",
    "gene_associated_with_condition",
}
HAS_PHENOTYPE = "biolink:has_phenotype"


def parse_input_spec(input_spec: str) -> tuple[str, Path]:
    if "::" in input_spec:
        source_id, path = input_spec.split("::", maxsplit=1)
        return source_id, Path(path)
    path = Path(input_spec)
    return "unknown", path


def open_edge_file(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "rt", encoding="utf-8")


def curie_prefix(curie: str) -> str:
    return curie.split(":", maxsplit=1)[0] if ":" in curie else curie


def value_counter_key(value):
    if value is None:
        return "none"
    if isinstance(value, list):
        return "|".join(sorted(str(v) for v in value)) if value else "none"
    return str(value)


def edge_family(edge: dict) -> str:
    subject_prefix = curie_prefix(edge.get("subject", ""))
    object_prefix = curie_prefix(edge.get("object", ""))
    predicate = edge.get("predicate", "")
    if predicate == HAS_PHENOTYPE and object_prefix in PHENOTYPE_PREFIXES:
        if subject_prefix in GENE_PREFIXES:
            return "gene_phenotype"
        return "disease_phenotype"
    if predicate in GENE_DISEASE_PREDICATES:
        return "gene_disease"
    return "other"


def summarize_edge_file(source_id: str, path: Path) -> dict:
    counters = {
        "by_source_id": Counter(),
        "by_predicate": Counter(),
        "by_primary_knowledge_source": Counter(),
        "by_supporting_data_source": Counter(),
        "by_aggregator_knowledge_source": Counter(),
        "by_source_predicate_primary": Counter(),
        "by_subject_object_prefix_pair": Counter(),
        "by_edge_family": Counter(),
    }
    total_edges = 0

    with open_edge_file(path) as fp:
        for line in fp:
            if not line.strip():
                continue
            edge = json.loads(line)
            total_edges += 1
            edge_source_id = edge.get("source_id") or edge.get("provided_by") or source_id
            predicate = edge.get("predicate", "missing")
            primary_source = edge.get("primary_knowledge_source", "missing")
            supporting_source = edge.get("supporting_data_source", "none")
            aggregator_source = edge.get("aggregator_knowledge_source")
            prefix_pair = (
                curie_prefix(edge.get("subject", "")),
                curie_prefix(edge.get("object", "")),
            )

            counters["by_source_id"][edge_source_id] += 1
            counters["by_predicate"][predicate] += 1
            counters["by_primary_knowledge_source"][primary_source] += 1
            counters["by_supporting_data_source"][supporting_source] += 1
            counters["by_aggregator_knowledge_source"][value_counter_key(aggregator_source)] += 1
            counters["by_source_predicate_primary"][
                f"{edge_source_id}\t{predicate}\t{primary_source}"
            ] += 1
            counters["by_subject_object_prefix_pair"][f"{prefix_pair[0]}\t{prefix_pair[1]}"] += 1
            counters["by_edge_family"][edge_family(edge)] += 1

    result = {"total_edges": total_edges}
    result.update({name: dict(counter) for name, counter in counters.items()})
    return result


def merge_summaries(summaries: list[dict]) -> dict:
    merged_counters = {}
    total_edges = 0
    for summary in summaries:
        total_edges += summary["total_edges"]
        for key, value in summary.items():
            if key == "total_edges":
                continue
            merged_counters.setdefault(key, Counter()).update(value)
    merged = {"total_edges": total_edges}
    merged.update({name: dict(counter) for name, counter in merged_counters.items()})
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize ORION KGX edge files by source, predicate, knowledge source, "
            "prefix pair, and HPOA rationalization edge family. Inputs can be plain "
            "paths or SOURCE_ID::path specs."
        )
    )
    parser.add_argument("edges", nargs="+", help="Edge file path or SOURCE_ID::edge-file path")
    parser.add_argument("--per-file", action="store_true", help="Include a separate summary for each file")
    args = parser.parse_args()

    per_file = {}
    summaries = []
    for input_spec in args.edges:
        source_id, path = parse_input_spec(input_spec)
        summary = summarize_edge_file(source_id, path)
        summaries.append(summary)
        if args.per_file:
            per_file[input_spec] = summary

    output = {"combined": merge_summaries(summaries)}
    if args.per_file:
        output["per_file"] = per_file
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
