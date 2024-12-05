
import requests
from requests.adapters import HTTPAdapter, Retry

from parsers.LitCoin.src.NER.nameres import NameResNEREngine
from parsers.LitCoin.src.NER.sapbert import SAPBERTNEREngine
from parsers.LitCoin.src.bagel.bagel_gpt import ask_classes_and_descriptions, LLM_RESULTS
from Common.normalization import NODE_NORMALIZATION_URL


BAGEL_SUBJECT_SYN_TYPE = 'subject_bagel_syn_type'
BAGEL_OBJECT_SYN_TYPE = 'object_bagel_syn_type'

# output of parse_gpt looks like {"entity": triple["object"], "qualifier": triple["object_qualifier"]}
session = requests.Session()
retries = Retry(total=6,
                backoff_factor=1,
                status_forcelist=[502, 503, 504, 520, 429])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))
nameres = NameResNEREngine(session)
sapbert = SAPBERTNEREngine(session)


def get_orion_bagel_results(text, term, abstract_id):
    # print(f'bagelizing term {term}.')
    taxon_id_to_name = {}
    try:
        nr_results = nameres.annotate(term, props={}, limit=10)
    except requests.exceptions.HTTPError:
        nr_results = []
    try:
        sb_results = sapbert.annotate(term, props={}, limit=10)
    except requests.exceptions.HTTPError:
        sb_results = []

    # We have results from both nr and sb. But we want to fill those out with consistent information that may
    # or may not be returned from each source
    # First merge the results by identifier (not label) and store in terms dict
    terms = {}
    update_by_id(terms, nr_results, "NameRes")
    update_by_id(terms, sb_results, "SAPBert")
    augment_results(terms, nameres, taxon_id_to_name)
    # make the call to the LLM asking it to classify the synonyms
    gpt_class_desc_response = ask_classes_and_descriptions(text, term, terms, abstract_id, session)
    return gpt_class_desc_response


def convert_orion_bagel_result_to_bagel_service_format(orion_bagel_result):
    converted_results = {}
    for match_type, matches in orion_bagel_result.items():
        for match in matches:
            converted_result = {"category": f"biolink:{match['biolink_type']}",
                                "description": match["description"],
                                "name": match["label"]}
            for ner_response in match["return_parameters"]:
                if ner_response["source"] == "NameRes":
                    converted_result["name_res_rank"] = ner_response["rank"]
                    converted_result["nameres_score"] = ner_response["score"]
            # walk through twice to ensure name res before sapbert in dict order
            for ner_response in match["return_parameters"]:
                if ner_response["source"] == "SAPBert":
                    converted_result["sapbert_rank"] = ner_response["rank"]
                    converted_result["sapbert_score"] = float(ner_response["score"])
            converted_result["synonym_type"] = match_type
            converted_result["taxa"] = match["taxa"]
            if match["curie"] in converted_result:
                raise Exception(f'Multiple instances of the same curie in bagel result: {match["curie"]} in '
                                f'{orion_bagel_result}')
            converted_results[match["curie"]] = converted_result
    return converted_results


def extract_best_match(bagel_results):
    # This whole function could be redesigned, it was supposed to be a ranking algorithm that would select the overall
    # best match out of potentially ambiguous results (from a list with different rankings coming from name res and
    # sapbert). In practice, there shouldn't be more than one exact match from either service, we should always select
    # one of those if they exist. The way this works now, picking the highest rank from either service,
    # with a heirarchy of match types, isn't necessarily what we want.
    if not bagel_results:
        return None
    ranking = {
        "exact": [],
        "narrow": [],
        "broad": [],
        "related": []
    }
    for result_curie, result in bagel_results.items():
        syn_type = result["synonym_type"]
        if syn_type not in ranking:
            continue
        rank = min(result.get("name_res_rank", 1000), result.get("sapbert_rank", 1000))
        ranking[syn_type].append({"id": result_curie,
                                  "name": result["name"],
                                  "synonym_type": result["synonym_type"],
                                  "rank": rank})
    ranking["exact"] = sorted(ranking["exact"], key=lambda k: k["rank"])
    ranking["narrow"] = sorted(ranking["narrow"], key=lambda k: k["rank"])
    ranking["broad"] = sorted(ranking["broad"], key=lambda k: k["rank"])
    ranking["related"] = sorted(ranking["related"], key=lambda k: k["rank"])
    if ranking["exact"]:
        return ranking["exact"][0]
    elif ranking["narrow"]:
        return ranking["narrow"][0]
    elif ranking["broad"]:
        return ranking["broad"][0]
    elif ranking["related"]:
        return ranking["related"][0]
    else:
        # throw out "unrelated" results, or return None if no matches come back
        return None


def augment_results(terms, nameres, taxes):
    """Given a dict where the key is a curie, and the value are data about the match, augment the value with
    results from nameres's reverse lookup.
    For cases where we get back a taxa, add the taxa name to the label of the item."""
    curies = list(terms.keys())
    augs = nameres.reverse_lookup(curies)
    for curie in augs:
        terms[curie].update(augs[curie])
        resp = requests.get(f"{NODE_NORMALIZATION_URL}get_normalized_nodes?curie="+curie+"&conflate=true&drug_chemical_conflate=true&description=true")
        if resp.status_code == 200:
            result = resp.json()
            try:
                terms[curie]["description"] = result[curie]["id"].get("description","")
            except:
                # print("No curie?" , curie)
                terms[curie]["description"] = ""
    for curie, annotation in terms.items():
        if len(annotation["taxa"]) > 0:
            tax_id = annotation["taxa"][0]
            if tax_id not in taxes:
                resp = requests.get(f"{NODE_NORMALIZATION_URL}get_normalized_nodes?curie="+tax_id)
                if resp.status_code == 200:
                    result = resp.json()
                    try:
                        tax_name = result[tax_id]["id"]["label"]
                    except (TypeError, KeyError):
                        tax_name = f'Taxon name failed for {tax_id}'
                    taxes[tax_id] = tax_name
            tax_name = taxes[tax_id]
            annotation["label"] = f"{annotation['label']} ({tax_name})"


def update_by_id(terms, results, source):
    for i, result in enumerate(results):
        identifier = result["id"]
        if identifier not in terms:
            terms[identifier] = {"return_parameters": []}
        r = {"source": source, "score": result["score"], "rank": i + 1}
        terms[identifier]["return_parameters"].append(r)
        terms[identifier]["label"] = result["label"]


def update_by_label(terms, results, source):
    for i,result in enumerate(results):
        r = {}
        label = result["label"]
        r["source"] = source
        r["score"] = result["score"]
        r["rank"] = i+1
        r["identifier"] = result["id"]
        terms[label].append(r)


def get_llm_results():
    return LLM_RESULTS

