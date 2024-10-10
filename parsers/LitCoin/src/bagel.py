
import requests
from requests.adapters import HTTPAdapter, Retry

from parsers.LitCoin.src.NER.nameres import NameResNEREngine
from parsers.LitCoin.src.NER.sapbert import SAPBERTNEREngine
from parsers.LitCoin.src.bagel_gpt import ask_classes_and_descriptions
from Common.normalization import NODE_NORMALIZATION_URL

# output of parse_gpt looks like {"entity": triple["object"], "qualifier": triple["object_qualifier"]}
session = requests.Session()
retries = Retry(total=8,
                backoff_factor=.75,
                status_forcelist=[502, 503, 504, 520, 429])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))
nameres = NameResNEREngine(session)
sapbert = SAPBERTNEREngine(session)


def get_orion_bagel_results(text, term):
    # print(f'bagelizing term {term}.')
    taxon_id_to_name = {}
    nr_results = nameres.annotate(term, props={}, limit=10)
    sb_results = sapbert.annotate(term, props={}, limit=10)
    # We have results from both nr and sb. But we want to fill those out with consistent information that may
    # or may not be returned from each source
    # First merge the results by identifier (not label) and store in terms dict
    terms = {}
    update_by_id(terms, nr_results, "NameRes")
    update_by_id(terms, sb_results, "SAPBert")
    augment_results(terms, nameres, taxon_id_to_name)
    gpt_class_desc_response = ask_classes_and_descriptions(text, term, terms, session)
    # gpt_label_response = ask_labels(abstract, term, terms)
    # gpt_class_response = ask_classes(abstract, term, terms)
    return gpt_class_desc_response


def extract_best_match(bagel_results):
    if not bagel_results:
        return None, None
    if "exact" in bagel_results:
        return bagel_results['exact'][0], "exact"
    elif "broad" in bagel_results:
        return bagel_results['broad'][0], "broad"
    elif "narrow" in bagel_results:
        return bagel_results['narrow'][0], "narrow"
    elif "related" in bagel_results:
        return bagel_results['related'][0], "related"
    else:
        # throw out unrelated
        return None, "unrelated"


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
                    tax_name = result[tax_id]["id"]["label"]
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



