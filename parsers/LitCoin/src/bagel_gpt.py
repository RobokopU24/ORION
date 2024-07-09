import requests
import json
from collections import defaultdict

from Common.config import CONFIG

OPENAI_API_KEY = CONFIG.get("OPENAI_API_KEY")


def ask_classes_and_descriptions(text, term, termlist):
    """Get GPT results based only on the labels of the terms."""

    # Get the Labels
    labels = defaultdict(list)
    descriptions = defaultdict(list)
    for curie, annotation in termlist.items():
        labels[(annotation["label"], annotation["biolink_type"])].append(curie)
        descriptions[(annotation["label"], annotation["biolink_type"])].append(annotation["description"])
    synonym_list = [(x[0], x[1], d) for x, d in descriptions.items()]

    # Define the Prompt
    prompt = f""" You are an expert in biomedical vocabularies and ontologies. I will provide you with the abstract to a scientific paper, as well as
    a query term: biomedical entity that occurs in that abstract.  I will also provide you a list of possible synonyms for the query term, along
    with their class as defined within their vocabulary, such as Gene or Disease.  This will help you distinguish between
    entities with the same name such as HIV, which could refer to either a particular virus (class OrganismTaxon) or a disease (class Disease). It can also
    help distinguish between a disease hyperlipidemia (class Disease) versus hyperlipidemia as a symptom of another disease (class PhenotpyicFeature).
    For some entities, I will also provide a description of the entity along with the name and class.
    Please determine whether the query term, as it is used in the abstract, is an exact synonym of any of the terms in the list.  There should be at most one
    exact synonym of the query term.  If there are no exact synonyms for the query term in the list, please look for narrow, broad, or related synonyms, 
    The synonym is narrow if the query term is a more specific form of one of the list terms. For example, the query term "Type 2 Diabetes" would be a 
    narrow synonym of "Diabetes" because it is not an exact synonym, but a more specific form. 
    The synonym is broad if the query term is a more general form of the list term.  For instance, the query term "brain injury" would be a broad synonym
    of "Cerebellar Injury" because it is more generic.
    The synonym is related if it is neither exact, narrow, or broad, but is still a similar enough term.  For instance the query term "Pain" would be
    a related synonym of "Pain Disorder".
    It is also possible that there are neither exact nor narrow synonyms of the query term in the list.
    Provide your answers in the following JSON structure:
    [
        {{ 
            "synonym": ...,
            "vocabulary class": ...,
            "synonymType": ...
        }}
    ]
    where the value for synonym is the element from the synonym list, vocabulary class is the 
    class that I input associated with that synonym, and synonymType is either "exact" or "narrow".

    abstract: {text}
    query_term: {term}
    possible_synonyms_classes_and_descriptions: {synonym_list}
    """

    results = query(prompt)

    for result in results:
        syn = result['synonym']
        cls = result['vocabulary class']
        syntype = result['synonymType']
        curies = labels[(syn, cls)]
        for curie in curies:
            termlist[curie]["synonym_Type"] = syntype

    grouped_by_syntype = dict()
    for curie in termlist:
        syntype = termlist[curie].get("synonym_Type", "unrelated")
        termlist[curie]["curie"] = curie
        if syntype not in grouped_by_syntype:
            grouped_by_syntype[syntype] = []
        grouped_by_syntype[syntype].append(termlist[curie])
    return grouped_by_syntype


def ask_classes(text, term, termlist):
    """Get GPT results based only on the labels of the terms."""

    # Get the Labels
    labels = dict()
    for curie, annotation in termlist.items():
        key = (annotation["label"], annotation["biolink_type"])
        if key not in labels:
            labels[key] = []
        labels[key].append(curie)
    synonym_list = list(labels.keys())

    # Define the Prompt
    prompt = f""" You are an expert in biomedical vocabularies and ontologies. I will provide you with the abstract to a scientific paper, as well as
    a query term: biomedical entity that occurs in that abstract.  I will also provide you a list of possible synonyms for the query term, along
    with their class as defined within their vocabulary, such as Gene or Disease.  This will help you distinguish between
    entities with the same name such as HIV, which could refer to either a particular virus (class OrganismTaxon) or a disease (class Disease). It can also
    help distinguish between a disease hyperlipidemia (class Disease) versus hyperlipidemia as a symptom of another disease (class PhenotpyicFeature).
    Please determine whether the query term, as it is used in the abstract, is an exact synonym of any of the terms in the list.  There should be at most one
    exact synonym of the query term.  If there are no exact synonyms for the query term in the list, please look for narrow, broad, or related synonyms, 
    The synonym is narrow if the query term is a more specific form of one of the list terms. For example, the query term "Type 2 Diabetes" would be a 
    narrow synonym of "Diabetes" because it is not an exact synonym, but a more specific form. 
    The synonym is broad if the query term is a more general form of the list term.  For instance, the query term "brain injury" would be a broad synonym
    of "Cerebellar Injury" because it is more generic.
    The synonym is related if it is neither exact, narrow, or broad, but is still a similar enough term.  For instance the query term "Pain" would be
    a related synonym of "Pain Disorder".
    It is also possible that there are neither exact nor narrow synonyms of the query term in the list.
    Provide your answers in the following JSON structure:
    [
        {{ 
            "synonym": ...,
            "vocabulary class": ...,
            "synonymType": ...
        }}
    ]
    where the value for synonym is the element from the synonym list, vocabulary class is the 
    class that I input associated with that synonym, and synonymType is either "exact" or "narrow".

    abstract: {text}
    query_term: {term}
    possible_synonyms_and_classes: {synonym_list}
    """

    results = query(prompt)

    for result in results:
        syn = result['synonym']
        cls = result['vocabulary class']
        syntype = result['synonymType']
        curies = labels[(syn, cls)]
        for curie in curies:
            termlist[curie]["synonym_Type"] = syntype

    grouped_by_syntype = dict()
    for curie in termlist:
        syntype = termlist[curie].get("synonym_Type", "unrelated")
        termlist[curie]["curie"] = curie
        if syntype not in grouped_by_syntype:
            grouped_by_syntype[syntype] = []
        grouped_by_syntype[syntype].append(termlist[curie])
    return grouped_by_syntype


def ask_labels(text, term, termlist):
    """Get GPT results based only on the labels of the terms."""

    # Get the Labels
    labels = dict()
    for curie, annotation in termlist.items():
        if annotation["label"] not in labels:
            labels[annotation["label"]] = []
        labels[annotation["label"]].append(curie)
    synonym_list = list(labels.keys())

    # Define the Prompt
    prompt = f""" You are an expert in biomedical vocabularies and ontologies. I will provide you with the abstract to a scientific paper, as well as
    a query term: biomedical entity that occurs in that abstract.  I will also provide you a list of possible synonyms for the query term.  Please
    determine whether the query term, as it is used in the abstract, is an exact synonym of any of the terms in the list.  There should be at most one
    exact synonym of the query term.  If there are no exact synonyms for the query term in the list, please look for narrow, broad, or related synonyms, 
    The synonym is narrow if the query term is a more specific form of one of the list terms. For example, the query term "Type 2 Diabetes" would be a 
    narrow synonym of "Diabetes" because it is not an exact synonym, but a more specific form. 
    The synonym is broad if the query term is a more general form of the list term.  For instance, the query term "brain injury" would be a broad synonym
    of "Cerebellar Injury" because it is more generic.
    The synonym is related if it is neither exact, narrow, or broad, but is still a similar enough term.  For instance the query term "Pain" would be
    a related synonym of "Pain Disorder".
    It is also possible that there are neither exact nor narrow synonyms of the query term in the list.
    Provide your answers in the following JSON structure:
    [
        {{ 
            "synonym": ...,
            "synonymType": ...
        }}
    ]
    where the value for synonym is the element from the synonym list, and synonymType is either "exact" or "narrow".

    abstract: {text}
    query_term: {term}
    possible_synonyms: {synonym_list}
    """

    results = query(prompt)

    for result in results:
        syn = result['synonym']
        if 'synonymType' in result:
            syntype = result['synonymType']
        elif 'synonymousType' in result:
            syntype = result['synonymousType']
        else:
            print(f'{result} does not contain synonymType or synonymousType')
            continue
        curies = labels[syn]
        for curie in curies:
            termlist[curie]["synonym_Type"] = syntype

    grouped_by_syntype = dict()
    for curie in termlist:
        syntype = termlist[curie].get("synonym_Type", "unrelated")
        termlist[curie]["curie"] = curie
        if syntype not in grouped_by_syntype:
            grouped_by_syntype[syntype] = []
        grouped_by_syntype[syntype].append(termlist[curie])
    return grouped_by_syntype


def query(prompt):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    payload = {
        "model": "gpt-4-0125-preview",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    }

    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    if response.status_code != 200:
        print(f'openai call returned non-200 status: {response.status_code}')
        response.raise_for_status()
    try:
        content = response.json()["choices"][0]["message"]["content"]
        # print(content)
    except KeyError as k:
        print(f'openai json did not contain expected key {k}: {response.json()}')
        raise k

    chunk = content[content.index("["):(content.rindex("]") + 1)]
    try:
        output = json.loads(chunk)
    except json.JSONDecodeError as e:
        print(f'openai results not contain valid json chunk, chunk: {chunk}')
        output = []
    return output
