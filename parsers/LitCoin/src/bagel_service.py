import requests
from Common.config import CONFIG

BAGEL_ENDPOINT = 'https://bagel.apps.renci.org/'
BAGEL_ENDPOINT += 'find_curies_openai'

bagel_nameres_url = CONFIG.get('NAMERES_ENDPOINT', 'https://name-resolution-sri.renci.org/')
bagel_nameres_url += 'lookup?autocomplete=false&offset=0&limit=10&string="'

bagel_sapbert_url = CONFIG.get('SAPBERT_URL', 'https://sap-qdrant.apps.renci.org/')
bagel_sapbert_url += "annotate/"

bagel_nodenorm_url = CONFIG.get('NODE_NORMALIZATION_ENDPOINT', 'https://nodenormalization-sri.renci.org/')
bagel_nodenorm_url += 'get_normalized_nodes'

OPENAI_API_KEY = CONFIG.get("OPENAI_API_KEY")
assert OPENAI_API_KEY
OPENAI_API_ORGANIZATION = CONFIG.get("OPENAI_API_ORGANIZATION")
assert OPENAI_API_ORGANIZATION


def call_bagel_service(text, entity, entity_type=''):

    bagel_json = {
        "prompt_name": "bagel/ask_classes",
        "text": text,
        "entity": entity,
        "entity_type": entity_type,
        "config": {
            "llm_model_name": "gpt-4o-mini",
            "organization": OPENAI_API_ORGANIZATION,
            "access_key": OPENAI_API_KEY,
            "llm_model_args": {
              "top_p": 0,
              "temperature": 0.1
            }
        },
        "name_res_url": bagel_nameres_url,
        "sapbert_url": bagel_sapbert_url,
        "nodenorm_url": bagel_nodenorm_url
    }
    print(f'Querying bagel with:\n {bagel_json}')
    bagel_response = requests.post(BAGEL_ENDPOINT,
                                   json=bagel_json)

    if bagel_response.status_code == 200:
        bagel_results = bagel_response.json()
        print(bagel_results)
        return bagel_results
    elif bagel_response.status_code == 403:
        return {'error': '403'}
    elif bagel_response.status_code == 500:
        return {'error': '500'}
    else:
        bagel_response.raise_for_status()


