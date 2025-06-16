import requests
from requests.auth import HTTPBasicAuth
from Common.config import CONFIG

BAGEL_ENDPOINT = 'https://bagel.apps.renci.org/'
BAGEL_ENDPOINT += 'find_curies_openai'

bagel_nameres_url = CONFIG.get('NAMERES_ENDPOINT', 'https://name-resolution-sri.renci.org/')
bagel_nameres_url += 'lookup?autocomplete=false&offset=0&limit=10&string="'

bagel_sapbert_url = CONFIG.get('SAPBERT_URL', 'https://sap-qdrant.apps.renci.org/')
bagel_sapbert_url += "annotate/"

bagel_nodenorm_url = CONFIG.get('NODE_NORMALIZATION_ENDPOINT', 'https://nodenormalization-sri.renci.org/')
bagel_nodenorm_url += 'get_normalized_nodes'

BAGEL_SERVICE_USERNAME = CONFIG.get("BAGEL_SERVICE_USERNAME")
assert BAGEL_SERVICE_USERNAME
BAGEL_SERVICE_PASSWORD = CONFIG.get("BAGEL_SERVICE_PASSWORD")
assert BAGEL_SERVICE_PASSWORD


def call_bagel_service(text, entity, entity_type=''):

    bagel_json = {
        "prompt_name": "bagel/ask_classes",
        "text": text,
        "entity": entity,
        "entity_type": entity_type,
        "config": {
            "llm_model_name": "google/gemma-3-12b-it",
            "organization": "",
            "access_key": "",
            "url": "http://vllm-server/v1",
            "llm_model_args": {
              "top_p": 0.1,
              "temperature": 0.1
            }
        },
        "name_res_url": bagel_nameres_url,
        "sapbert_url": bagel_sapbert_url,
        "nodenorm_url": bagel_nodenorm_url
    }
    # print(f'Querying bagel with:\n {bagel_json}')
    bagel_response = requests.post(BAGEL_ENDPOINT,
                                   auth=HTTPBasicAuth(BAGEL_SERVICE_USERNAME, BAGEL_SERVICE_PASSWORD),
                                   json=bagel_json)

    if bagel_response.status_code == 200:
        bagel_results = bagel_response.json()
        # print(bagel_results)
        return bagel_results
    elif bagel_response.status_code == 403:
        return {'error': '403'}
    elif bagel_response.status_code == 500:
        return {'error': '500'}
    else:
        bagel_response.raise_for_status()


