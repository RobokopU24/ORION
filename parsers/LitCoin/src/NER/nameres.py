import os
import logging

import requests

from comparator.engines.base import BaseNEREngine

# Configuration: NameRes
NAMERES_ENDPOINT = os.getenv('NAMERES_ENDPOINT', 'https://name-resolution-sri-dev.apps.renci.org/lookup')
NAMERES_RL_ENDPOINT = os.getenv('NAMERES_RL_ENDPOINT', 'https://name-resolution-sri-dev.apps.renci.org/reverse_lookup')

# Configuration: the `/get_normalized_nodes` endpoint on a Node Normalization instance to use.
NODE_NORM_ENDPOINT = os.getenv('NODE_NORM_ENDPOINT', 'https://nodenormalization-sri.renci.org/get_normalized_nodes')


class NameResNEREngine(BaseNEREngine):
    def __init__(self, requests_session):
        """
        Create a NameResNEREngine.

        :param requests_session: A Requests session to use for HTTP/HTTPS requests.
        """
        if requests_session:
            self.requests_session = requests_session
        else:
            self.requests_session = requests.Session()

    def annotate(self, text, props, limit=1):
        biolink_type = props.get('biolink_type', '')

        skip_umls = False
        if props.get('skip_umls', False):
            skip_umls = True

        timeout = props.get('timeout', 10) # Default to 10 seconds

        # Make a request to Nemo-Serve.
        nameres_options = {
            'autocomplete': 'false',
            'offset': 0,
            'limit': limit,
            'string': text,
            'biolink_type': biolink_type,
        }

        if skip_umls:
            nameres_options['exclude_prefixes'] = 'UMLS'

        response = self.requests_session.get(NAMERES_ENDPOINT, params=nameres_options, timeout=timeout)
        logging.debug(f"Response from NameRes: {response.content}")
        if not response.ok:
            raise RuntimeError(f"Could not contact NameRes: {response}")

        results = response.json()
        annotations = []

        for result in results:
            biolink_type = 'biolink:NamedThing'
            biolink_types = result.get('types', [])
            if len(biolink_types) > 0:
                biolink_type = biolink_types[0]

            annotation = {
                'text': text,
                'span': {
                    'begin': 0,
                    'end': len(text)
                },
                'id': result.get('curie', ''),
                'label': result.get('label', ''),
                'biolink_type': biolink_type,
                'score': result.get('score', ''),
                'props': {
                    'clique_identifier_count': result.get('clique_identifier_count', ''),
                }
            }

            annotations.append(annotation)

        return annotations

    def reverse_lookup(self,identifiers):
        payload = { "curies": identifiers }
        response = self.requests_session.post(NAMERES_RL_ENDPOINT, json=payload)

        logging.debug(f"Response from NameRes: {response.content}")
        if not response.ok:
            raise RuntimeError(f"Could not contact NameRes: {response}")

        results = response.json()
        annotations = {}

        for input,result in results.items():
            biolink_types = result.get('types', [])
            if len(biolink_types) > 0:
                biolink_type = biolink_types[0]
            else:
                biolink_type = "NamedThing"

            annotation = {
                'biolink_type': biolink_type,
                'clique_identifier_count': result.get('clique_identifier_count', ''),
                'taxa': result.get('taxa', [])
            }

            annotations[input] = annotation

        return annotations
