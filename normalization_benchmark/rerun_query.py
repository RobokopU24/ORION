import argparse
import json
import os
import time
import requests
from datetime import datetime

ENDPOINTS = {
    'redis': 'https://nodenormalization-sri.renci.org/',
    'es': 'https://biothings.ci.transltr.io/nodenorm/',
}


def run_query(endpoint_url, curies, conflate=True):
    """Run a single normalization query and return timing and result info."""
    if not endpoint_url.endswith('/'):
        endpoint_url += '/'

    call_start = time.time()
    status_code = None
    error_message = None
    success = False
    try:
        resp = requests.post(
            f'{endpoint_url}get_normalized_nodes',
            json={'curies': curies,
                  'conflate': conflate,
                  'drug_chemical_conflate': conflate,
                  'description': True},
        )
        status_code = resp.status_code
        if resp.status_code == 200:
            response_json = resp.json()
            if response_json:
                success = True
            else:
                error_message = '200 response with empty body'
        else:
            error_message = f'Non-200 response: {resp.status_code}'
    except Exception as e:
        error_message = str(e)

    elapsed = time.time() - call_start
    return {
        'num_curies': len(curies),
        'elapsed_seconds': round(elapsed, 4),
        'status_code': status_code,
        'success': success,
        'error': error_message,
        'curies': curies,
    }


def main():
    parser = argparse.ArgumentParser(description='Rerun queries from an api_call_log.json')
    parser.add_argument('api_call_log',
                        help='Path to an api_call_log.json file')
    parser.add_argument('--call-index', type=int, default=None,
                        help='Rerun a specific call by its call_index (default: rerun all)')
    parser.add_argument('--endpoint', choices=list(ENDPOINTS.keys()), default=None,
                        help='Override endpoint (default: use the same endpoint from the benchmark)')
    parser.add_argument('--endpoint-url', default=None,
                        help='Override endpoint with a custom URL')
    parser.add_argument('--output',
                        help='Path to write rerun results (default: rerun_<timestamp>.json next to input)')
    parser.add_argument('--conflate', action='store_true', default=True,
                        help='Enable conflation (default: True)')
    parser.add_argument('--no-conflate', action='store_false', dest='conflate',
                        help='Disable conflation')

    args = parser.parse_args()

    # load the original call log
    with open(args.api_call_log) as f:
        call_log = json.load(f)

    # select which calls to rerun
    if args.call_index is not None:
        calls_to_run = [c for c in call_log if c['call_index'] == args.call_index]
        if not calls_to_run:
            print(f'No call with call_index={args.call_index} found in {args.api_call_log}')
            return
    else:
        calls_to_run = call_log

    # determine endpoint
    if args.endpoint_url:
        endpoint_url = args.endpoint_url
        endpoint_name = 'custom'
    elif args.endpoint:
        endpoint_url = ENDPOINTS[args.endpoint]
        endpoint_name = args.endpoint
    else:
        # try to infer from the benchmark_results.json next to the call log
        results_path = os.path.join(os.path.dirname(args.api_call_log), 'benchmark_results.json')
        if os.path.isfile(results_path):
            with open(results_path) as f:
                benchmark_results = json.load(f)
            endpoint_url = benchmark_results.get('benchmark_params', {}).get('endpoint_url')
            endpoint_name = benchmark_results.get('benchmark_params', {}).get('endpoint_name')
            if endpoint_url:
                print(f'Using endpoint from benchmark_results.json: {endpoint_name} ({endpoint_url})')
            else:
                print('Could not determine endpoint from benchmark_results.json, defaulting to redis')
                endpoint_url = ENDPOINTS['redis']
                endpoint_name = 'redis'
        else:
            print('No benchmark_results.json found, defaulting to redis endpoint')
            endpoint_url = ENDPOINTS['redis']
            endpoint_name = 'redis'

    print(f'Endpoint: {endpoint_name} ({endpoint_url})')
    print(f'Calls to rerun: {len(calls_to_run)}')
    print(f'Conflation: {args.conflate}')
    print()

    rerun_results = []

    for original_call in calls_to_run:
        curies = original_call['curies']
        call_index = original_call['call_index']

        result = run_query(endpoint_url, curies, conflate=args.conflate)
        result['original_call_index'] = call_index
        result['original_elapsed_seconds'] = original_call['elapsed_seconds']
        result['original_success'] = original_call['success']
        rerun_results.append(result)

        status_str = 'OK' if result['success'] else f'FAILED (HTTP {result["status_code"]})'
        delta = result['elapsed_seconds'] - original_call['elapsed_seconds']
        delta_str = f'+{delta:.4f}s' if delta >= 0 else f'{delta:.4f}s'
        print(f'  Call {call_index}: {result["num_curies"]} curies in {result["elapsed_seconds"]:.4f}s '
              f'(was {original_call["elapsed_seconds"]:.4f}s, delta {delta_str}) [{status_str}]')

    # summary
    successful = [r for r in rerun_results if r['success']]
    failed = [r for r in rerun_results if not r['success']]
    print(f'\n--- Rerun Summary ---')
    print(f'Total calls:          {len(rerun_results)}')
    print(f'Successful:           {len(successful)}')
    print(f'Failed:               {len(failed)}')
    if successful:
        rerun_times = [r['elapsed_seconds'] for r in successful]
        original_times = [r['original_elapsed_seconds'] for r in successful]
        print(f'Total rerun time:     {sum(rerun_times):.4f}s')
        print(f'Total original time:  {sum(original_times):.4f}s')
        print(f'Mean rerun time:      {sum(rerun_times) / len(rerun_times):.4f}s')
        print(f'Mean original time:   {sum(original_times) / len(original_times):.4f}s')

    # write output
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        output_path = os.path.join(os.path.dirname(args.api_call_log),
                                   f'rerun_{timestamp}.json')

    output_data = {
        'endpoint_name': endpoint_name,
        'endpoint_url': endpoint_url,
        'source_log': os.path.abspath(args.api_call_log),
        'conflate': args.conflate,
        'timestamp': datetime.now().isoformat(),
        'calls': rerun_results,
    }
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    print(f'\nResults written to: {output_path}')


if __name__ == '__main__':
    main()
