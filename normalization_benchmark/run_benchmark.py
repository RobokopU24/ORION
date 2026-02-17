import argparse
import json
import os
import sys
import time
import random
import requests
from datetime import datetime

# add the project root to the path so we can import Common modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from orion.kgx_file_normalizer import KGXFileNormalizer
from orion.normalization import NormalizationScheme

ENDPOINTS = {
    'redis': 'https://nodenormalization-sri.renci.org/',
    'es': 'https://biothings.ci.transltr.io/nodenorm/',
}


def load_node_ids_from_map(normalization_map_path: str, num_nodes: int = None, seed: int = None):
    """Load node IDs from a normalization_map.json file.

    Returns a list of node IDs (the keys of the normalization_map dict).
    If num_nodes is specified, a random sample of that size is returned.
    """
    with open(normalization_map_path) as f:
        data = json.load(f)
    node_ids = list(data['normalization_map'].keys())
    if num_nodes is not None and num_nodes < len(node_ids):
        rng = random.Random(seed)
        node_ids = rng.sample(node_ids, num_nodes)
    return node_ids


def write_nodes_jsonl(node_ids: list, output_path: str):
    """Write a jsonl nodes file with one {"id": node_id} per line."""
    with open(output_path, 'w') as f:
        for node_id in node_ids:
            f.write(json.dumps({"id": node_id}) + '\n')


def run_benchmark(normalization_map_path: str,
                  output_dir: str,
                  endpoint_url: str,
                  endpoint_name: str = None,
                  num_nodes: int = None,
                  batch_size: int = 1000,
                  strict: bool = True,
                  conflation: bool = True,
                  seed: int = None):
    """Run the normalization benchmark against a specific endpoint.

    1. Read node IDs from the normalization map
    2. Write a jsonl nodes file
    3. Run normalize_node_file via KGXFileNormalizer
    4. Report timing and metadata
    """
    os.makedirs(output_dir, exist_ok=True)

    # file paths
    source_nodes_path = os.path.join(output_dir, 'source_nodes.jsonl')
    normalized_nodes_path = os.path.join(output_dir, 'normalized_nodes.jsonl')
    norm_map_path = os.path.join(output_dir, 'normalization_map.json')
    norm_failures_path = os.path.join(output_dir, 'normalization_failures.txt')
    # edge files are required by KGXFileNormalizer but we won't use them
    source_edges_path = os.path.join(output_dir, 'source_edges.jsonl')
    normalized_edges_path = os.path.join(output_dir, 'normalized_edges.jsonl')
    edge_predicate_map_path = os.path.join(output_dir, 'edge_predicate_map.json')

    # load and sample node IDs
    print(f'Loading node IDs from {normalization_map_path}...')
    node_ids = load_node_ids_from_map(normalization_map_path, num_nodes=num_nodes, seed=seed)
    print(f'Selected {len(node_ids)} nodes for benchmarking.')

    # write source nodes file
    write_nodes_jsonl(node_ids, source_nodes_path)
    print(f'Wrote source nodes to {source_nodes_path}')

    # write an empty edges file (required by KGXFileNormalizer constructor)
    with open(source_edges_path, 'w') as f:
        pass

    # patch the batch size on the NodeNormalizer after construction
    normalization_scheme = NormalizationScheme(strict=strict, conflation=conflation)

    print(f'Initializing KGXFileNormalizer (batch_size={batch_size}, strict={strict}, conflation={conflation})...')
    normalizer = KGXFileNormalizer(
        source_nodes_file_path=source_nodes_path,
        nodes_output_file_path=normalized_nodes_path,
        node_norm_map_file_path=norm_map_path,
        node_norm_failures_file_path=norm_failures_path,
        source_edges_file_path=source_edges_path,
        edges_output_file_path=normalized_edges_path,
        edge_norm_predicate_map_file_path=edge_predicate_map_path,
        normalization_scheme=normalization_scheme,
        preserve_unconnected_nodes=True,
    )

    # monkey-patch the batch_size into normalize_node_data
    original_normalize = normalizer.node_normalizer.normalize_node_data

    def patched_normalize(node_list, batch_size_arg=batch_size):
        return original_normalize(node_list, batch_size=batch_size_arg)

    normalizer.node_normalizer.normalize_node_data = patched_normalize

    # ensure the endpoint_url has a trailing slash
    if not endpoint_url.endswith('/'):
        endpoint_url += '/'

    # instrument hit_node_norm_service to record per-call timing and curies,
    # and direct requests to the specified endpoint
    api_call_log = []

    def timed_hit_service(curies, retries=0):
        call_start = time.time()
        status_code = None
        error_message = None
        result = None
        try:
            resp = normalizer.node_normalizer.requests_session.post(
                f'{endpoint_url}get_normalized_nodes',
                json={'curies': curies,
                      'conflate': normalizer.node_normalizer.conflate_node_types,
                      'drug_chemical_conflate': normalizer.node_normalizer.conflate_node_types,
                      'description': True,
                      'include_taxa': normalizer.node_normalizer.include_taxa},
            )
            status_code = resp.status_code
            if resp.status_code == 200:
                response_json = resp.json()
                if response_json:
                    result = response_json
                else:
                    error_message = f'200 response with empty body'
                    raise requests.exceptions.HTTPError(error_message)
            else:
                error_message = f'Non-200 response: {resp.status_code}'
                resp.raise_for_status()
        except Exception as e:
            if error_message is None:
                error_message = str(e)
        finally:
            call_elapsed = time.time() - call_start
            status_label = f'HTTP {status_code}' if status_code else 'connection error'
            success = result is not None
            api_call_log.append({
                'call_index': len(api_call_log),
                'num_curies': len(curies),
                'elapsed_seconds': round(call_elapsed, 4),
                'status_code': status_code,
                'success': success,
                'error': error_message,
                'curies': curies,
            })
            status_str = 'OK' if success else f'FAILED ({status_label})'
            print(f'  API call {len(api_call_log)}: {len(curies)} curies in {call_elapsed:.4f}s [{status_str}]')

        if result is None:
            # return None for each curie so the normalizer treats them as failed-to-normalize
            # rather than halting the entire benchmark
            return {curie: None for curie in curies}
        return result

    normalizer.node_normalizer.hit_node_norm_service = timed_hit_service

    # run and time the normalization
    print(f'Running node normalization...')
    start_time = time.time()
    normalizer.normalize_node_file()
    elapsed = time.time() - start_time

    # collect results
    metadata = normalizer.normalization_metadata
    metadata['benchmark_params'] = {
        'endpoint_name': endpoint_name,
        'endpoint_url': endpoint_url,
        'num_nodes': len(node_ids),
        'batch_size': batch_size,
        'strict': strict,
        'conflation': conflation,
        'seed': seed,
    }
    metadata['elapsed_seconds'] = round(elapsed, 3)
    successful_calls = [c for c in api_call_log if c['success']]
    failed_calls = [c for c in api_call_log if not c['success']]
    metadata['api_calls_summary'] = {
        'total_calls': len(api_call_log),
        'successful_calls': len(successful_calls),
        'failed_calls': len(failed_calls),
        'total_api_seconds': round(sum(c['elapsed_seconds'] for c in api_call_log), 4),
        'mean_call_seconds': round(sum(c['elapsed_seconds'] for c in successful_calls) / len(successful_calls), 4) if successful_calls else 0,
        'min_call_seconds': min(c['elapsed_seconds'] for c in successful_calls) if successful_calls else 0,
        'max_call_seconds': max(c['elapsed_seconds'] for c in successful_calls) if successful_calls else 0,
    }
    if failed_calls:
        # summarize failures by status code
        failure_status_codes = {}
        for c in failed_calls:
            key = str(c['status_code']) if c['status_code'] else 'connection_error'
            failure_status_codes[key] = failure_status_codes.get(key, 0) + 1
        metadata['api_calls_summary']['failure_status_codes'] = failure_status_codes

    results_path = os.path.join(output_dir, 'benchmark_results.json')
    with open(results_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    # write detailed per-call log
    api_log_path = os.path.join(output_dir, 'api_call_log.json')
    with open(api_log_path, 'w') as f:
        json.dump(api_call_log, f, indent=2)

    print(f'\n--- Benchmark Results ({endpoint_name or endpoint_url}) ---')
    print(f'Endpoint:             {endpoint_url}')
    print(f'Nodes input:          {metadata.get("regular_nodes_pre_norm", "N/A")}')
    print(f'Nodes post-norm:      {metadata.get("regular_nodes_post_norm", "N/A")}')
    print(f'Norm failures:        {metadata.get("regular_node_norm_failures", "N/A")}')
    print(f'Merged nodes:         {metadata.get("merged_nodes_post_norm", "N/A")}')
    print(f'Final nodes:          {metadata.get("final_normalized_nodes", "N/A")}')
    print(f'Elapsed time:         {elapsed:.3f}s')
    api_summary = metadata['api_calls_summary']
    print(f'\n--- API Call Stats ---')
    print(f'Total API calls:      {api_summary["total_calls"]}')
    print(f'Successful calls:     {api_summary["successful_calls"]}')
    print(f'Failed calls:         {api_summary["failed_calls"]}')
    if 'failure_status_codes' in api_summary:
        print(f'Failure breakdown:    {api_summary["failure_status_codes"]}')
    print(f'Total API time:       {api_summary["total_api_seconds"]:.4f}s')
    print(f'Mean call time:       {api_summary["mean_call_seconds"]:.4f}s')
    print(f'Min call time:        {api_summary["min_call_seconds"]:.4f}s')
    print(f'Max call time:        {api_summary["max_call_seconds"]:.4f}s')
    print(f'\nResults written to:   {results_path}')
    print(f'API call log:         {api_log_path}')

    return metadata


def load_normalized_nodes(nodes_file_path: str) -> dict:
    """Load normalized nodes from a jsonl file into a dict keyed by node id."""
    nodes = {}
    with open(nodes_file_path) as f:
        for line in f:
            line = line.strip()
            if line:
                node = json.loads(line)
                nodes[node['id']] = node
    return nodes


def compare_endpoint_outputs(base_output_dir: str, endpoint_names: list):
    """Compare normalized_nodes.jsonl across endpoints and record differences.

    For each pair of endpoints, finds:
    - nodes present in one but not the other
    - nodes present in both but with different content
    """
    print(f'\n{"=" * 60}')
    print(f'  Comparing endpoint outputs')
    print(f'{"=" * 60}\n')

    # load all endpoint nodes
    endpoint_nodes = {}
    for name in endpoint_names:
        nodes_path = os.path.join(base_output_dir, name, 'normalized_nodes.jsonl')
        if os.path.isfile(nodes_path):
            endpoint_nodes[name] = load_normalized_nodes(nodes_path)
            print(f'Loaded {len(endpoint_nodes[name])} nodes from {name}')
        else:
            print(f'WARNING: No normalized_nodes.jsonl found for {name}, skipping')

    if len(endpoint_nodes) < 2:
        print('Need at least 2 endpoint outputs to compare, skipping comparison.')
        return

    comparison_results = {}
    names = list(endpoint_nodes.keys())

    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            name_a = names[i]
            name_b = names[j]
            nodes_a = endpoint_nodes[name_a]
            nodes_b = endpoint_nodes[name_b]

            ids_a = set(nodes_a.keys())
            ids_b = set(nodes_b.keys())

            only_in_a = ids_a - ids_b
            only_in_b = ids_b - ids_a
            common_ids = ids_a & ids_b

            # find nodes that exist in both but differ, recording only the differing fields
            differing_nodes = {}
            for node_id in common_ids:
                node_a = nodes_a[node_id]
                node_b = nodes_b[node_id]
                if node_a != node_b:
                    all_keys = set(node_a.keys()) | set(node_b.keys())
                    diff = {}
                    for key in sorted(all_keys):
                        val_a = node_a.get(key)
                        val_b = node_b.get(key)
                        if val_a != val_b:
                            diff[key] = {name_a: val_a, name_b: val_b}
                    differing_nodes[node_id] = diff

            pair_key = f'{name_a}_vs_{name_b}'
            comparison_results[pair_key] = {
                'summary': {
                    f'{name_a}_total_nodes': len(ids_a),
                    f'{name_b}_total_nodes': len(ids_b),
                    f'only_in_{name_a}': len(only_in_a),
                    f'only_in_{name_b}': len(only_in_b),
                    'common_nodes': len(common_ids),
                    'differing_nodes': len(differing_nodes),
                    'identical_nodes': len(common_ids) - len(differing_nodes),
                },
                f'only_in_{name_a}': sorted(only_in_a),
                f'only_in_{name_b}': sorted(only_in_b),
                'differing_nodes': differing_nodes,
            }

            summary = comparison_results[pair_key]['summary']
            print(f'\n--- {name_a} vs {name_b} ---')
            print(f'  {name_a} nodes:    {summary[f"{name_a}_total_nodes"]}')
            print(f'  {name_b} nodes:    {summary[f"{name_b}_total_nodes"]}')
            print(f'  Only in {name_a}:  {summary[f"only_in_{name_a}"]}')
            print(f'  Only in {name_b}:  {summary[f"only_in_{name_b}"]}')
            print(f'  Common nodes:      {summary["common_nodes"]}')
            print(f'  Identical:         {summary["identical_nodes"]}')
            print(f'  Differing:         {summary["differing_nodes"]}')

    comparison_path = os.path.join(base_output_dir, 'endpoint_comparison.json')
    with open(comparison_path, 'w') as f:
        json.dump(comparison_results, f, indent=2)
    print(f'\nComparison written to: {comparison_path}')

    return comparison_results


def cleanup_output_files(base_output_dir: str, endpoint_names: list):
    """Remove bulky intermediate files from each endpoint output directory."""
    files_to_remove = ['normalized_nodes.jsonl', 'source_nodes.jsonl', 'source_edges.jsonl',
                       'normalized_edges.jsonl']
    for name in endpoint_names:
        for filename in files_to_remove:
            filepath = os.path.join(base_output_dir, name, filename)
            if os.path.isfile(filepath):
                os.remove(filepath)
                print(f'Removed {filepath}')


def make_run_dir_name(num_nodes, batch_size):
    """Build a run directory name like: 10000_nodes_batch_1000_2026-02-17"""
    date_str = datetime.now().strftime('%Y-%m-%d')
    nodes_label = f'nodes_{num_nodes}' if num_nodes else 'all_nodes'
    return f'{nodes_label}_batch_{batch_size}__{date_str}'


def get_total_node_count(normalization_map_path: str):
    """Get the total number of nodes in the normalization map without loading them all."""
    with open(normalization_map_path) as f:
        data = json.load(f)
    return len(data['normalization_map'])


def main():
    parser = argparse.ArgumentParser(description='Benchmark node normalization')
    parser.add_argument('--normalization-map',
                        default=os.path.join(os.path.dirname(__file__), 'normalization_map.json'),
                        help='Path to normalization_map.json (default: normalization_benchmark/normalization_map.json)')
    parser.add_argument('--output-dir',
                        default=os.path.join(os.path.dirname(__file__), 'output'),
                        help='Base directory for benchmark output files (default: normalization_benchmark/output/)')
    parser.add_argument('--num-nodes', type=int, default=None,
                        help='Number of nodes to sample from the map (default: all)')
    parser.add_argument('--batch-size', type=int, default=100000,
                        help='Batch size for node normalization API calls (default: 100000)')
    parser.add_argument('--strict', action='store_true', default=True,
                        help='Use strict normalization (default: True)')
    parser.add_argument('--no-strict', action='store_false', dest='strict',
                        help='Disable strict normalization')
    parser.add_argument('--conflation', action='store_true', default=True,
                        help='Enable conflation (default: True)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for node sampling')

    endpoint_group = parser.add_mutually_exclusive_group()
    endpoint_group.add_argument('--endpoint', choices=list(ENDPOINTS.keys()),
                                help=f'Run benchmark against a named endpoint: {list(ENDPOINTS.keys())}')
    endpoint_group.add_argument('--endpoint-url',
                                help='Run benchmark against a custom endpoint URL')
    endpoint_group.add_argument('--all-endpoints', action='store_true',
                                help='Run benchmark against all named endpoints sequentially')

    args = parser.parse_args()

    # determine the actual node count for the directory name
    actual_num_nodes = args.num_nodes
    if actual_num_nodes is None:
        actual_num_nodes = get_total_node_count(args.normalization_map)

    # build the run-specific output directory
    run_dir_name = make_run_dir_name(actual_num_nodes, args.batch_size)
    run_output_dir = os.path.join(args.output_dir, run_dir_name)
    print(f'Output directory: {run_output_dir}')

    if args.all_endpoints:
        all_results = {}
        for name, url in ENDPOINTS.items():
            endpoint_output_dir = os.path.join(run_output_dir, name)
            print(f'\n{"=" * 60}')
            print(f'  Benchmarking endpoint: {name} ({url})')
            print(f'{"=" * 60}\n')
            try:
                result = run_benchmark(
                    normalization_map_path=args.normalization_map,
                    output_dir=endpoint_output_dir,
                    endpoint_url=url,
                    endpoint_name=name,
                    num_nodes=args.num_nodes,
                    batch_size=args.batch_size,
                    strict=args.strict,
                    conflation=args.conflation,
                    seed=args.seed,
                )
                all_results[name] = result
            except Exception as e:
                print(f'\n  ENDPOINT {name} FAILED: {e}\n')
                all_results[name] = {'error': str(e)}

        # write a combined summary
        summary_path = os.path.join(run_output_dir, 'all_endpoints_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f'\n{"=" * 60}')
        print(f'  All endpoints summary written to: {summary_path}')
        print(f'{"=" * 60}')

        # print a comparison table
        print(f'\n--- Endpoint Comparison ---')
        print(f'{"Endpoint":<12} {"Time (s)":<12} {"API Time (s)":<14} {"Calls":<8} {"Failed":<8} {"Nodes Out":<12}')
        print('-' * 66)
        for name, result in all_results.items():
            if 'error' in result:
                print(f'{name:<12} {"FAILED":<12} {result["error"][:40]}')
            else:
                api_summary = result.get('api_calls_summary', {})
                print(f'{name:<12} '
                      f'{result.get("elapsed_seconds", "N/A"):<12} '
                      f'{api_summary.get("total_api_seconds", "N/A"):<14} '
                      f'{api_summary.get("total_calls", "N/A"):<8} '
                      f'{api_summary.get("failed_calls", "N/A"):<8} '
                      f'{result.get("final_normalized_nodes", "N/A"):<12}')

        # compare normalized outputs across endpoints and clean up
        successful_endpoints = [name for name, result in all_results.items() if 'error' not in result]
        compare_endpoint_outputs(run_output_dir, successful_endpoints)
        # cleanup_output_files(run_output_dir, list(ENDPOINTS.keys()))
    else:
        # single endpoint mode
        if args.endpoint:
            endpoint_name = args.endpoint
            endpoint_url = ENDPOINTS[args.endpoint]
        elif args.endpoint_url:
            endpoint_name = 'custom'
            endpoint_url = args.endpoint_url
        else:
            # default to redis
            endpoint_name = 'redis'
            endpoint_url = ENDPOINTS['redis']

        endpoint_output_dir = os.path.join(run_output_dir, endpoint_name)

        run_benchmark(
            normalization_map_path=args.normalization_map,
            output_dir=endpoint_output_dir,
            endpoint_url=endpoint_url,
            endpoint_name=endpoint_name,
            num_nodes=args.num_nodes,
            batch_size=args.batch_size,
            strict=args.strict,
            conflation=args.conflation,
            seed=args.seed,
        )


if __name__ == '__main__':
    main()