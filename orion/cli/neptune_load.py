import argparse
import sys

from orion.config import config
from orion.neptune_loader import (DEFAULT_PARALLELISM, NeptuneLoadError, load_graph_into_neptune)


def main():
    from orion.logging import configure_cli_logging
    configure_cli_logging()

    ap = argparse.ArgumentParser(
        description='Upload a directory of Neptune CSV files to S3 and bulk load it into a Neptune '
                    'cluster. Create the files first with orion-neptune-dump.')
    ap.add_argument('csv_directory',
                    help='Directory holding the CSV files and the neptune_load_manifest.json that '
                         'orion-neptune-dump wrote.')
    ap.add_argument('--s3_uri', default=config.NEPTUNE_S3_URI,
                    help='S3 uri to upload the files to and load them from, e.g. '
                         's3://my-bucket/graphs/RobokopKG/1.0.2. Defaults to NEPTUNE_S3_URI.')
    ap.add_argument('--neptune_host', default=config.NEPTUNE_HOST,
                    help='Hostname of the Neptune cluster writer endpoint. Defaults to NEPTUNE_HOST.')
    ap.add_argument('--iam_role_arn', default=config.NEPTUNE_IAM_ROLE_ARN,
                    help='ARN of an IAM role attached to the cluster that can read the bucket. '
                         'Defaults to NEPTUNE_IAM_ROLE_ARN.')
    ap.add_argument('--region', default=config.NEPTUNE_REGION,
                    help='AWS region of the cluster and the bucket. Defaults to NEPTUNE_REGION.')
    ap.add_argument('--parallelism', default=DEFAULT_PARALLELISM,
                    choices=['LOW', 'MEDIUM', 'HIGH', 'OVERSUBSCRIBE'],
                    help=f'Loader thread count setting (default {DEFAULT_PARALLELISM}). Neptune '
                         f'documents HIGH as a cause of deadlocks on openCypher loads.')
    ap.add_argument('--continue_on_error', action='store_true',
                    help='Load everything that can be loaded instead of stopping at the first error.')
    ap.add_argument('--skip_upload', action='store_true',
                    help='Load from the S3 uri without uploading, for retrying a load whose files '
                         'are already in place.')
    ap.add_argument('--no_wait', action='store_true',
                    help='Start the load and exit instead of polling it to completion.')

    args = ap.parse_args()

    missing_arguments = [name for name, value in (('--s3_uri', args.s3_uri),
                                                  ('--neptune_host', args.neptune_host),
                                                  ('--iam_role_arn', args.iam_role_arn),
                                                  ('--region', args.region)) if not value]
    if missing_arguments:
        ap.error(f'missing required arguments (no configured default): {", ".join(missing_arguments)}')

    try:
        load_graph_into_neptune(csv_directory=args.csv_directory,
                                s3_uri=args.s3_uri,
                                neptune_host=args.neptune_host,
                                iam_role_arn=args.iam_role_arn,
                                region=args.region,
                                fail_on_error=not args.continue_on_error,
                                parallelism=args.parallelism,
                                skip_upload=args.skip_upload,
                                wait=not args.no_wait)
    except (NeptuneLoadError, FileNotFoundError) as e:
        print(f'Neptune load failed: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()