import json
import requests

from orion.config import config
from orion.logging import get_orion_logger

logger = get_orion_logger(__name__)

_STATUS_EMOJI = {
    'stable': ':large_green_circle:',
    'failed': ':red_circle:',
}

_SOURCE_LABEL = {
    'built': 'built',
    'cached': 'cached',
    'failed': 'FAILED',
}


def send_slack_notification(results_path: str) -> None:
    try:
        with open(results_path) as f:
            results = json.load(f)
        message = _format_message(results)
        resp = requests.post(config.ORION_SLACK_WEBHOOK_URL, json={'text': message}, timeout=10)
        resp.raise_for_status()
        logger.info('Slack notification sent.')
    except Exception as e:
        logger.warning(f'Failed to send Slack notification: {e}')


def _format_message(results: list) -> str:
    any_failed = any(
        r.get('build_status') == 'failed' or
        any(s.get('status') == 'failed' for s in r.get('sources', {}).values())
        for r in results
    )
    header = ':red_circle: *ORION build completed with errors*' if any_failed \
        else ':large_green_circle: *ORION build completed*'

    lines = [header, '']
    for graph in results:
        graph_id = graph.get('graph_id', '?')
        status = graph.get('build_status', 'failed')
        emoji = _STATUS_EMOJI.get(status, ':white_circle:')

        if status == 'stable':
            version = graph.get('release_version', '?')
            build_v = (graph.get('build_version') or '')[:8]
            lines.append(f'{emoji} *{graph_id}* `{build_v}` — built as v{version}')
        else:
            reason = graph.get('reason', '')
            lines.append(f'{emoji} *{graph_id}* — failed ({reason})')

        sources = graph.get('sources', {})
        if sources:
            parts = []
            for src_id, src in sources.items():
                src_status = src.get('status', 'failed')
                label = _SOURCE_LABEL.get(src_status, src_status)
                version = src.get('release_version') or src.get('build_version') or ''
                version_str = f' {version}' if version else ''
                if src_status == 'failed':
                    error = src.get('error') or ''
                    short_error = error[:80] + '…' if len(error) > 80 else error
                    parts.append(f'`{src_id}`: {label} — {short_error}' if short_error else f'`{src_id}`: {label}')
                else:
                    parts.append(f'`{src_id}`:{version_str} ({label})')
            lines.append('    ' + ' | '.join(parts))

        lines.append('')

    return '\n'.join(lines).strip()
