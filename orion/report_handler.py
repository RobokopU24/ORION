import os
import datetime
import requests

from orion.logging import get_orion_logger
from orion.config import config

logger = get_orion_logger("orion.report_handler")

SOURCE_STATUS_LABEL = {
    'built': 'built',
    'already_built': 'cached',
    'failed': 'FAILED',
}


class ReportHandler:
    """Handles generation and delivery of build reports."""

    def __init__(self, outcomes: list):
        self.outcomes = outcomes  # list of per-graph outcome dicts

    def write_report(self) -> str:
        """Write report to ORION_LOGS if set, otherwise /tmp/orion_logs. Returns report path."""
        logs_dir = config.ORION_LOGS or '/tmp/orion_logs'
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(logs_dir, f'build-report-{timestamp}.txt')

        with open(report_path, 'w') as f:
            f.write('ORION Build Report\n')
            f.write(f'Generated: {datetime.datetime.now().isoformat()}\n')
            f.write('=' * 80 + '\n\n')

            for graph in self.outcomes:
                graph_id = graph['graph_id']
                version = graph['version']
                status = graph['status']
                reason = graph.get('reason')
                sources = graph.get('sources', [])

                f.write(f'Graph: {graph_id}\n')
                f.write(f'  Version: {version}\n')
                f.write(f'  Status:  {status}')
                if reason:
                    f.write(f' — {reason}')
                f.write('\n')

                if sources:
                    f.write('  Sources:\n')
                    for s in sources:
                        label = SOURCE_STATUS_LABEL.get(s['status'], s['status'])
                        f.write(f'    - {s["source_id"]} ({label})')
                        if s.get('version'):
                            f.write(f'  v:{s["version"]}')
                        f.write('\n')
                f.write('\n')

            f.write('=' * 80 + '\n')
            total = len(self.outcomes)
            built = sum(1 for g in self.outcomes if g['status'] == 'built')
            already_built = sum(1 for g in self.outcomes if g['status'] == 'already_built')
            failed = sum(1 for g in self.outcomes if g['status'] == 'failed')
            skipped = sum(1 for g in self.outcomes if g['status'] == 'skipped')
            f.write(f'Summary: {total} graphs — built: {built}  already built: {already_built}  '
                    f'failed: {failed}  skipped: {skipped}\n')

        logger.info(f'Build report written to {report_path}')
        return report_path

    def send_slack_notification(self, report_path: str) -> bool:
        """Post build summary to Slack. Returns True on success, False otherwise."""
        webhook_url = os.environ.get('ORION_SLACK_WEBHOOK_URL', '')
        if not webhook_url:
            logger.info('ORION_SLACK_WEBHOOK_URL not configured, skipping Slack notification.')
            return False

        try:
            any_failed = any(
                g['status'] == 'failed' or any(s['status'] == 'failed' for s in g.get('sources', []))
                for g in self.outcomes
            )
            status_emoji = ':white_check_mark:' if not any_failed else ':x:'
            lines = [
                f'{status_emoji} *ORION Build Report* — {datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")}',
            ]

            for graph in self.outcomes:
                graph_id = graph['graph_id']
                version = graph['version']
                status = graph['status']
                reason = graph.get('reason')
                sources = graph.get('sources', [])

                if status == 'built':
                    g_mark = ':large_green_circle:'
                elif status == 'already_built':
                    g_mark = ':white_circle:'
                elif status == 'failed':
                    g_mark = ':red_circle:'
                else:
                    g_mark = ':yellow_circle:'

                version_str = f' `{version}`' if version else ''
                reason_str = f' — {reason}' if reason else ''
                lines.append(f'{g_mark} *{graph_id}*{version_str} — {status}{reason_str}')

                if sources:
                    source_parts = []
                    for s in sources:
                        label = SOURCE_STATUS_LABEL.get(s['status'], s['status'])
                        mark = ':small_red_triangle:' if s['status'] == 'failed' else ''
                        source_parts.append(f'{mark}{s["source_id"]} ({label})')
                    lines.append(f'    _Sources: {" | ".join(source_parts)}_')

            if report_path:
                lines.append(f'Report: `{report_path}`')

            payload = {'text': '\n'.join(lines)}
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info('Slack build notification sent.')
            return True
        except Exception as e:
            logger.error(f'Failed to send Slack notification: {e}')
            return False
