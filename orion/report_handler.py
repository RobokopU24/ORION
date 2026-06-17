import os
import datetime
import requests

from orion.logging import get_orion_logger
from orion.config import config

logger = get_orion_logger("orion.report_handler")


class ReportHandler:
    """Handles generation and delivery of build reports."""

    def __init__(self, outcomes: dict):
        self.outcomes = outcomes

    def write_report(self) -> str:
        """Write report to ORION_LOGS directory. Returns report path or None."""
        logs_dir = config.ORION_LOGS
        if not logs_dir:
            logger.warning('ORION_LOGS not configured, skipping report generation.')
            return None

        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(logs_dir, f'build-report-{timestamp}.txt')

        with open(report_path, 'w') as f:
            f.write(f'ORION Build Report\n')
            f.write(f'Generated: {datetime.datetime.now().isoformat()}\n')
            f.write('='*80 + '\n\n')

            f.write(f'Built ({len(self.outcomes["built"])} graphs):\n')
            for item in self.outcomes['built']:
                f.write(f'  - {item["graph_id"]} (version: {item["version"]})\n')
            f.write('\n')

            f.write(f'Already Built ({len(self.outcomes["already_built"])} graphs):\n')
            for item in self.outcomes['already_built']:
                f.write(f'  - {item["graph_id"]} (version: {item["version"]})\n')
            f.write('\n')

            f.write(f'Failed ({len(self.outcomes["failed"])} graphs):\n')
            for item in self.outcomes['failed']:
                reason = item.get('reason', 'Unknown error')
                f.write(f'  - {item["graph_id"]} (version: {item["version"]})\n')
                f.write(f'    Reason: {reason}\n')
            f.write('\n')

            f.write(f'Skipped ({len(self.outcomes["skipped"])} graphs):\n')
            for item in self.outcomes['skipped']:
                reason = item.get('reason', 'Unknown reason')
                f.write(f'  - {item["graph_id"]} (version: {item["version"]})\n')
                f.write(f'    Reason: {reason}\n')
            f.write('\n')

            f.write('='*80 + '\n')
            total = sum(len(v) for v in self.outcomes.values())
            f.write(f'Summary: {total} graphs processed\n')
            f.write(f'  Built: {len(self.outcomes["built"])}\n')
            f.write(f'  Already Built: {len(self.outcomes["already_built"])}\n')
            f.write(f'  Failed: {len(self.outcomes["failed"])}\n')
            f.write(f'  Skipped: {len(self.outcomes["skipped"])}\n')

        logger.info(f'Build report written to {report_path}')
        return report_path

    def send_slack_notification(self, report_path: str) -> bool:
        """Post build summary to Slack. Returns True on success, False otherwise."""
        webhook_url = os.environ.get('ORION_SLACK_WEBHOOK_URL', '')
        if not webhook_url:
            logger.info('ORION_SLACK_WEBHOOK_URL not configured, skipping Slack notification.')
            return False

        try:
            built = self.outcomes['built']
            already_built = self.outcomes['already_built']
            failed = self.outcomes['failed']
            skipped = self.outcomes['skipped']

            status_emoji = ':white_check_mark:' if not failed else ':x:'
            lines = [
                f'{status_emoji} *ORION Build Report* — {datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")}',
                f'Built: {len(built)}   Already built: {len(already_built)}   Failed: {len(failed)}   Skipped: {len(skipped)}',
            ]
            if failed:
                lines.append('*Failed graphs:*')
                for item in failed:
                    lines.append(f'  • `{item["graph_id"]}` — {item.get("reason", "unknown error")}')
            if report_path:
                lines.append(f'Report written to `{report_path}`')

            payload = {'text': '\n'.join(lines)}
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info('Slack build notification sent.')
            return True
        except Exception as e:
            logger.error(f'Failed to send Slack notification: {e}')
            return False
