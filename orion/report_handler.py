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

        sources = self.outcomes.get('sources', {})
        with open(report_path, 'w') as f:
            f.write(f'ORION Build Report\n')
            f.write(f'Generated: {datetime.datetime.now().isoformat()}\n')
            f.write('='*80 + '\n\n')

            f.write('Sources\n')
            f.write(f'  Built ({len(sources.get("built", []))} sources):\n')
            for item in sources.get('built', []):
                f.write(f'    - {item["source_id"]} (version: {item.get("version", "?")})\n')
            f.write(f'  Already Built ({len(sources.get("already_built", []))} sources):\n')
            for item in sources.get('already_built', []):
                f.write(f'    - {item["source_id"]} (version: {item.get("version", "?")})\n')
            f.write(f'  Failed ({len(sources.get("failed", []))} sources):\n')
            for item in sources.get('failed', []):
                f.write(f'    - {item["source_id"]}\n')
            f.write('\n')

            f.write(f'Graphs\n')
            f.write(f'  Built ({len(self.outcomes["built"])} graphs):\n')
            for item in self.outcomes['built']:
                f.write(f'    - {item["graph_id"]} (version: {item["version"]})\n')
            f.write(f'  Already Built ({len(self.outcomes["already_built"])} graphs):\n')
            for item in self.outcomes['already_built']:
                f.write(f'    - {item["graph_id"]} (version: {item["version"]})\n')
            f.write(f'  Failed ({len(self.outcomes["failed"])} graphs):\n')
            for item in self.outcomes['failed']:
                reason = item.get('reason', 'Unknown error')
                f.write(f'    - {item["graph_id"]} (version: {item["version"]})\n')
                f.write(f'      Reason: {reason}\n')
            f.write(f'  Skipped ({len(self.outcomes["skipped"])} graphs):\n')
            for item in self.outcomes['skipped']:
                reason = item.get('reason', 'Unknown reason')
                f.write(f'    - {item["graph_id"]} (version: {item["version"]})\n')
                f.write(f'      Reason: {reason}\n')
            f.write('\n')

            f.write('='*80 + '\n')
            total_graphs = (len(self.outcomes['built']) + len(self.outcomes['already_built']) +
                            len(self.outcomes['failed']) + len(self.outcomes['skipped']))
            f.write(f'Summary: {total_graphs} graphs processed\n')
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
            sources = self.outcomes.get('sources', {})
            sources_built = sources.get('built', [])
            sources_already_built = sources.get('already_built', [])
            sources_failed = sources.get('failed', [])

            any_failed = failed or sources_failed
            status_emoji = ':white_check_mark:' if not any_failed else ':x:'
            lines = [
                f'{status_emoji} *ORION Build Report* — {datetime.datetime.now().strftime("%Y-%m-%d %H:%M UTC")}',
            ]

            if sources_built or sources_already_built or sources_failed:
                source_parts = []
                if sources_built:
                    source_parts.append(f'built: {", ".join(s["source_id"] for s in sources_built)}')
                if sources_already_built:
                    source_parts.append(f'cached: {", ".join(s["source_id"] for s in sources_already_built)}')
                if sources_failed:
                    source_parts.append(f'failed: {", ".join(s["source_id"] for s in sources_failed)}')
                lines.append(f'*Sources:* {" | ".join(source_parts)}')

            lines.append(
                f'*Graphs:* built: {len(built)}   already built: {len(already_built)}   '
                f'failed: {len(failed)}   skipped: {len(skipped)}'
            )
            for item in built:
                lines.append(f'  • `{item["graph_id"]}` — new version `{item["version"]}`')
            for item in already_built:
                lines.append(f'  • `{item["graph_id"]}` — stable at `{item["version"]}`')
            if failed:
                lines.append('*Failed:*')
                for item in failed:
                    lines.append(f'  • `{item["graph_id"]}` — {item.get("reason", "unknown error")}')
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
