import os
import logging
from logging.handlers import RotatingFileHandler

from orion.config import config


ORION_LOG_FORMAT = '[%(asctime)s] %(name)s %(levelname)s: %(message)s'


def get_orion_logger(name):
    """Return a logger for ORION library code.

    Follows the standard library convention (as used by requests, urllib3, etc.):
    attaches a NullHandler so importing ORION never produces output or
    "no handlers could be found" warnings. Applications — including ORION's own
    CLI entry points via configure_cli_logging() — can apply handler/level
    configuration which will pick up the logs through propagation.
    """
    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.NullHandler) for h in logger.handlers):
        logger.addHandler(logging.NullHandler())
    return logger


def configure_cli_logging(level=None):
    """Configure stderr (and optional file) logging on the root logger.

    Intended to be called from ORION CLI entry points (scripts run as __main__).
    Handlers are attached to the root logger so that every logger tree used by
    ORION and its parsers (orion.*, parsers.*, etc.) is captured via normal
    propagation. Safe to call more than once; handlers are added only if absent,
    and the level is refreshed on each call.
    """
    root_logger = logging.getLogger()
    if level is None:
        level = logging.DEBUG if config.ORION_TEST_MODE else logging.INFO
    root_logger.setLevel(level)

    formatter = logging.Formatter(ORION_LOG_FORMAT)

    # type() check (not isinstance): FileHandler and RotatingFileHandler are
    # subclasses of StreamHandler, but we want to treat console vs file as
    # distinct cases here.
    has_stream = any(type(h) is logging.StreamHandler for h in root_logger.handlers)
    if not has_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    if config.ORION_LOGS is not None:
        has_file = any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers)
        if not has_file:
            file_handler = RotatingFileHandler(
                filename=os.path.join(config.ORION_LOGS, 'orion.log'),
                maxBytes=100_000_000,
                backupCount=10,
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)