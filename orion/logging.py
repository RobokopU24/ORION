import os
import logging
from logging.handlers import RotatingFileHandler

from orion.config import config


def get_orion_logger(name):
    """
        Logging utility controlling format and setting initial logging level
    """

    # get the logger with the specified name
    logger = logging.getLogger(name)

    # if it already has handlers, it was already instantiated - return it
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter('%(asctime)-15s - %(funcName)s(): %(message)s')

    level = logging.DEBUG if config.ORION_TEST_MODE else logging.INFO
    logger.setLevel(level)

    # if ORION_LOGS is set, write logs to files there
    if config.ORION_LOGS is not None:
        # create a rotating file handler, 100mb max per file with a max number of 10 files
        file_handler = RotatingFileHandler(filename=os.path.join(config.ORION_LOGS, name + '.log'), maxBytes=100000000, backupCount=10)

        # set the formatter
        file_handler.setFormatter(formatter)

        # set the log level
        file_handler.setLevel(level)

        # add the handler to the logger
        logger.addHandler(file_handler)

    # create a stream handler as well (default to console/stdout)
    stream_handler = logging.StreamHandler()

    # set the formatter on the console stream
    stream_handler.setFormatter(formatter)

    # add the console handler to the logger
    logger.addHandler(stream_handler)

    # return to the caller
    return logger