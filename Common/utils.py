import os
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

class LoggingUtil(object):
    """
    creates and configures a logger
    """
    @staticmethod
    def init_logging (name, level=logging.INFO, format='short', logFilePath=None, logFileLevel=None):
        """
            Logging utility controlling format and setting initial logging level
        """
        # get a new logger
        logger = logging.getLogger(__name__)

        # is this the root
        if not logger.parent.name == 'root':
            return logger

        # define the various output formats
        FORMAT = {
            "short" : '%(funcName)s: %(message)s',
            "medium" : '%(funcName)s: %(asctime)-15s %(message)s',
            "long"  : '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        }[format]

        # create a stream handler (default to console)
        stream_handler = logging.StreamHandler()

        # create a formatter
        formatter = logging.Formatter(FORMAT)

        # set the formatter on the console stream
        stream_handler.setFormatter(formatter)

        # get the name of this logger
        logger = logging.getLogger(name)

        # set the logging level
        logger.setLevel(level)

        # if there was a file path passed in use it
        if logFilePath is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=os.path.join(logFilePath, name + '.log'), maxBytes=1000000, backupCount=10)

            # set the formatter
            file_handler.setFormatter(formatter)

            # if a log level for the file was passed in use it
            if logFileLevel is not None:
                level = logFileLevel

            # set the log level
            file_handler.setLevel(level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # add the console handler to the logger
        logger.addHandler(stream_handler)

        # return to the caller
        return logger

    @staticmethod
    def print_debug_msg(msg: str):
        """
        Adds a timestamp to a printed message

        :param msg: the message that gets appended onto a timestamp and output to console
        :return: None
        """

        # get the timestamp
        now: datetime = datetime.now()

        # output the text
        print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')
