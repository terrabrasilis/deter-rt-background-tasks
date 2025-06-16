import logging
from typing import Any
import sys


class TasksLogger:
    logger = None
    logPrefix = ""

    def __init__(self, logPrefix=""):
        self.logger = self.get_logger(__name__, sys.stdout)
        self.logPrefix = logPrefix + " - "

    def get_handler(self, textio: Any):
        handler = logging.StreamHandler(textio)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        return handler

    def get_logger(self, name: str, textio: Any):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self.get_handler(textio=textio))
        return logger

    def debug(self, msg):
        self.logger.debug(self.logPrefix + msg)

    def info(self, msg):
        self.logger.info(self.logPrefix + msg)

    def error(self, msg):
        self.logger.error(self.logPrefix + msg)

    def warning(self, msg):
        self.logger.warning(self.logPrefix + msg)

    def log(self, msg: str, level: int = 0):
        self.logger.log(level, self.logPrefix + msg)

    # Defining logging level 'CRITICAL', 'FATAL',  'ERROR', 'WARN', 'WARNING', 'INFO' or 'DEBUG'
    def setLoggerLevel(self, level: str):
        if level:
            self.logger.setLevel(level)
        else:
            self.logger.setLevel("NOTSET")

    def getLoggerLevel(self):
        return self.logger.level
