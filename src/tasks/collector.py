from abc import ABC, abstractmethod
from utils.logger import TasksLogger


# Defining the Abstract Collector
class Collector(ABC):
    """Abstract Collector: Represents a reader data from different sources."""

    @abstractmethod
    def __init__(self, log_level: str):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(log_level)

    @abstractmethod
    def read_data(self):
        """Read data"""
        pass
