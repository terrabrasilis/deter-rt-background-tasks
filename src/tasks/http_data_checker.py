from tasks.http_data_source import HTTPDataSource
from utils.logger import TasksLogger


class HTTPDataChecker:
    """HTTP DataChecker: Represents a checker data for a HTTP sources."""

    def __init__(self, log_level: str):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def has_new_data(self) -> bool:
        """Verify if exists new data using a HTTP resource via TCP/IP request"""

        ctrl_files = []
        try:
            ctrl_files=self.data_source.get_trigger_file_list()
        except:
            self.logger.error("Failed to read remote data.")

        return len(ctrl_files) > 0

