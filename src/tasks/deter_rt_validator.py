from tasks.http_data_source import HTTPDataSource
from tasks.output_database import OutputDatabase
from utils.logger import TasksLogger


class DeterRTValidator:
    """DeterRTValidator: The DETER RT automatic validator."""

    def __init__(self, log_level: str = "DEBUG"):
        self.log_level = log_level
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def validation(self):
        """Used to validate the data loaded into output database."""

        try:
            outputdb = OutputDatabase(log_level=self.log_level)
            outputdb.validate_data()

        except Exception as ex:
            ex_msg = "Failed to validate data on output database"
            self.logger.error(ex_msg)
            self.logger.error(f"{ex}")
            raise Exception(ex_msg)