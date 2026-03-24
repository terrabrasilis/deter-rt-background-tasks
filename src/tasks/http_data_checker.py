from tasks.http_data_source import HTTPDataSource
from utils.logger import TasksLogger
from tasks.output_database import OutputDatabase


class HTTPDataChecker:
    """HTTP DataChecker: Represents a checker data for a HTTP sources."""

    def __init__(self, log_level: str):
        self.log_level = log_level
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.logger.debug("Initializing HTTP Data Checker")
        self.data_source = HTTPDataSource(log_level=log_level)

    def has_new_data(self) -> bool:
        """
        Verify if exists new data using a HTTP resource via TCP/IP request.

        The first check is the **upload.lock** file.
        If this file exists in the remote shared directory, the execution process should be stopped.
        """
        lock_file="upload.lock"
        ctrl_files = []
        try:
            if self.data_source.lock_file_exists(lock_file=lock_file):
                self.logger.debug(f"Found {lock_file} on remote server. Abort.")
            else:
                outputdb = OutputDatabase(log_level=self.log_level)
                reference_date = outputdb.get_max_date_input_file()
                self.logger.debug(f"Reference date to check new data: {reference_date}")

                ctrl_files=self.data_source.make_shapefile_list(reference_date=reference_date)
                self.logger.debug(f"Found {len(ctrl_files)} new files on remote server.")

        except Exception as e:
            self.logger.error("Failed to read remote data.")
            self.logger.error(f"{e}")

        return len(ctrl_files) > 0

