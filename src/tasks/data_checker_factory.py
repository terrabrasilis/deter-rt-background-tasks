from tasks.data_checker import DataChecker
from tasks.data_source_configuration import DataSourceConfiguration
from tasks.http_data_checker import HTTPDataChecker
from tasks.http_data_source import HTTPDataSource
from tasks.sqlview_data_checker import SQLViewDataChecker
from tasks.sqlview_data_source import SQLViewDataSource


class DataCheckerFactory:
    """Creator: Defines the Factory Method to create data checkers."""

    data_checkers = {"dblink": SQLViewDataChecker, "http": HTTPDataChecker}
    data_sources = {"dblink": SQLViewDataSource, "http": HTTPDataSource}

    def __init__(self, source_name: str, log_level: str):
        self.source_name = source_name
        self.log_level = log_level

    def create_data_checker(self) -> DataChecker:
        """Factory Method: Create a DataChecker instance."""

        data_source = DataSourceConfiguration(source_name=self.source_name)

        return self.data_checkers[data_source.access_type](
            self.data_sources[data_source.access_type](source_name=self.source_name),
            log_level=self.log_level,
        )
