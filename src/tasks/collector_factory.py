from tasks.collector import Collector
from tasks.http_collector import HTTPCollector
from tasks.http_data_source import HTTPDataSource
from tasks.sqlview_collector import SQLViewCollector
from tasks.data_source_configuration import DataSourceConfiguration
from tasks.sqlview_data_source import SQLViewDataSource


class CollectorFactory:
    """Creator: Defines the Factory Method to create collectors."""

    collectors = {"dblink": SQLViewCollector, "http": HTTPCollector}
    data_sources = {"dblink": SQLViewDataSource, "http": HTTPDataSource}

    def __init__(self, source_name: str, log_level: str):
        self.source_name = source_name
        self.log_level = log_level

    def create_collector(self, project_dir: str) -> Collector:
        """Factory Method: Create a Collector instance."""

        data_source = DataSource(source_name=self.source_name)
        data_source.project_dir = project_dir

        return self.collectors[data_source.access_type](
            self.data_sources[data_source.access_type](source_name=self.source_name),
            log_level=self.log_level
        )
