import os
from tasks.collector import Collector
from tasks.http_data_source import HTTPDataSource
from tasks.output_database import OutputDatabase
from datetime import date


class HTTPCollector(Collector):
    """HTTP Collector: Represents a reader data from HTTP sources."""

    def __init__(self, log_level: str):
        super().__init__(log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def read_data(self):
        """Read data using request by an URL"""

        self.outdb: OutputDatabase

        try:
            self.outdb = OutputDatabase().get_database_facade(keep_connection=True)
        except Exception as exc:
            self.logger.error("Error connecting to output database.")
            raise exc

        try:

            outputdb = OutputDatabase()
            reference_date = outputdb.get_max_date_input_file()

            file_list = self.data_source.make_shapefile_list(reference_date=reference_date)

            for file in file_list:
                self.data_source.download_file(output_db=self.outdb, file=file)

            self.outdb.commit()
        except Exception as exc:
            self.logger.error(f"Reverting records of downloaded data.")
            self.outdb.rollback()
            raise exc
        finally:
            self.outdb.close()

    def __extract_metadata(self, file_path: str):
        """Extract the matadata from file name."""

        file_name = os.path.basename(file_path)
        metadata = file_name.split("_")
        str_date = metadata[2]
        str_date = str_date.split("-")
        file_date = date(year=int(str_date[0]), month=int(str_date[1]), day=int(str_date[2]))
        tile_id = metadata[3]
        return file_name, file_date, tile_id