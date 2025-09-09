import glob
import os
import pathlib
import geopandas as gpd
from tasks.http_data_source import HTTPDataSource
from tasks.output_database import OutputDatabase
from utils.logger import TasksLogger


class DeterRTTransformer():
    """DeterRTTransformer: The DETER RT data processor."""

    def __init__(self, log_level: str="DEBUG"):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def process_data(self):
        """
        Used to read temporary tables and write the alerts into the final table.
        Some adjustments on the data will be made here.
        1) Update the view_date and tile_id on temporary table with the file_date and tile_id from input_data.
        2) Read all temporary tables from the tmp schema and insert data into the final table on public schema.
        4) Remove temporary tables.
        """

        try:
            self.__update_tmp_data()
            self.__tmp_to_final()
        except Exception as ex:
            self.logger.error(f"Error processing data: {ex}")
            raise ex
        
        try:
            self.__remove_tmp_tables()
        except Exception as ex:
            self.logger.error(f"Error removing temporary tables: {ex}")
            raise ex

    def __update_tmp_data(self):
        """Update the view_date and tile_id on temporary table with the file_date and tile_id from input_data."""

        outdb = OutputDatabase()
        tmp_tables = outdb.get_tmp_tables()
        for table in tmp_tables:
            outdb.update_tmp_table(table=table)

    def __tmp_to_final(self,):
        """Read all temporary tables from the tmp schema and insert data into the final table on public schema."""

        outdb = OutputDatabase()
        outdb.tmp_to_final()

    def __remove_tmp_tables(self):
        """Remove all temporary tables from tmp schema."""

        outdb = OutputDatabase()
        outdb.drop_tmp_tables()
