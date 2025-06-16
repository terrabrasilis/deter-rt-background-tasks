import glob
import os
import pathlib
import shutil
import geopandas as gpd
from tasks.http_data_source import HTTPDataSource
from tasks.output_database import OutputDatabase
from utils.database_facade import DatabaseFacade
from utils.logger import TasksLogger


class DeterRTLoader():
    """DeterRTLoader: The DETER RT loader."""

    def __init__(self, log_level: str):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def data_loader(self):
        """Used to read files from local directory and import that into output database."""

        data_dir = self.data_source.get_local_directory()

        temporary_tables = self.__shapefile_to_postgis(data_dir)

        # remove all imported files
        self.__remove_files(
            data_dir=data_dir,
            extension_list=[
                "shz",
                "sbn",
                "sbx",
                "dbf",
                "prj",
                "shx",
                "shp",
                "cpg",
                "xml",
            ],
        )

    def __shapefile_to_postgis(self, data_dir: str) -> list[str]:
        """Import shapefiles to temporary table on Postgres/Postgis database."""

        # TODO: name of the columns we need in the temporary table
        columns_raw = ["","",""]
        tables = []
        ext = "shp"
        try:

            engine = OutputDatabase().get_sqlalchemy_engine()

            files = glob.glob(os.path.join(data_dir, f"*.{ext}"))
            for filein in sorted(files):

                # Read shapefile using GeoPandas
                gdf = gpd.read_file(filein, columns=columns_raw)

                # get name of file without extension
                filein = str(str(filein).split(f".{ext}")[0]).split("/")
                table_name = filein[len(filein) - 1]

                # Import shapefile to database
                gdf.to_postgis(
                    name=table_name,
                    con=engine,
                    if_exists="replace",
                    index=True,
                    index_label="id",
                    schema="tmp",
                )

                tables.append(table_name)

                # remove shapefile
                # os.system(str("rm -f {:s}.*".format(filein)))

        except Exception as ex:
            ex_msg = f"Failed to transform {ext} files"
            self.logger.error(ex_msg)
            self.logger.error(f"{ex.__str__}")
            raise Exception(ex_msg)

        return tables


    def __remove_files(self, data_dir: str, extension_list: list):
        """
        Removes all files from a data directory, as per the extension list.

        Parameters:
        ----
        :param:data_dir a location to find files.
        :param:extension_list a list of file extensions like this: ['shz','sbn','sbx','dbf','prj','shx','shp','cpg','xml']
        """

        any_files = []
        for ext in extension_list:
            any_files.extend(glob.glob(f"{data_dir}/*.{ext}"))

        for f in any_files:
            pathlib.Path(f).unlink(missing_ok=True)
