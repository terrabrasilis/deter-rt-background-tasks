import glob
import os
import pathlib
import geopandas as gpd
from tasks.http_data_source import HTTPDataSource
from tasks.output_database import OutputDatabase
from utils.logger import TasksLogger


class DeterRTLoader():
    """DeterRTLoader: The DETER RT loader."""

    def __init__(self, log_level: str="DEBUG"):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = HTTPDataSource(log_level=log_level)

    def data_loader(self):
        """Used to read files from local directory and import that into output database."""

        data_dir = self.data_source.get_local_directory()

        temporary_tables = self.__shapefile_to_postgis(data_dir)

        self.__set_imported_file_list(files=temporary_tables)

        # remove all imported files
        # self.__remove_files(
        #     data_dir=data_dir,
        #     extension_list=[
        #         "shz",
        #         "sbn",
        #         "sbx",
        #         "dbf",
        #         "prj",
        #         "shx",
        #         "shp",
        #         "cpg",
        #         "xml",
        #     ],
        # )

    def __shapefile_to_postgis(self, data_dir: str) -> list[str]:
        """Import shapefiles to temporary table on Postgres/Postgis database."""

        # TODO: name of the columns we need in the temporary table
        columns_raw = ["area_ha","RCRmin","Date", "Confidence", "Date_dt"]
        tables = []
        ext = "shp"
        try:

            engine = OutputDatabase().get_sqlalchemy_engine()

            files = self.__get_files(data_dir=data_dir, extension=ext)
            num_files = len(files)
            for filein in sorted(files):

                # Read shapefile using GeoPandas
                try:
                    self.logger.debug(f"Reading shapefile {filein}")
                    gdf = gpd.read_file(filein)  #, columns=columns_raw)
                except Exception as ex:
                    self.logger.error(f"Failed to read shapefile {filein}")
                    self.logger.error(f"{ex}")
                    continue

                # get name of file without extension
                filein = str(str(filein).split(f".{ext}")[0]).split("/")
                table_name = filein[len(filein) - 1]

                self.logger.debug(f"Importing shapefile {filein} to table {table_name}")

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
            
            if num_files == 0:
                self.logger.warning(f"No {ext} files found on {data_dir}")
            elif num_files == len(tables):
                self.logger.info(f"All {num_files} {ext} files were imported to database")
            else:
                self.logger.warning(f"Only {len(tables)} of {num_files} {ext} files were imported to database")

        except Exception as ex:
            ex_msg = f"Failed to transform {ext} files"
            self.logger.error(ex_msg)
            self.logger.error(f"{ex}")
            raise Exception(ex_msg)

        return tables

    def __get_files(self, data_dir: str, extension: str) -> list[str]:
        """Get all files from a data directory, as per the extension."""

        files = glob.glob(os.path.join(data_dir, f"*.{extension}"))
        
        self.logger.info(f"Found {len(files)} *.{extension} files on {data_dir}")

        files = OutputDatabase().get_input_files_to_import(files=files, extension=extension)

        self.logger.info(f"Found {len(files)} files on database not imported yet")

        return files

    def __set_imported_file_list(self, files: list[str]):
        """Update the input_data table to set the import_date field."""

        outdb = OutputDatabase()
        for file_name in files:
            outdb.update_imported_file(file_name=file_name)

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
