import os
import pathlib
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from utils.logger import TasksLogger
from webdav3.client import Client
from webdav3.client import ConnectionException
from datetime import date
from utils.database_facade import DatabaseFacade
from datetime import datetime


class HTTPDataSource:

    # the Airflow connection ids
    WEBDAV_CONNECTION_ID = "DETER_RT_WEBDAV_NEXTCLOUD"
    data_source_config: Connection = None

    def __init__(self, log_level: str, project_dir: str = ""):
        self.project_dir = project_dir
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.__fill_airflow_configuration()

    def __fill_airflow_configuration(self):

        self.data_source_config = BaseHook.get_connection(self.WEBDAV_CONNECTION_ID)

        if not self.data_source_config:
            raise Exception(
                f"Connection config not found on Airflow connections. Connection id: {self.WEBDAV_CONNECTION_ID}"
            )

    def __connect(self) -> Client:
        """Prepare a client connection to WebDav and return it."""

        url = self.get_data_source_base_url()
        user, password = self.get_data_source_credential()

        options = {
            "webdav_hostname": url,
            "webdav_login": user,
            "webdav_password": password,
        }

        try:
            client = Client(options=options)
        except ConnectionException as connexc:
            self.logger.error("WebDav connection failed.")
            raise connexc

        return client

    def make_shapefile_list(self, trigger_file: str) -> list[str]:
        shp_files = []

        remote_path_base = f"{self.get_remote_directory()}/{trigger_file}"
        local_path_base = f"{self.get_local_directory()}/{trigger_file}"

        client = self.__connect()

        client.download_file(remote_path=remote_path_base, local_path=local_path_base)

        lines: list[str]
        with open(local_path_base, "r") as f:
            lines = f.readlines()

        for line in lines:
            if line.endswith(".shp"):
                shp_files.append(line)

        return shp_files

    def download_file(self, output_db: DatabaseFacade, file_name: str, file_date: date, tile_id: str):
        """
        To download a file from http source.
        
        Parameters
        ----
        :param:output_db: The facade to read end write data on output database.
        :param:file_name: The expected file name without location path.
        :param:file_date: The expected date of the file.
        :param:tile_id: The tile identifier extracted from original file name.
        """
        remote_path_base = f"{self.get_remote_directory()}/{file_name}"
        local_path_base = f"{self.get_local_directory()}/{file_name}"

        client = self.__connect()

        # if file already exists, avoid download again
        if os.path.isfile(path=local_path_base):
            self.logger.debug(f"Local file is up to date. Skipping download step.")
            return True
        else:
            self.logger.debug(f"The local file needs to be updated.")

        try:
            client.download_file(remote_path=remote_path_base, local_path=local_path_base)
        except Exception as exc:
            self.logger.error("Failure while trying to download file from data source.")
            raise exc

        self.logger.info(f"file_name={file_name}")
        self.logger.info(f"file_date={file_date}")
        self.logger.info(f"tile_id={tile_id}")

        try:
            self.__registry_on_control_table(output_db=output_db, file_name=file_name, file_date=file_date, tile_id=tile_id)
        except Exception as exc:
            self.logger.error("Failed to register remote file metadata in control table.")
            raise exc

    def __registry_on_control_table(self, output_db: DatabaseFacade, file_name:str, file_date: date, tile_id: str):
        """Write the metadata file into control table"""

        last_modified, etag, content_length = self.__get_file_metadata(file_name=file_name)

        sql = f"""INSERT INTO public.input_data(file_name, file_date, tile_id, etag, file_size, last_modified)
        VALUES ('{file_name}', '{file_date}'::date, '{tile_id}', '{etag}', {content_length}, '{last_modified}'::date);"""

        output_db.execute(sql=sql, logger=self.logger)


    def __get_file_metadata(self, file_name: str):
        """Get metadata from remote file."""

        remote_path_base = f"{self.get_remote_directory()}/{file_name}"
        client = self.__connect()

        f_info = client.info(remote_path=f"{remote_path_base}")
        last_modified = datetime.strptime(f_info['modified'], '%a, %d %b %Y %H:%M:%S %Z')
        etag = str(f_info['etag']).replace('"','')
        content_length = f_info['size']
        
        return last_modified, etag, content_length


    def get_trigger_file_list(self) -> list[str]:
        
        ctrl_files=[]
        remote_path_base = self.get_remote_directory()

        client = self.__connect()
        trigger_file_prefix = self.get_trigger_file_prefix()

        if client.check(remote_path=remote_path_base):
            remote_folder_info = client.list(remote_path_base, get_info=True)
            for item in remote_folder_info:
                if not item['isdir'] and str(os.path.basename(item['path'])).startswith(trigger_file_prefix):
                    ctrl_files.append(os.path.basename(item['path']))
        
        return ctrl_files

    def get_data_source_base_url(self) -> str:
        """Create a base URL using AirFlow connection settings."""

        url = f"{self.data_source_config.schema}://{self.data_source_config.host}"
        url = (
            f"{url}:{self.data_source_config.port}"
            if self.data_source_config.port is not None
            and len(str(self.data_source_config.port)) > 0
            else f"{url}"
        )

        return url

    def get_data_source_credential(self) -> tuple[str, str]:
        """
        Read credential from AirFlow connection settings.

        Return: tuple[user,password]
        """

        return self.data_source_config.user, self.data_source_config.password

    def get_local_directory(self) -> str:
        """
        Returns the directory to store data on download process.
        If the directory does not exist, it will be created.
        """

        default_dir = str(pathlib.Path(__file__).parent.parent.resolve().absolute())
        base_dir = self.project_dir if self.project_dir else default_dir
        base_dir = f"{base_dir}/data/tmp"

        if not os.path.isdir(base_dir):
            self.logger.info(f"Creating a directory in {base_dir}")
            os.makedirs(base_dir)

        return base_dir

    def get_remote_directory(self) -> str:
        """
        Read remote directory from AirFlow connection settings.

        Return: str
        """

        return self.data_source_config.extra_dejson.get("remote_directory")

    def get_trigger_file_prefix(self) -> str:
        """
        Read trigger file prefix from AirFlow connection settings.

        Return: str
        """

        return self.data_source_config.extra_dejson.get("trigger_file_prefix")
