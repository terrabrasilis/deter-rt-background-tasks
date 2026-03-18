import os
import pathlib
import requests
from requests.exceptions import HTTPError, RequestException
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from utils.logger import TasksLogger
from webdav3.client import Client
from webdav3.exceptions import ConnectionException
from datetime import date
from utils.database_facade import DatabaseFacade
from datetime import datetime
from time import sleep


class HTTPDataSource:

    # the Airflow connection ids
    WEBDAV_CONNECTION_ID = "DETER_RT_WEBDAV_NEXTCLOUD"
    data_source_config: Connection = None

    def __init__(self, log_level: str, project_dir: str = ""):
        self.project_dir = project_dir
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.__fill_airflow_configuration()
        self.client = self.__connect()

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
            "webdav_root": "/",
            "webdav_verbose": "on"
        }

        try:
            client = Client(options=options)
        except ConnectionException as connexc:
            self.logger.error("WebDav connection failed.")
            raise connexc

        return client

    def make_shapefile_list(self, reference_date: date) -> list[dict]:
        shp_files = []

        remote_path_base = f"{self.get_remote_directory()}"
        shapefile_extensions = tuple(self.get_shapefile_sufixes())
        self.logger.debug(f"{remote_path_base} path on remote server.")
        self.logger.debug(f"{','.join(shapefile_extensions)} shapefiles extensions.")

        if self.client.check(remote_path=remote_path_base):
            remote_folder_info = self.client.list(remote_path_base, get_info=True)
            self.logger.debug(f"{len(remote_folder_info)} files found on remote server.")

            for item in remote_folder_info:
                if not item['isdir'] and str(os.path.basename(item['path'])).endswith(shapefile_extensions):

                    file_date = datetime.strptime(item['modified'], '%a, %d %b %Y %H:%M:%S %Z').date()
                    if reference_date is None or file_date > reference_date:
                        item_tmp = {
                            'size':item['size'],
                            'modified':file_date.strftime('%Y-%m-%d'),
                            'path':item['path'],
                            'etag':item['etag'],
                            'file_name':os.path.basename(item['path'])
                        }
                        shp_files.append(item_tmp)

        self.logger.info(f"{len(shp_files)} files found on remote server.")

        return shp_files

    def download_file(self, output_db: DatabaseFacade, file: dict):
        """
        To download a file from http source.
        
        Parameters
        ----
        :param:output_db: The facade to read end write data on output database.
        :param:file_name: The expected file name without location path.
        :param:file_date: The expected date of the file.
        :param:tile_id: The tile identifier extracted from original file name.
        """
        file_name = file['file_name']
        remote_path_base = f"{self.get_remote_directory()}/{file_name}"
        local_path_base = f"{self.get_tmp_directory()}/{file_name}"
        url = self.get_data_source_base_url()
        username, password = self.get_data_source_credential()

        # if file already exists, avoid download again
        if os.path.isfile(path=local_path_base):
            self.logger.debug(f"Local file already exists, avoid download again.")
        else:
            self.logger.debug(f"The local file needs to be updated.")

            try:
                self.download_from_webdav(url=f"{url}/{remote_path_base}",
                                          local_filename=local_path_base,
                                          username=username,
                                          password=password)
            except HTTPError as http_err:
                # Catches 4xx or 5xx errors
                self.logger.error("Failure while trying to download file from data source.")
                self.logger.error(f"HTTP error occurred: {http_err}")
                self.logger.error(f"Status Code: {http_err.response.status_code}")
                if http_err.response.status_code == 404:
                    # do not try again if the file was not found
                    self.logger.error(f"File not found: {remote_path_base}")
                    raise FileNotFoundError
            except RequestException as err:
                # Catches broader issues like ConnectionError, Timeout, etc.
                self.logger.error(f"Other error occurred: {err}")
            except Exception as exc:
                self.logger.error("Failure while trying to download file from data source.")
                raise exc

            if not os.path.isfile(path=local_path_base):
                self.logger.info("Retrying the download after 5 seconds...")
                sleep(5)
                self.download_file(output_db=output_db, file=file)
            else:
                self.logger.info(f"file_name={file_name}")

        try:
            self.__registry_on_control_table(output_db=output_db, file=file)
        except Exception as exc:
            self.logger.error("Failed to register remote file metadata in control table.")
            raise exc

    def __registry_on_control_table(self, output_db: DatabaseFacade, file:dict):
        """Write the metadata file into control table"""

        content_length = int(file['size'])
        etag = str(file['etag']).replace('"', '')
        last_modified = file['modified']
        file_name = file['file_name']
        file_date = (file_name.split("."))[0].split("_")[3]
        tile_id = (file_name.split("."))[0].split("_")[4]

        self.logger.debug(f"file_name={file_name}")
        self.logger.debug(f"file_date={file_date}")
        self.logger.debug(f"tile_id={tile_id}")
        self.logger.debug(f"content_length={content_length}")
        self.logger.debug(f"etag={etag}")
        self.logger.debug(f"last_modified={last_modified}")

        sql = f"""INSERT INTO public.input_data(file_name, file_date, tile_id, etag, file_size, last_modified)
        VALUES ('{file_name}', '{file_date}'::date, '{tile_id}', '{etag}', {content_length}, '{last_modified}'::date)
        ON CONFLICT DO NOTHING;"""

        output_db.execute(sql=sql, logger=self.logger)

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

        return self.data_source_config.login, self.data_source_config.password

    def get_backup_directory(self) -> str:
        """
        Returns the backup directory to store data.
        If the directory does not exist, it will be created.
        """

        default_dir = str(pathlib.Path(__file__).parent.parent.resolve().absolute())
        base_dir = self.project_dir if self.project_dir else default_dir
        base_dir = f"{base_dir}/data/bkp"

        if not os.path.isdir(base_dir):
            self.logger.info(f"Creating a backup directory in {base_dir}")
            os.makedirs(base_dir)

        return base_dir


    def get_data_directory(self) -> str:
        """
        Returns the project data directory.
        We expect a data directory inside the project directory.
        If the directory does not exist, it will be created.
        """

        default_dir = str(pathlib.Path(__file__).parent.parent.resolve().absolute())
        base_dir = self.project_dir if self.project_dir else default_dir
        base_dir = f"{base_dir}/data"

        if not os.path.isdir(base_dir):
            self.logger.info(f"Creating a data directory in {base_dir}")
            os.makedirs(base_dir)

        return base_dir

    def get_tmp_directory(self) -> str:
        """
        Returns the temporary directory to store data on download process.
        If the directory does not exist, it will be created.
        """

        default_dir = str(pathlib.Path(__file__).parent.parent.resolve().absolute())
        base_dir = self.project_dir if self.project_dir else default_dir
        base_dir = f"{base_dir}/data/tmp"

        if not os.path.isdir(base_dir):
            self.logger.info(f"Creating a temporary directory in {base_dir}")
            os.makedirs(base_dir)

        return base_dir

    def get_remote_directory(self) -> str:
        """
        Read remote directory from AirFlow connection settings.

        Return: str
        """

        return self.data_source_config.extra_dejson.get("remote_directory")

    def get_shapefile_sufixes(self) -> list[str]:
        """
        Read a shapefile extension list from AirFlow connection settings.

        Return: list[str]
        """
        extension_list = str(self.data_source_config.extra_dejson.get("shapefile_extensions")).split(",")

        return extension_list

    def download_from_webdav(self, url, local_filename, username, password):
        """
        Downloads a file from a WebDAV URL using the requests library.
        Raise request exceptions.

        Args:
            url (str): The full URL to the file on the WebDAV server.
            local_filename (str): The path to save the file locally.
            username (str): The WebDAV username.
            password (str): The WebDAV password.
        """
        # Use requests.get with stream=True for efficient handling of large files
        response = requests.get(url, auth=(username, password), stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        # Write the file content in chunks
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        print(f"Successfully downloaded {local_filename}")
