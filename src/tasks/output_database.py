from datetime import date
from types import NoneType
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from utils.database_facade import DatabaseFacade
from utils.deter_parameters import DETERParameters
from sqlalchemy import create_engine


class OutputDatabase:

    # the Airflow connection ids
    # used to access the output database
    DETER_RT_CONNECTION_ID: str = "DETER_RT_DB_URL"

    def __init__(self):
        self.deter_rt_db_url: Connection = BaseHook.get_connection(self.DETER_RT_CONNECTION_ID)
        self.database: DatabaseFacade
        self.class_group = DETERParameters().class_group

    def get_sqlalchemy_engine(self):
        """Gets the connection engine base on SqlAlchemy to use in GeoPandas"""

        url = engine = None
        url = self.get_database_facade().sqlalchemy_url
        engine = create_engine(url=url)
        assert engine

        return engine

    def get_database_facade(self, keep_connection: bool = True) -> DatabaseFacade:
        """Return a DatabaseFacade of the requested connection id."""

        if self.database is not None and isinstance(self.database, DatabaseFacade):
            return self.database

        if self.deter_rt_db_url is not None:

            database = DatabaseFacade.from_url(self.deter_rt_db_url.get_uri())

            if database:
                if keep_connection == True:
                    self.database = database

                return database
            else:
                raise Exception(
                    f"Missing config on Airflow connections. Connection id: {self.DETER_RT_CONNECTION_ID}"
                )

        else:
            raise Exception(
                f"Connection config not found on Airflow connections. Connection id: {self.DETER_RT_CONNECTION_ID}"
            )

    def test_connection(self):
        """Gets database connection and exec a simple query."""

        outdb = self.get_database_facade()
        sql = f"""SELECT 'test';"""
        data = outdb.execute(sql=sql)
        outdb.close()

        assert data and data == 1

    def create_data_source_sql_view(self, sql):
        outdb = self.get_database_facade()
        outdb.execute(sql=sql)
        outdb.commit()

    def drop_data_source_sql_view(self, sql):
        outdb = self.get_database_facade()
        outdb.execute(sql=sql)
        outdb.commit()

    def get_max_date_optical_deter(self) -> date | NoneType:
        """Gets the max date of optical DETER."""

        outdb = self.get_database_facade()
        sql = f"SELECT MAX(view_date)::date FROM public.deter_otico WHERE class_name IN ({self.class_group['deter']['DS']});"
        data = outdb.fetchone(query=sql)
        max_date = None
        outdb.close()
        if data is not None and len(data) > 0:
            max_date = date(year=data[0].year, month=data[0].month, day=data[0].day)

        return max_date


    # def get_file_metadata(self, data_source_name: str, file_name: str):
    #     """
    #     To get the metadata of the most recently downloaded file for a data source

    #     Return
    #     -----
    #     last_modified, etag, file_size
    #     """

    #     outdb = OutputDatabase().get_database_facade(keep_connection=False)
    #     sql = f"""SELECT last_modified, etag, file_size
    #     FROM public.input_data WHERE file_name='{file_name}'
    #     ORDER BY download_date DESC LIMIT 1;"""
    #     data = outdb.fetchone(query=sql)
    #     outdb.close()
    #     metadata = None

    #     if data is not None and len(data) > 0:
    #         metadata = data

    #     return metadata
