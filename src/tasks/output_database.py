from datetime import datetime, date
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from utils.database_facade import DatabaseFacade
from utils.deter_parameters import DETERParameters
from utils.logger import TasksLogger
from sqlalchemy import create_engine


class OutputDatabase:

    # the Airflow connection ids
    # used to access the output database
    DETER_RT_CONNECTION_ID: str = "DETER_RT_DB_URL"
    database: DatabaseFacade = None # type: ignore
    logger: TasksLogger = None # type: ignore

    def __init__(self, log_level:str="WARNING"):
        self.deter_rt_db_url: Connection = BaseHook.get_connection(self.DETER_RT_CONNECTION_ID)
        self.class_group = DETERParameters().class_group
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)

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
        data = outdb.execute(sql=sql, logger=self.logger)
        outdb.close()

        assert data and data == 1

    def create_dblink_extension(self):
        outdb = self.get_database_facade()
        sql = f"""CREATE EXTENSION IF NOT EXISTS dblink;"""
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()

    def create_data_source_sql_view(self, sql):
        outdb = self.get_database_facade()
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()

    def drop_data_source_sql_view(self, sql):
        outdb = self.get_database_facade()
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()

    def get_max_date_optical_deter(self) -> date:
        """Gets the max date of optical DETER."""

        outdb = self.get_database_facade()
        sql = f"SELECT MAX(view_date)::date FROM public.deter_otico WHERE class_name IN ({self.class_group['deter']['DS']});"
        data = outdb.fetchone(query=sql, logger=self.logger)
        max_date = None
        outdb.close()
        if data is not None and len(data) > 0:
            max_date = date(year=data[0].year, month=data[0].month, day=data[0].day)

        return max_date # type: ignore


    def get_max_date_input_file(self) -> date:
        """Gets the max date of last downloaded shapefile."""

        outdb = self.get_database_facade()
        sql = f"SELECT TO_CHAR(MAX(last_modified), 'YYYY-MM-DD') FROM public.input_data ip, public.deter_rt rt WHERE ip.file_date=rt.view_date AND ip.tile_id=rt.tile_id;"
        data = outdb.fetchone(query=sql, logger=self.logger)
        max_date = None
        outdb.close()
        if data is not None and len(data) > 0 and data[0] is not None:
            max_date = datetime.strptime(data[0], '%Y-%m-%d').date()

        return max_date # type: ignore


    def get_input_files_to_import(self, files: list[str], extension:str) -> list[str]:
        """
        Gets the list of downloaded shapefiles that were not imported into the database.
        Using the input list of files on disc to filter the list of files not imported.
        """

        outdb = self.get_database_facade()
        sql = f"SELECT file_name FROM public.input_data ip WHERE ip.import_date IS NULL AND file_name ilike '%.{extension}';"
        data = outdb.fetchall(query=sql, logger=self.logger)
        out_files = []
        if data is not None and len(data) > 0 and files is not None and len(files) > 0:
            files_on_db = [r[0] for r in data]
            _files_on_db=":".join(files_on_db)
            _files=":".join(files)
            self.logger.debug(f"Files on database: {_files_on_db}")
            self.logger.debug(f"Files on datastore: {_files}")
            for f in files:
                if f.split("/").pop() in files_on_db:
                    out_files.append(f)

            #out_files.append(f for f in files if f.split("/").pop() in [r[0] for r in data])

        outdb.close()
        return out_files

    def update_imported_file(self, file_name: str):
        """Update the input_data table to set the import_date field."""

        outdb = self.get_database_facade()
        sql = f"""UPDATE public.input_data SET import_date=NOW()::date WHERE file_name ilike '{file_name}%';"""
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()

    def get_tmp_tables(self) -> list[str]:
        """Get the list of temporary tables on tmp schema."""

        outdb = self.get_database_facade()
        sql = f"""SELECT table_name FROM information_schema.tables WHERE table_schema='tmp' AND table_type='BASE TABLE';"""
        data = outdb.fetchall(query=sql, logger=self.logger)
        tables = []
        if data is not None and len(data) > 0:
            tables = [r[0] for r in data]

        outdb.close()
        return tables
    

    def update_tmp_table(self, table: str):
        """Update the view_date and tile_id on temporary table with the file_date and tile_id from input_data."""

        outdb = self.get_database_facade()
        alter_sql = f"""ALTER TABLE IF EXISTS tmp."{table}" ADD COLUMN IF NOT EXISTS view_date date;"""
        outdb.execute(sql=alter_sql, logger=self.logger)
        outdb.commit()
        alter_sql = f"""ALTER TABLE IF EXISTS tmp."{table}" ADD COLUMN IF NOT EXISTS tile_id character varying;"""
        outdb.execute(sql=alter_sql, logger=self.logger)
        outdb.commit()

        sql = f"""UPDATE tmp."{table}" AS tmp SET view_date=ip.file_date, tile_id=ip.tile_id FROM public.input_data AS ip WHERE ip.file_name='{table}.shp';"""
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()

  
    def tmp_to_final(self):
        """Insert data from all temporary tables on tmp schema into the final table on public schema."""

        outdb = self.get_database_facade()
        sql = f"""DO $$ DECLARE r RECORD;
                  BEGIN
                      FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema='tmp' AND table_type='BASE TABLE') LOOP
                          EXECUTE 'INSERT INTO public.deter_rt (geom, class_name, view_date, detection_date, area_km, tile_id) SELECT ST_Multi(ST_Transform(geometry,4674)), ''DESMATAMENTO_CR'', view_date, "Date_dt"::date, (ST_Area((ST_Transform(geometry,4674))::geography))/1000000, tile_id FROM tmp.' || quote_ident(r.table_name);
                      END LOOP;
                  END $$;"""
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()


    def drop_tmp_tables(self):
        """Drop all temporary tables on tmp schema."""

        outdb = self.get_database_facade()
        sql = f"""DO $$ DECLARE r RECORD;
                  BEGIN
                      FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema='tmp' AND table_type='BASE TABLE') LOOP
                          EXECUTE 'DROP TABLE IF EXISTS tmp.' || quote_ident(r.table_name) || ' CASCADE';
                      END LOOP;
                  END $$;"""
        outdb.execute(sql=sql, logger=self.logger)
        outdb.commit()