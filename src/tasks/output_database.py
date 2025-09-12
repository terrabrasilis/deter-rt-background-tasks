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
    database: DatabaseFacade = None  # type: ignore
    logger: TasksLogger = None  # type: ignore

    def __init__(self, log_level: str):
        self.deter_rt_db_url: Connection = BaseHook.get_connection(
            self.DETER_RT_CONNECTION_ID
        )
        self.class_group = DETERParameters().class_group
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)

        # validation parameters
        self.current_table = "deter_rt"
        self.deter_optical_table = "deter_otico"
        self.audited_table = "deter_rt_validados"
        self.intermediary_table = "by_percentage_of_coverage"
        self.threshold = "0.5"  # 50%
        # the number of selected candidates by bigger areas
        self.limit_bigger_area = "100"
        # the number of randomly selected candidates
        self.limit_random = "100"

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
        sql = f"SELECT MAX(view_date)::date FROM public.{self.deter_optical_table} WHERE class_name IN ({self.class_group['deter']['DS']});"
        data = outdb.fetchone(query=sql, logger=self.logger)
        max_date = None
        outdb.close()
        if data and len(data) > 0 and isinstance(data[0], date):
            max_date = date(year=data[0].year, month=data[0].month, day=data[0].day)

        return max_date  # type: ignore

    def get_max_date_input_file(self) -> date:
        """Gets the max date of last downloaded shapefile."""

        outdb = self.get_database_facade()
        sql = f"SELECT MAX(last_modified)::date FROM public.input_data ip, public.{self.current_table} rt WHERE ip.file_date=rt.view_date AND ip.tile_id=rt.tile_id;"
        data = outdb.fetchone(query=sql, logger=self.logger)
        max_date = None
        outdb.close()
        if data is not None and len(data) > 0 and isinstance(data[0], date):
            max_date = date(year=data[0].year, month=data[0].month, day=data[0].day)

        return max_date  # type: ignore

    def get_input_files_to_import(self, files: list[str], extension: str) -> list[str]:
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
            _files_on_db = ":".join(files_on_db)
            _files = ":".join(files)
            self.logger.debug(f"Files on database: {_files_on_db}")
            self.logger.debug(f"Files on datastore: {_files}")
            for f in files:
                if f.split("/").pop() in files_on_db:
                    out_files.append(f)

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
                          EXECUTE 'INSERT INTO public.{self.current_table} (geom, class_name, view_date, detection_date, area_km, tile_id) SELECT ST_Multi(ST_Transform(geometry,4674)), ''alerta'', view_date, "Date_dt"::date, (ST_Area((ST_Transform(geometry,4674))::geography))/1000000, tile_id FROM tmp.' || quote_ident(r.table_name);
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

    def validate_data(self):
        """Validate the deter rt data with intersection over otical deter."""

        outdb = self.get_database_facade()

        # Compute difference between DETER_RT and DETER_B
        CREATE_TABLE = f"""
        CREATE TABLE public.{self.intermediary_table} AS
        SELECT null::character varying as nome_avaliador1, null::integer as auditar, null::timestamp without time zone as datafim_avaliador1, 
            area_km, b.class_name as optical_class_name, view_date, now()::date as created_at, tile_id, uuid, null::character varying as classe_avaliador1,
            (ST_Multi(ST_CollectionExtract(
                COALESCE(
                safe_diff(a.geom,
                    ( SELECT st_union(st_buffer(b.geom,0.000000001))
                    FROM public.{self.deter_optical_table} b
                    WHERE
                        b.class_name IN ('DESMATAMENTO_VEG','DESMATAMENTO_CR','MINERACAO')
                        AND created_at<=now()::date
                        AND (a.geom && b.geom)
                    )
                ),
                a.geom
                )
            ,3))
            ) AS geom_diff,
            ST_Multi(a.geom) as geom_original
        FROM public.{self.current_table} a
        WHERE a.view_date IN(
            SELECT file_date FROM public.input_data WHERE import_date=now()::date GROUP BY 1
        );
        """

        # update area_km on intermediary table
        UPDATE_AREA = f"""
        UPDATE public.{self.intermediary_table} SET area_km=ST_Area(geom_original::geography)/1000000;
        """

        # DETER_RT alerts are marked as audited by default when DETER_B coverage is greater than or equal to one threshold (50%)
        WITHOUT_AUDIT = f"""
        WITH calculate_area AS (
            SELECT optical_class_name, ST_Area(geom_diff::geography)/1000000 as area_diff,ST_Area(geom_original::geography)/1000000 as area_original, uuid
            FROM public.{self.intermediary_table}
        )
        UPDATE public.{self.intermediary_table}
        SET auditar=0, datafim_avaliador1=now()::timestamp without time zone,
        classe_avaliador1=b.optical_class_name, nome_avaliador1='automatico'
        FROM calculate_area b
        WHERE public.{self.intermediary_table}.uuid=b.uuid AND b.area_diff < (b.area_original*{self.threshold});
        """

        # the candidates by bigger areas
        CANDIDATES_BY_AREA = f"""
        UPDATE public.{self.intermediary_table}
        SET auditar=1
        WHERE uuid IN (
            SELECT uuid FROM public.{self.intermediary_table}
        WHERE auditar IS NULL AND datafim_avaliador1 IS NULL ORDER BY area_km DESC LIMIT {self.limit_bigger_area}
        );
        """

        # the candidates by random
        CANDIDATES_BY_RANDOM = f"""
        UPDATE public.{self.intermediary_table}
        SET auditar=1
        WHERE uuid IN (
            SELECT uuid FROM public.{self.intermediary_table}
        WHERE auditar IS NULL AND datafim_avaliador1 IS NULL ORDER BY random() LIMIT {self.limit_random}
        );
        """

        # Set auditar=0 for anyone who is still null after applying the rules
        ANYONE_STILL_NULL = f"""
        UPDATE public.{self.intermediary_table}
        SET auditar=0
        WHERE auditar IS NULL AND datafim_avaliador1 IS NULL;
        """

        # copy the automated audited entries to the audited table
        COPY_TO_ADITED = f"""
        INSERT INTO public.{self.audited_table}(
        uuid, lon, lat, area_km, view_date, class_name,
        nome_avaliador1, classe_avaliador1, datafim_avaliador1, deltat_avaliador1,
        nome_avaliador2, classe_avaliador2, datafim_avaliador2, deltat_avaliador2,
        geom, created_at, tile_id, auditar)
        SELECT uuid, ST_X(ST_Centroid(geom_original)) as lon, ST_Y(ST_Centroid(geom_original)) as lat,
        area_km, view_date, 'alerta'::character varying(256) as class_name,
        nome_avaliador1, classe_avaliador1, datafim_avaliador1, 0 as deltat_avaliador1,
        nome_avaliador1 as nome_avaliador2, classe_avaliador1 as classe_avaliador2, datafim_avaliador1 as datafim_avaliador2, 0 as deltat_avaliador2,
        geom_original as geom, created_at, tile_id, auditar
        FROM public.{self.intermediary_table}
        WHERE auditar=1 OR (auditar=0 AND nome_avaliador1='automatico');
        """

        DROP_TMP_TABLE = f"DROP TABLE public.{self.intermediary_table};"

        # create the intermeriary table without overlap
        outdb.execute(sql=CREATE_TABLE, logger=self.logger)
        # update area
        outdb.execute(sql=UPDATE_AREA, logger=self.logger)

        # Marked as audited by default when coverage is greater than or equal to 50%
        outdb.execute(sql=WITHOUT_AUDIT, logger=self.logger)
        self.logger.info(
            "Marked as audited by default when coverage is greater than or equal to 50%"
        )

        # Update audit to 1 to the first limit_bigger_area candidates
        outdb.execute(sql=CANDIDATES_BY_AREA, logger=self.logger)
        self.logger.info(f"Define the first {self.limit_bigger_area} candidates")

        # Update audit to 1 to the random limit_random candidates
        outdb.execute(sql=CANDIDATES_BY_RANDOM, logger=self.logger)
        self.logger.info(f"Define the random {self.limit_random} candidates")

        # Update the audit to 0 for the residual
        outdb.execute(sql=ANYONE_STILL_NULL, logger=self.logger)
        self.logger.info("Update the auditar to 0 for the residual")

        # Copy data, audited by the automatic method, to the audited data table.
        outdb.execute(sql=COPY_TO_ADITED, logger=self.logger)
        self.logger.info(
            "Copy data, audited by the automatic method, to the audited data table."
        )

        # drop the temporary table
        outdb.execute(sql=DROP_TMP_TABLE, logger=self.logger)
        self.logger.info(f"Drop the temporary table ({self.intermediary_table})")

        outdb.commit()

    def get_info_to_report(self):
        """
        Get some information to build the automatic validation report.
        Return a dictionary with the number of alerts sent to audit and the number of alerts approved by automatic audit.
        Return example: {"alerts_to_audit":120, "alerts_approved":45}
        """

        outdb = self.get_database_facade()

        # Number of alerts sent to audit
        SELECT_RESULT1 = f"""
        SELECT count(*) 
        FROM public.{self.audited_table}
        WHERE created_at>=now()::date AND auditar=1
        """

        result1 = outdb.fetchall(query=SELECT_RESULT1, logger=self.logger)

        # Number of alerts approved by automatic audit
        SELECT_RESULT2 = f"""
        SELECT count(*) 
        FROM public.{self.audited_table}
        WHERE created_at>=now()::date AND nome_avaliador1='automatico'
        """

        result2 = outdb.fetchall(query=SELECT_RESULT2, logger=self.logger)

        return {
            "alerts_to_audit": result1[0][0] if result1 and len(result1) > 0 else 0,
            "alerts_approved": result2[0][0] if result2 and len(result2) > 0 else 0,
        }
