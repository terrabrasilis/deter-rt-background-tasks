from datetime import date
from utils.database_facade import DatabaseFacade
from utils.deter_parameters import DETERParameters
from airflow.models import Connection
from airflow.hooks.base import BaseHook


class SQLViewDataSource:

    database: DatabaseFacade
    # the Airflow connection ids
    # used to build the SQLView to copy deter amz data
    DETER_AMAZONIA_CONNECTION_ID: str = "DETER_AMAZONIA_DB_URL"
    deter_amz_db_url: Connection = None

    def __init__(self):
        self.deter_amz_db_url = BaseHook.get_connection(
            self.DETER_AMAZONIA_CONNECTION_ID
        )
        self.class_group = DETERParameters().class_group

    def __build_dblink_url(self) -> str:
        """Return a DBLink URL from AirFlow connection id."""

        if self.deter_amz_db_url is not None:

            database = DatabaseFacade.from_url(self.deter_amz_db_url.get_uri())
            return database.dblink
        else:
            raise Exception(
                f"Missing config on Airflow connections. Connection id: {self.DETER_AMAZONIA_CONNECTION_ID}"
            )

    def sql_view_to_create(self) -> str:
        """Build query string to create the SQL View to DETER data source using DBLink"""

        dblink = self.__build_dblink_url()

        return f"""
                CREATE OR REPLACE VIEW public.deter_data_source
                AS
                SELECT data_source.classname,
                    data_source.date,
                    data_source.geom
                FROM dblink('{dblink}'::text,
                    'SELECT classname, date, st_multi(geom)::geometry(MultiPolygon,4674) AS geom FROM terrabrasilis.deter_table
                    UNION
                    SELECT classname, date, st_multi(geom)::geometry(MultiPolygon,4674) AS geom FROM public.deter_history'::text)
                    data_source(classname character varying(256), date date, geom geometry(MultiPolygon,4674));
                """

    def sql_view_to_drop(self) -> str:
        """Build query string to DROP the SQL View to DETER data source"""

        return f"""
        DROP VIEW public.deter_data_source;
        """

    def sql_copy_from_data_source(self, reference_date: date) -> str:
        """Build query string to COPY data from DETER data source to output database"""

        filter_by_date = ""
        if reference_date:
            str_dt = reference_date.strftime("%Y%m%d")
            filter_by_date = f"AND date >= '{str_dt}'::date"

        return f"""
        INSERT INTO public.deter(geom, view_date, class_name)
        SELECT geom, date, classname FROM public.deter_data_source
        WHERE classname IN ({self.class_group['deter']['DS']})
        {filter_by_date};
        """

    def sql_count_data_source(self, reference_date: date) -> str:
        """Build query string to COUNT data from DETER data source filtered by a reference date."""

        filter_by_date = ""

        if reference_date:
            str_dt = reference_date.strftime("%Y%m%d")
            filter_by_date = (
                f"WHERE date >= '{str_dt}'::date"
            )

        return f"""SELECT count(*) FROM public.deter_data_source {filter_by_date};"""
