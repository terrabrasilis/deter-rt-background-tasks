from tasks.collector import Collector
from tasks.output_database import OutputDatabase
from tasks.sqlview_data_source import SQLViewDataSource
from utils.database_facade import DatabaseFacade
from datetime import date


class SQLViewCollector(Collector):
    """SQLView Collector: Represents a reader data from internal Database sources."""

    def __init__(self, log_level: str):
        super().__init__(log_level=log_level)
        self.data_source: SQLViewDataSource = SQLViewDataSource()

    def read_data(self):
        """Load data using a SQLView resouce via DBLink"""

        outdb: DatabaseFacade
        db: OutputDatabase
        num_rows: int

        try:
            db = OutputDatabase()
            outdb = db.get_database_facade()

        except Exception as exc:
            self.logger.error("Failure on SQLViewCollector.read_data")
            self.logger.error(str(exc))
            raise Exception("Failed to connect to output database")

        try:
            last_deter_date = self.__get_last_deter_date(outdb=outdb)
            self.logger.info(f"last_deter_date={last_deter_date}")

            # create dblink extension if not exists
            db.create_dblink_extension()
            # create a SQLView in the output database from the source database
            db.create_data_source_sql_view(sql=self.data_source.sql_view_to_create())

            sql = self.data_source.sql_copy_from_data_source(reference_date=last_deter_date)
            num_rows = outdb.execute(sql=sql, logger=self.logger)
            
            # Remove SQLView from the output database
            db.drop_data_source_sql_view(sql=self.data_source.sql_view_to_drop())
            
            outdb.commit()
            
            # test the copy data
            if num_rows is None or num_rows <= 0:
                raise Exception(f"Missing DETER optical data to copy.")

        except Exception as exc:
            outdb.rollback()
            self.logger.error("Failure on SQLViewCollector.read_data")
            self.logger.error(str(exc))
            raise Exception("Failed to load data using a SQL View resource via DBLink")
        finally:
            outdb.close()

    def __get_last_deter_date(self, outdb: DatabaseFacade) -> date:
        """To get the latest date of DETER data loaded from the data source."""

        sql = f"""SELECT MAX(view_date) FROM public.deter_otico;"""
        data = outdb.fetchone(query=sql)
        
        # the default date based on the project definition
        deter_date = None

        if data and data[0]:
            deter_date = data[0]

        return deter_date