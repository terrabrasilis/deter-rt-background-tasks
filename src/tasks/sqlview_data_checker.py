from datetime import date
from tasks.output_database import OutputDatabase
from tasks.sqlview_data_source import SQLViewDataSource
from utils.database_facade import DatabaseFacade
from utils.logger import TasksLogger


class SQLViewDataChecker():
    """SQLView DataChecker: Represents a checker data for a Database sources."""

    def __init__(self, log_level: str):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(level=log_level)
        self.data_source = SQLViewDataSource()

    def has_new_data(self) -> bool:
        """Check for new data using a SQL View resource via DBLink"""

        output_db: OutputDatabase
        try:
            output_db = OutputDatabase()
        except Exception as exc:
            self.logger.error("Failure on SQLViewDataChecker.has_new_data")
            self.logger.error(str(exc))
            raise Exception("Failed to connect to output database")

        reference_date = output_db.get_max_date_optical_deter()

        return self.__has_deter_in_data_source(output_db=output_db, reference_date=reference_date)

    def __has_deter_in_data_source(self, output_db: OutputDatabase, reference_date: date) -> bool:
        """Do we have new DETER-RT files in the data source?"""

        db_facade: DatabaseFacade

        try:
            db_facade = output_db.get_database_facade()
        except Exception as exc:
            self.logger.error("Failure on SQLViewDataChecker.__has_deter_in_data_source")
            self.logger.error(str(exc))
            raise Exception("Failed to connect to output database")

        try:
            # create a SQLView in the output database from the source database
            output_db.create_data_source_sql_view(sql=self.data_source.sql_view_to_create())

            sql = self.data_source.sql_count_data_source(reference_date=reference_date)
            data = db_facade.fetchone(query=sql)

        except Exception as exc:
            db_facade.rollback()
            self.logger.error(
                "Failure on SQLViewDataChecker.__has_deter_in_data_source"
            )
            self.logger.error(str(exc))
            raise Exception("Failed to check data using a SQL View resource via DBLink")
        finally:
            # Remove SQLView from the output database
            output_db.drop_data_source_sql_view(sql=self.data_source.sql_view_to_drop())
            db_facade.close()

        return  data is not None and data[0] and int(data[0]) > 0
