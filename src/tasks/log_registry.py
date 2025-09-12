from tasks.output_database import OutputDatabase
from utils.logger import TasksLogger


class LogRegistry:
    """To handler the control log registers."""

    def __init__(self, log_level: str):
        self.logger = TasksLogger(self.__class__.__name__)
        self.logger.setLoggerLevel(log_level)
        db = OutputDatabase(log_level=log_level)
        self.db_facade = db.get_database_facade()

    def write(self, description: str = "Missing description", success: bool = False):
        """To write one registry on control log table"""

        sql = f"""INSERT INTO public.collector_log(description, success)
        VALUES ( '{description}', '{('1' if success else '0')}'::boolean );
        """
        self.db_facade.execute(sql=sql, logger=self.logger)
        self.db_facade.commit()

    def read(self, filter_by_date: str = None, filter_by_status: str = "true"): # type: ignore
        """To read a most recent record from the control log table"""

        filter_by_date = (
            f"AND processed_on='{filter_by_date}'::date"
            if filter_by_date is not None
            else ""
        )
        filter_by_status = (
            f"AND success='{filter_by_status}'::boolean"
            if filter_by_status == "true" or filter_by_status == "false"
            else ""
        )

        sql = f"""SELECT processed_on, description, success
        FROM public.collector_log
        WHERE description IS NOT NULL
        {filter_by_status}
        {filter_by_date}
        ORDER BY processed_on DESC LIMIT 1;
        """

        data = self.db_facade.fetchone(query=sql, logger=self.logger)

        if data is not None and data[0]:
            data = data[0]

        return data
