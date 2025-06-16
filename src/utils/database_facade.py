"""Database utilities."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlparse

from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

from utils.logger import TasksLogger


def get_connection_components(db_url: str):
    """
    Use the template to allow correct parsing by urlparse:
    postgresql://postgres:postgres@localhost/postgres
    """
    parsed_url = urlparse(db_url)

    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port
    db_name = parsed_url.path[1:]
    return user, password, host, str(port), db_name


class DatabaseFacade(BaseModel):
    """Database facade."""

    user: str
    password: str
    host: str
    port: str
    db_name: str
    _conn: Optional[connection] = None

    @classmethod
    def from_url(cls, db_url: str) -> "DatabaseFacade":
        user, password, host, port, db_name = get_connection_components(db_url=db_url)
        return cls(
            user=user,
            password=password,
            host=host,
            port=port,
            db_name=db_name,
        )

    @property
    def db_url(self):
        """Database url."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
    
    @property
    def sqlalchemy_url(self):
        """SQLAlchemy url."""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @property
    def dblink(self):
        """Database DBLink url."""
        return f"hostaddr={self.host} port={self.port} dbname={self.db_name} user={self.user} password={self.password}"

    @property
    def conn(self):
        """Database connection."""
        if self._conn is None:
            self._conn = connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=self.db_name
            )
            self._conn.set_isolation_level(1)
            assert self._conn.status == 1
        return self._conn

    def close(self):
        """Close Database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def commit(self):
        """Commit the current transaction."""

        self.conn.commit()

    def rollback(self):
        """Rollback the current transaction."""

        self.conn.rollback()

    def execute(self, sql: str, logger: TasksLogger = None):
        """Execute a sql string."""
        if logger:
            logger.debug(" ".join(sql.split()))

        cursor = self.conn.cursor()

        cursor.execute(sql)

        rowcount = cursor.rowcount

        return rowcount

    def create_schema(self, name: str, comment: str = "", force_recreate: bool = False):
        """Create a schema."""
        sql = ""

        if force_recreate:
            sql += f"DROP SCHEMA IF EXISTS {name} CASCADE;"

        sql += f"CREATE SCHEMA IF NOT EXISTS {name} AUTHORIZATION {self.user};"

        if comment:
            sql += f"COMMENT ON SCHEMA {name} IS {comment};"

        sql += f"GRANT ALL ON SCHEMA {name} TO {self.user};"

        self.execute(sql)

    def create_table(
        self, schema: str, name: str, columns: list, force_recreate: bool = False
    ):
        """Create a database table."""
        table = f'{schema}."{name}"'

        sql = ""

        if force_recreate:
            sql += f"DROP TABLE IF EXISTS {table};"

        sql += f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                {", ".join(columns)}
            )
        """

        self.execute(sql)

    def create_index(
        self,
        schema: str,
        name: str,
        table: str,
        method: str,
        column: str,
        force_recreate: bool = False,
    ):
        """Create an index."""
        sql = ""

        index = f"{schema}.{name}"
        table = f"{schema}.{table}"

        if force_recreate:
            sql += f"DROP INDEX IF EXISTS {index};"

        sql += f"""
            CREATE INDEX IF NOT EXISTS {name}
            ON {table} USING {method}
            ({column});
        """
        self.execute(sql)

    def create_indexes(
        self, schema: str, name: str, columns: tuple, force_recreate: bool
    ):
        """Create an index for each column."""
        for _ in columns:
            col, method = _.split(":")
            self.create_index(
                schema=schema,
                name=f"{name}_{col.replace(',', '_')}_idx",
                table=name,
                method=method,
                column=col,
                force_recreate=force_recreate,
            )

    def fetchall(self, query, logger: TasksLogger = None):
        if logger:
            logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def fetchone(self, query, logger: TasksLogger = None):
        if logger:
            logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchone()
        cursor.close()
        return data

    def fetchfirst(self, query, logger: TasksLogger = None):
        if logger:
            logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchone()
        cursor.close()
        return data

    def insert(self, query: str, data: Any, logger: TasksLogger = None):
        if logger:
            logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.executemany(query, data)

        cursor.close()

    def truncate(self, table: str, cascade: bool = False):
        _cascade = "CASCADE" if cascade else ""
        sql = f"TRUNCATE {table} {_cascade};"
        self.execute(sql=sql)

    def copy_table(self, src: str, dst: str):
        self.execute(f"INSERT INTO {dst} SELECT * FROM {src}")

    def drop_table(self, table: str, cascade: bool = False):
        _cascade = "CASCADE" if cascade else ""
        self.execute(f"DROP TABLE IF EXISTS {table} {_cascade};")

    def create_postgis_extension(self):
        self.execute("CREATE EXTENSION IF NOT EXISTS POSTGIS")

    def create_dblink_extension(self):
        self.execute("CREATE EXTENSION IF NOT EXISTS dblink")

    def count_rows(self, table: str, conditions: str = ""):
        where = ""
        if conditions:
            where = f"WHERE {conditions}"

        query = f"""
            SELECT COUNT(*)
            FROM {table}
            {where};
        """

        return self.fetchone(query=query)

    def table_exist(self, table: str, schema: str):
        query = f"""
            SELECT EXISTS (
	            SELECT 1 
	            FROM information_schema.tables 
	            WHERE table_schema = '{schema}' 
	            AND table_name = '{table}'
            );
        """
        return self.fetchone(query=query)

    def get_dblink_remote_table_columns(self, schema_name, table_name):

        # sql = "	 select column_name, data_type, character_maximum_length, column_default, is_nullable"
        # sql += " from INFORMATION_SCHEMA.COLUMNS where table_name = '{0}';".format(table_name)

        sql = "SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type"
        sql += " FROM pg_attribute a"
        sql += " JOIN pg_class b ON (a.attrelid = b.oid)"
        sql += " JOIN pg_catalog.pg_namespace AS ns ON (ns.oid = b.relnamespace)"
        sql += (
            " WHERE b.relname = '{1}' and a.attstattarget = -1 and ns.nspname = '{0}';"
        )

        sql = sql.format(schema_name, table_name)

        print(sql)

        columns = self.fetchall(sql)

        remote_table_columns = ""

        for column in columns:
            column_name = column[0]
            column_type = column[1]

            if remote_table_columns != "":
                remote_table_columns += ","

            remote_table_columns += " {0} {1} ".format(column_name, column_type)

        return remote_table_columns
