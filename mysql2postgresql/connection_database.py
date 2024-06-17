import mysql.connector
import psycopg2
import psycopg2.extras
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


class mysql2postgresql:
    __slots__ = [
        "db",
        "dbx",
        "DB",
        "DBX",
        "tables",
        "without",
        "database",
        "limit",
        "connect_mysql_kwargs",
        "connectpostgresql_kwargs",
    ]

    def __init__(self):
        self.db = None
        self.dbx = None
        self.DB = None
        self.DBX = None
        self.tables: list = []
        self.without: list = []
        self.database: str = None
        self.limit: int = 10000
        self.connect_mysql_kwargs: dict = {}
        self.connectpostgresql_kwargs: dict = {}

    def connect_mysql(self, **kwargs):
        tqdm.write(f'Connecting MySQL host: {kwargs["host"]}')
        self.connect_mysql_kwargs = kwargs

    def connect_mysql_database(self):
        self.db = mysql.connector.connect(**self.connect_mysql_kwargs)
        self.dbx = self.db.cursor(buffered=True)
        self.database = self.connect_mysql_kwargs["db"]

    def close_mysql(self):
        if self.db or self.dbx:
            # tqdm.write('close connect mysql ...')
            self.dbx.close()
            self.db.close()

    ###############################################################

    def connect_postgresql(self, **kwargs):
        tqdm.write(f'Connecting PostgreSQL host: {kwargs["host"]}')
        self.connectpostgresql_kwargs = kwargs

    def connect_postgresql_database(self):
        self.DB = psycopg2.connect(**self.connectpostgresql_kwargs)
        self.DB.autocommit = True
        self.DBX = self.DB.cursor()
        self.DBX.execute("set client_encoding = 'utf8'")

    def close_postgresql(self):
        if self.DB or self.DBX:
            # tqdm.write('close connect postgresql ...')
            self.DBX.close()
            self.DB.close()

    # -------------------------------------------------------------

    def run(self):
        tqdm.write(f"Running in Python version: {sys.version}")
        try:
            self.main()
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
        except Exception as e:
            tqdm.write(str(e))
        finally:
            print("Closing connections.")
            self.close_postgresql()
            self.close_mysql()

    # -------------------------------------------------------------
    def main(self):
        """Show tables from MySQL to copy to PostgreSQL"""

        if len(self.tables) == 0:
            self.connect_mysql_database()
            mysql = f"SHOW TABLES FROM {self.database}"
            tqdm.write('\n'+mysql)
            self.dbx.execute(mysql)
            self.tables: list = [table_name[0] for table_name in self.dbx.fetchall()]
            self.close_mysql()

        self.tables = [table for table in self.tables if table not in self.without]
        inc = "\n    - ".join(table for table in self.tables)
        exc = "\n    - ".join(table for table in self.without) or 'no tables'
        tqdm.write("Copying tables: \n    - " + inc)
        tqdm.write("Exclude tables: \n    - " + exc)
        tqdm.write("\n")

        tqdm.write(", ".join(func for func in map(self.create_table, tqdm(self.tables))))

    # --------------------------------------------------------------

    def create_table(self, table):
        self.connect_mysql_database()
        self.connect_postgresql_database()

        tqdm.write("\n" + table)
        tqdm.write('-' * len(table))

        primary: list = []
        serial_names: str = ""
        primary_key: str = ""

        drop_psql: str = f"DROP TABLE IF EXISTS {table} CASCADE"
        try:
            self.DBX.execute(drop_psql)
        except Exception as e:
            tqdm.write(str(e))

        mysql: str = f"SHOW COLUMNS FROM {table}"
        self.dbx.execute(mysql)
        rows: generator = (value for value in self.dbx.fetchall())

        """Create table in PostgreSQL"""

        psql: str = f"CREATE TABLE IF NOT EXISTS {table} ("

        for row in rows:

            name: str = row[0]
            typed: str = row[1]
            null: str = row[2]
            key: str = row[3]
            default: str = row[4]
            extra: str = row[5]

            # reserve: tuple = ()

            """This changes data type from MySQL to PostgreSQL"""
            if "int" in typed:
                typed = "int"
            elif "tinyint" in typed:
                typed = "int4"
            elif "bigint" in typed:
                typed = "int8"
            elif "blob" in typed:
                typed = "bytea"
            elif "datetime" in typed:
                typed = "timestamp without time zone"
            elif "date" in typed:
                typed = "date"
            elif "text" in typed:
                typed = "text"
            # This would drop VARCHAR lengths
            # elif "varchar" in typed:
            #     typed = "character varying"
            elif "double" in typed:
                typed = "double precision"
            elif "enum" in typed:
                typed = "character varying"

            if key == "PRI":
                """When column is primary append it to list"""
                primary.append(name)

            if extra == "auto_increment":
                """When column is auto_increment"""

                serial_names = name
                self.create_sequence(table, name)
                # this could be: typed = 'BIGSERIAL', default = ''
                default = f"DEFAULT nextval('{table[0:56]}_{name}_seq'::regclass)"
                psql += f"\n    {name} {typed} {default},"

            else:
                """When column is not auto_increment"""
                if default is not None:
                    default = default.strip("()")
                    if typed == "date":
                        default = f"DEFAULT DATE('{default}')"
                    elif (
                        typed == "timestamp"
                        or default == "NULL"
                        or default.startswith("'")
                    ):
                        default = f"DEFAULT {default}"
                    else:
                        default = f"DEFAULT '{default}'"
                    psql += f'\n    "{name}" {typed} {default},'
                else:
                    psql += f'\n    "{name}" {typed},'

        if len(primary) != 0:
            primary_key = "\n    , ".join(primary)

        if primary_key != "":
            """add primary key from list"""
            psql += f"\n    PRIMARY KEY ({primary_key})"

        # TODO: other keys and constrains
        create_psql: str = psql.strip(",") + ")"

        tqdm.write(create_psql)
        self.DBX.execute(create_psql)

        self.select_to_insert(table)

        if len(serial_names) > 0:
            self.setval(table, serial_names)

        self.close_mysql()
        self.close_postgresql()

        return table

    def create_sequence(self, table: str, name: str):
        """
        Function to create sequence (seq) in PostgreSQL
        create sequence by tablename_primarykey_seq
        """

        psql: list = [
            f"DROP SEQUENCE {table[0:56]}_{name}_seq CASCADE",
            f"CREATE SEQUENCE {table[0:56]}_{name}_seq",
        ]

        for psql in psql:
            try:
                self.DBX.execute(psql)
            except:
                pass

    def select_to_insert(self, table: str):
        """
        Function to select data from MySQL and insert into PostgreSQL
        """

        msql: str = f"SELECT COUNT(*) FROM {table}"
        self.dbx.execute(msql)
        count: int = self.dbx.fetchone()[0]

        msql: str = f"SELECT * from {table};"
        tqdm.write(msql)
        self.dbx.execute(msql)

        if self.limit > count:
            self.insert_postgresql(iter(self.dbx.fetchall()), table)

        else:
            while count > 0:
                self.insert_postgresql(iter(self.dbx.fetchmany(self.limit)), table)
                count = count - self.limit

    def insert_postgresql(self, rows, table):
        psql: str = f"INSERT INTO {table} values %s"
        tqdm.write(psql)
        with ThreadPoolExecutor() as executor:
            executor.submit(psycopg2.extras.execute_values, self.DBX, psql, rows)

    def setval(self, table: str, serial_name: str):
        """
        Function setval to sequence (seq) in PostgreSQL from Last ID
        Set default value for sequence when insert new data
        """

        psql: str = (
            f"SELECT {serial_name} FROM {table} ORDER BY {serial_name} DESC LIMIT 1"
        )
        tqdm.write(psql)
        self.DBX.execute(psql)
        try:
            id_: int = self.DBX.fetchone()[0]
            psql = f"SELECT SETVAL('{table[0:56]}_{serial_name}_seq', {id_})"
            self.DBX.execute(psql)
            # self.DB.commit()
        except:
            pass
