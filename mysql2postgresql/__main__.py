from fire import Fire
from mysql2postgresql import mysql2postgresql
from tqdm import tqdm
import os

DB_NAME = os.getenv("CONVERT_DATABASE", "database_name")


class Mysql2PostgresqlMain:
    def convert(
        self,
        mysql_host=os.getenv("MYSQL_HOST", "localhost"),
        mysql_port=3306,
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PWD", ""),
        mysql_database=DB_NAME,
        postgresql_host=os.getenv("PGHOST", "localhost"),
        postgresql_port=5432,
        postgresql_user=os.getenv("PGUSER", "postgres"),
        postgresql_password=os.getenv("PGPASSWORD", "postgres"),
        postgresql_database=DB_NAME,
    ):
        tqdm.write("MySQL Server")
        tqdm.write(f"    host: {mysql_host}")
        tqdm.write(f"    port: {mysql_port}")
        tqdm.write(f"    username: {mysql_user}")
        tqdm.write(f"    database: {mysql_database}")
        tqdm.write("PostgreSQL Server")
        tqdm.write(f"    host: {postgresql_host}")
        tqdm.write(f"    port: {postgresql_port}")
        tqdm.write(f"    username: {postgresql_user}")
        tqdm.write(f"    database: {postgresql_database}")

        mysql_postgresql = mysql2postgresql()
        mysql_postgresql.connect_mysql(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            passwd=mysql_password,
            db=mysql_database,
            charset="utf8",
        )

        mysql_postgresql.connect_postgresql(
            host=postgresql_host,
            port=postgresql_port,
            user=postgresql_user,
            password=postgresql_password,
            database=postgresql_database,
        )

        mysql_postgresql.run()

    def export_example(self):
        with open("demo.py", mode="w", encoding="UTF-8") as f:

            f.write(
                """
# Example
from mysql2postgresql import mysql2postgresql

a = mysql2postgresql()

# connect MySQL server
a.connect_mysql(
    host='localhost',
    port='3306',
    user='root',
    passwd='',
    db='database_name'
)

# connect PostgreSQL server
a.connect_postgresql(
    host='localhost',             
    port=5432,
    user='postgres',                                
    password='postgres',                     
    database='database_name'
)  


# shortlist of tables to copy data from -> default all tables in a database
a.tables = ['table1', 'table2', ...]

# shortlist of tables to exclude  -> default empty
a.without = ['table3', 'table4', ...]

# manual limit to query data -> default 10000 
# not limit -> a.limit = 0 
a.limit = 10000


# program run
a.run()   
        """
            )


if __name__ == "__main__":
    Fire(Mysql2PostgresqlMain)
