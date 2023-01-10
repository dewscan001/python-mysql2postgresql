
from fire import Fire
from mysql2postgresql import mysql2postgresql
from tqdm import tqdm

class Mysql2PostgresqlMain:
    def convert(self, mysql_host="localhost", mysql_port=3306, mysql_user="root", mysql_password="", mysql_database="db_name", 
                postgresql_host="localhost", postgresql_port=5432, postgresql_user="postgres", postgresql_password="postgres", postgresql_database="database_name"):
        tqdm.write("Mysql Server")
        tqdm.write(f"Mysql host : {mysql_host}")
        tqdm.write(f"Mysql port : {mysql_port}")
        tqdm.write(f"Mysql username : {mysql_user}")
        tqdm.write(f"Mysql database : {mysql_database}")
        tqdm.write("Postgresql Server")
        tqdm.write(f"Postgresql host : {postgresql_host}")
        tqdm.write(f"Postgresql port : {postgresql_port}")
        tqdm.write(f"Postgresql username : {postgresql_user}")
        tqdm.write(f"Postgresql database : {postgresql_database}")

        mysql_postgresql = mysql2postgresql()
        mysql_postgresql.connect_mysql(
            host=mysql_host, 
            port=mysql_port, 
            user=mysql_user, 
            passwd=mysql_password, 
            db=mysql_database,
            charset='utf8')

        mysql_postgresql.connect_postgresql(
            host=postgresql_host, 
            port=postgresql_port,
            user=postgresql_user, 
            password=postgresql_password,
            database=postgresql_database)

        mysql_postgresql.run()

    def export_example(self):
        with open("demo.py", mode="w", encoding="UTF-8") as f:

            f.write("""
# Example

from mysql2postgresql import mysql2postgresql

a = mysql2postgresql()

#connect mysql server
a.connect_mysql(host='localhost',
            port='3306',
            user='root',
            passwd='',
            db='database_name')

#connect postgresql server
a.connect_postgresql(host='localhost',             
                    port=5432,
                    user='postgres',                                
                    password='postgres',                     
                    database='database_name')  


# manual table to transfer data  -> default all table in database
a.tables = ['table1', 'table2', ...]

# manual without table to transfer data  -> default empty
a.without = ['table3', 'table4', ...]

# manual limit to query data -> default 10000 
# not limit -> a.limit = 0 
a.limit = 10000


# program run
a.run()   
        """)

if __name__ == '__main__':

    Fire(Mysql2PostgresqlMain)

    