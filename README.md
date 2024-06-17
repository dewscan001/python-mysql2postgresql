# python-mysql2postgresql

## Installation - pure Python
```
pip install python-mysql2postgresql
```

## Usage via Python file

```
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

# run the program
a.run()   

```


## Usage via command line

You can copy example above or create a demo.py file from example using:
```
python -m mysql2postgresql export_example
```

You can convert all tables in a MySQL database to PostgreSQL using a command:
```
python -m mysql2postgresql convert\
    --mysql_host=localhost\
    --mysql_port=3306\
    --mysql_user=root\
    --mysql_password=\
    --mysql_database=db_name\
    --postgresql_host=localhost\
    --postgresql_port=5432\
    --postgresql_user=postgres\
    --postgresql_password=postgres\
    --postgresql_database=database_name
```

