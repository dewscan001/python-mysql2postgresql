# python-mysql2postgresql

## Status : Alpha

## Installation -- Pure Python
```
    pip install python-mysql2postgresql
```

## Usage Python file

```
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

```


## Usage command line

you can create demo.py file from example by
```
python -m mysql2postgresql export_example
```

you can run convert mysql to postgresql (all table in database) by 
```
python -m mysql2postgresql convert --mysql_host=localhost --mysql_port=3306 --mysql_user=root --mysql_password= --mysql_database=db_name --postgresql_host=localhost --postgresql_port=5432 --postgresql_user=postgres --postgresql_password=postgres --postgresql_database=database_name
```

