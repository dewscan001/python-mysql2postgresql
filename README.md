# python-mysql2postgresql

## Status : Pre-Alpha

## Installation -- Pure Python
```
    pip install mysql2postgresql
```

## Usage

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

# manual limit to query data -> default 10000 
# not limit -> a.limit = 0 
a.limit = 10000


# program run
a.run()   

```


