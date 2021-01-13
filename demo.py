from mysql2postgresql import mysql2postgresql
from time import time
from math import floor 
import tracemalloc

time1 = time()
tracemalloc.start()

a = mysql2postgresql()
a.connect_mysql(host='localhost',
            port=3306,
            user='root',
            passwd='',
            db='db_name',
            charset='utf8')

a.connect_postgresql(host='localhost',             
                    port=5432,
                    user='postgres',                                
                    password='postgres',                     
                    database='database_name')  

a.tables = [] # Table list name

a.run()

current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
tracemalloc.stop()

diff = time() - time1
hour = floor(diff/3600)
mins = floor((diff/60)-(hour*60))
sec = diff - (mins*60) - (hour*3600)

print("\nfinish")
print(f'use time : {hour} hours. {mins} mins. {sec} sec.')
