
import mysql.connector
import psycopg2
import psycopg2.extras
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


class mysql2postgresql:
    def __init__(self):
        self.db = None
        self.dbx = None
        self.DB = None
        self.DBX = None
        self.tables:list = []
        self.database:str = None
        self.limit:int = 10000
        self.connect_mysql_kwargs:dict = {}
        self.connectpostgresql_kwargs:dict = {}
    
    def connect_mysql(self, **kwargs):
        self.connect_mysql_kwargs = kwargs
        
    def connect_mysql_database(self):
        # tqdm.write('connecting mysql server ...')
        self.db = mysql.connector.connect(**self.connect_mysql_kwargs)
        self.dbx = self.db.cursor()
        self.database = self.connect_mysql_kwargs['db']
        
    def close_mysql(self):
        if self.db or self.dbx:
            # tqdm.write('close connect mysql ...')
            self.dbx.close()
            self.db.close()
    
    ######################################################################################################
    
    def connect_postgresql(self, **kwargs):
        self.connectpostgresql_kwargs = kwargs
        
    def connect_postgresql_database(self):
        # tqdm.write('connecting postgresql server ...')
        self.DB = psycopg2.connect(**self.connectpostgresql_kwargs)
        self.DB.autocommit = True
        self.DBX = self.DB.cursor() 
        self.DBX.execute("set client_encoding = 'utf8'")
        
    def close_postgresql(self):
        if self.DB or self.DBX:
            # tqdm.write('close connect postgresql ...')
            self.DBX.close()
            self.DB.close()
    
    
    #-------------------------------------------------------------------#
    def setval(self, table:str, serial_name:str):
        '''
            function setval to sequnce (seq) in PostgreSQL from Last ID
            Setdefault value for sequence when insert new data
        '''

        psql:str = f"SELECT {serial_name} FROM {table} ORDER BY {serial_name} DESC LIMIT 1"
        tqdm.write(psql)
        self.DBX.execute(psql)
        try:
            id:int = self.DBX.fetchone()[0]
            psql = f"SELECT SETVAL('{table[0:56]}_{serial_name}_seq', {id})"
            self.DBX.execute(psql)
            # self.DB.commit()
        except:
            pass
                
                
    def insertinto(self, rows:list, table:str) -> None:
        '''
            Function insert data to PostgreSQL from function selecttoinsert 
        '''
        
        psql:str = f"INSERT INTO {table} values %s"
        tqdm.write(psql)
        psycopg2.extras.execute_values(self.DBX, psql, rows)
        # self.DB.commit()
        
    #-------------------------------- function select mysql ---------------------------------------#
    def selecttoinsert(self, table:str) -> None:
        '''
            Function Select data from MySQL to function insertinto 
        '''
        
        step:int = 0 
        msql:str = f'SELECT COUNT(*) FROM {table}'
        self.dbx.execute(msql)
        count:int = self.dbx.fetchone()[0]
        
        if self.limit == 0:
            self.limit = count 
        
        while count > 0:
            msql = f'SELECT * from {table} LIMIT {self.limit} OFFSET {step};'
            tqdm.write(msql)
            self.dbx.execute(msql)
            rows = self.dbx.fetchall()
            
            with ThreadPoolExecutor() as executor:
                executor.submit(self.insertinto , rows, table)
            
            step = step + self.limit
            count = count - self.limit
            

    def create_sequence(self, table:str, name:str):
        '''
            function create sequnce (seq) in PostgreSQL
            create sequence by tablename_primarykey_seq
        '''

        psql:str = f"DROP SEQUENCE {table[0:56]}_{name}_seq CASCADE"
        try:
            self.DBX.execute(psql)
        except:
            pass
        # self.DB.commit()
            
        psql = f"CREATE SEQUENCE {table[0:56]}_{name}_seq"
        self.DBX.execute(psql)
        # self.DB.commit()


    def main(self) -> None:
       
        ''' show column from MySQL to create table in PostgreSQL '''
        
        if len(self.tables) == 0:
            self.connect_mysql_database()
            mysql= f'show tables from {self.database}'
            tqdm.write(mysql)
            self.dbx.execute(mysql)
            self.tables:list = [table_name[0] for table_name in self.dbx.fetchall()]
            self.close_mysql()
        
        for table in tqdm(self.tables):
            
            self.connect_mysql_database()
            self.connect_postgresql_database()
            
            primary:list = []
            serial_names:str = ''
            primary_key:str = ''
            
            drop_psql:str = f'DROP TABLE IF EXISTS {table}'
            try:
                self.DBX.execute(drop_psql)
            except Exception as e:
                tqdm.write(str(e))

        
            mysql:str = f'SHOW COLUMNS FROM {table}'
            self.dbx.execute(mysql)
            rows:generator = (value for value in self.dbx.fetchall())

            '''
            create table in PostgreSQL
            '''
            
            psql:str = f'CREATE TABLE IF NOT EXISTS {table} ('

            for row in rows:
                
                name:str=row[0]; typed:str=row[1]; null:str=row[2]; key:str=row[3]; default:str=row[4]; extra:str=row[5]
                
                '''
                    this change data type from MySQL to PostgreSQL
                '''
                if 'int' in typed: typed='int'
                elif 'tinyint' in typed: typed='int4'
                elif 'bigint' in typed: typed='int8'
                elif 'blob' in typed: typed='bytea'
                elif 'datetime' in typed: typed='timestamp without time zone'
                elif 'date' in typed: typed='date'
                elif 'text' in typed: typed='text'
                elif 'varchar' in typed: typed='character varying'
                elif 'double' in typed: typed='double precision'
                elif 'enum' in typed: typed='character varying'     
                    
                if key == 'PRI':
                    ''' when column is primary it append to list'''
                    primary.append(name)

                if extra == "auto_increment":
                    ''' when column is auto_increment'''

                    serial_names = name
                    self.create_sequence(table, name)
                    default = f"DEFAULT nextval('{table[0:56]}_{name}_seq'::regclass)"
                    psql+= f'{name} {typed} {default},'
                    
                else:
                    ''' when column is not auto_increment'''
                    if default is not None:
                        default = default.strip("()")
                        if typed == 'date' :
                            default = f"DEFAULT DATE('{default}')"
                        elif typed == 'timestamp' or default == 'NULL' or default.startswith("'"):
                            default = f'DEFAULT {default}'
                        else:
                            default = f"DEFAULT '{default}'"
                        psql+= f'{name} {typed} {default},'
                    else:
                        psql+= f'{name} {typed},'

            if len(primary) != 0:
                primary_key = ', '.join(primary)

            if primary_key != '':
                ''' add primary key from list '''
                psql+= f'PRIMARY KEY ({primary_key})'

            create_psql:str=psql.strip(',')+')'

            tqdm.write(create_psql)
            
            self.DBX.execute(create_psql)
            # self.DB.commit()
            
            self.selecttoinsert(table)
            if len(serial_names) > 0:
                self.setval(table, serial_names)
                
            self.close_mysql()
            self.close_postgresql()
                
        
    def run(self):
        tqdm.write('Running in Python version')
        try:
            self.main()
        except Exception as e:
            tqdm.write(str(e))
        finally:
            self.close_postgresql()
            self.close_mysql()
            
            

if __name__ == '__main__':
    print('hello')
        