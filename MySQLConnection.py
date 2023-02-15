import mysql.connector as connection
import pandas as pd
import SQLConnection
from SQL_Parameters import *
import warnings

warnings.filterwarnings('ignore')

def load_data():
    try:
        # MySQL Connector
        MySQL_db = connection.connect(host=MySQL['host'], database=MySQL['database'], user=MySQL['user'], passwd=MySQL['password'], use_pure=MySQL['use_pure'])
            
        # MySQL Tables
        query_tables = f"SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = '{MySQL['database']}'"
        tables = pd.read_sql(query_tables, MySQL_db).fillna('')['table_name']
        exclude = ['ost_team_member', 'ost_staff_dept_access', 'ost_schedule_entry', 'ost_queue_sorts']
        
        for table in tables:
            if table not in exclude:
                print(f'\n{table}')
                try:
                    # Get MySQL data
                    query = f"Select * from {table};"
                    df_MySQL = pd.read_sql(query, MySQL_db).fillna('')
                    df_MySQL = pd.DataFrame(df_MySQL)
                    
                    # Insert from MySQL to SQLServer
                    SQLServer_table = f'[ggapeople1st_support].[{table}]'

                    SQLConnection.truncate(SQLServer_table)
                    SQLConnection.insert(df_MySQL, SQLServer_table)
                
                except Exception as e:
                    print(f'Exception has occurred on table {SQLServer_table}')
                    print(e)

        MySQL_db.close() #close the connection
    except Exception as e:
        MySQL_db.close()
        print(str(e))