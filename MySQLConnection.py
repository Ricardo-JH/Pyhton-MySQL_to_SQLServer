import mysql.connector as connection
import pandas as pd
import SQLConnection
from SQL_Parameters import *
import warnings
import time


warnings.filterwarnings('ignore')

def load_data():
    try:
        # MySQL Connector
        MySQL_db = connection.connect(host=MySQL['host'], database=MySQL['database'], user=MySQL['user'], passwd=MySQL['password'], use_pure=MySQL['use_pure'])
            
        # MySQL Tables
        query_tables = f"SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = '{MySQL['database']}'"
        tables = pd.read_sql(query_tables, MySQL_db).fillna('')['table_name']
        # exclude = ['ost_team_member', 'ost_staff_dept_access', 'ost_schedule_entry', 'ost_queue_sorts']
        include = ['ost_ticket', 'ost_form_entry', 'ost_form', 'ost_form_entry_values', 'ost_form_field', 'ost_thread', 'ost_thread_collaborator', 'ost_user', 'ost_thread_entry', 'ost_staff', 'ost_ticket_status', 'ost_event', 'ost_department', 'ost_help_topic', 'ost_ticket__cdata', 'ost_ticket_priority', 'ost_user__cdata', 'ost_sla', 'ost_schedule', 'ost_user_email']
        
        for table in tables:
            if table in include:
                
                start_time = time.time()

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
                
                elapsed_time = time.time() - start_time
                print('Elapsed_time: ', elapsed_time)

        MySQL_db.close() #close the connection
    except Exception as e:
        MySQL_db.close()
        print(str(e))