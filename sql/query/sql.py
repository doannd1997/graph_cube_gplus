import os
import pyodbc
from dotenv import load_dotenv

load_dotenv('.pyenv')

query_dir = os.environ.get('query_dir')
server_name = os.environ.get('SQL_SERVER_NAME')
username = os.environ.get('SQL_USERNAME')
password = os.environ.get('SQL_PASSWORD')
database = os.environ.get('SQL_DB_NAME')

def create_connection():
    connect_pattern = 'Driver={driver};Server={server_name};UID={username};PWD={password};Database={database};Trusted_Connection=yes'
    connect_query = connect_pattern.format(
        driver='SQL Server Native Client 11.0',
        server_name=server_name,
        username=username,
        password=password,
        database=database
    )
    conn = pyodbc.connect(connect_query)

    return conn