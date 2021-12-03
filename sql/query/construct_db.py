import os
import pyodbc
from dotenv import load_dotenv

from sql import create_connection
load_dotenv('.pyenv')

query_dir = os.environ.get('query_dir')
database = os.environ.get('SQL_DB_NAME')



def construct_db():
    conn = create_connection()
    cursor = conn.cursor()

    query_file = open(os.path.join(query_dir, f'construct_{database}.sql'), 'r')
    cursor.execute(query_file.read())
    cursor.close()
    conn.close()

if __name__ == '__main__':
    construct_db()