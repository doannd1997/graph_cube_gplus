import os
import sys
import pyodbc
import pandas as pd
import numpy as np
from dotenv import load_dotenv

sys.path.insert(1, '.')
from sql.query.sql import create_connection

load_dotenv('.pyenv')

query_dir = os.environ.get('query_dir')
database = os.environ.get('SQL_DB_NAME')
data_path = os.environ.get('data_path')


def load():
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute('DELETE FROM DBO.user_profile')
    cursor.execute('DELETE FROM DBO.follow')
    
    attr_df = pd.read_csv(os.path.join(data_path, 'extracted', 'attr_indexed_id.csv'), header=0, sep='\t')
    attr_df = attr_df.fillna(np.nan).replace([np.nan], [None])
    edge_df = pd.read_csv(os.path.join(data_path, 'extracted', 'edges_indexed_id.csv'), header=0, sep='\t')
    edge_df = edge_df.fillna(np.nan).replace([np.nan], [None])

    for i, attr in attr_df.iterrows():
        cursor.execute('INSERT INTO DBO.user_profile (indexed_id, gender, job_title, place, university, institution) VALUES (?,?,?,?,?,?)',
            (
                attr.indexed_id,
                attr.gender,
                attr.job_title,
                attr.place,
                attr.university,
                attr.institution
            )
            )
    conn.commit()

    f = 0
    s = 10000
    while True:
        df = edge_df.iloc[f:f+s,:] if f+s <= edge_df.shape[0] else edge_df[f:,:]
        print(f'{f}-{f+s-1}/{edge_df.shape[0]}', flush=True, end='\r')
        cursor.executemany('INSERT INTO DBO.follow (start_id, end_id) VALUES (?,?)',
            [(
                f'{edge.start}',
                f'{edge.end}',
            ) for v, edge in df.iterrows()])
        f = f + s
    
    conn.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    load()