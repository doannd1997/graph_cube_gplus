import os
import sys
import math
import json
import getopt
import itertools

from dotenv import load_dotenv
from networkx.generators import directed
load_dotenv('.pyenv')

sys.path.insert(1, '.')

from sql.query.sql import db_con_cur
from src.util.util import get_dim_alias

conn, cur = db_con_cur()

dim_info_dual = None

def get_dim_dual_external_entropy(dim):
    global dim_info_dual
    if not dim_info_dual:
        query_dim_info_dual = '''
        SELECT *
        FROM [dbo].[dim_info_dual]
        '''
        dim_info_dual = cur.execute(query_dim_info_dual).fetchall()

    for d in dim_info_dual:
        if d[0] == dim:
            return d[5]
    return None


def is_internal_computed(dim):
    query = f'''
        SELECT *
        FROM [dbo].[internal_dim]
        WHERE [dim] = '{dim}'
    '''

    cur.execute(query)
    result = cur.fetchone()

    return result != None


def get_dim_info_dual(dim):
    query = f'''
        SELECT *
        FROM [dim_info_dual]
        WHERE [dim] = '{dim}'
    '''

    internal_computed = is_internal_computed(dim)

    cur.execute(query)
    dim_info = cur.fetchone()
    dim_info = list(dim_info) + [get_dim_alias(dim), internal_computed]
    columns = [column[0] for column in cur.description] + ['dim_alias', 'internal_computed']

    return dict(zip(columns, dim_info))

# conn.close()