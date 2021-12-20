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
from src.util.util import get_dim_alias, get_dim_level

conn, cur = db_con_cur()

dim_info_dual = None
available_thresholds = None

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
        SELECT MIN([entropy_rate])
        FROM [dbo].[internal_dim]
        WHERE [dim] = '{dim}'
    '''

    cur.execute(query)
    result = cur.fetchone()[0]

    return result != None, result


def get_dim_info_dual(dim):
    query = f'''
        SELECT *
        FROM [dim_info_dual]
        WHERE [dim] = '{dim}'
    '''

    internal_computed, min_internal_entropy_rate = is_internal_computed(dim)

    cur.execute(query)
    dim_info = cur.fetchone()
    dim_info = list(dim_info) + [
            get_dim_alias(dim),
            get_dim_level(dim),
            internal_computed,
            min_internal_entropy_rate if min_internal_entropy_rate else 1
        ]
    columns = [column[0] for column in cur.description] + [
            'dim_alias',
            'level',
            'internal_computed',
            'min_internal_entropy_rate'
        ]

    return dict(zip(columns, dim_info))


def get_available_thresholds():
    global available_thresholds
    if not available_thresholds:
        query = f'''
            SELECT DISTINCT [threshold]
            FROM [navigation_threshold]
            ORDER BY [threshold]
        '''
        cur.execute(query)
        available_thresholds = cur.fetchall()
        available_thresholds = [a[0] for a in available_thresholds]

    return available_thresholds


def get_min_internal_entropy_rate(dim):
    query = f'''
        SELECT MIN(entropy_rate)
        FROM [internal_dim]
        WHERE [dim] = '{dim}'
    '''

    cur.execute(query)

# conn.close()