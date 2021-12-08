import os
import sys
import json
from typing import AsyncGenerator
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(1, '.')
from src.graph.single_coboid import single_cuboid
from sql.query.sql import create_connection, database

load_dotenv('.pyenv')

data_path = os.environ.get('data_path')
property = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))
attrs = property['attrs']
node_path = os.path.join(data_path, 'extracted', 'refined', 'node_attr.csv')
edge_path = os.path.join(data_path, 'extracted', 'edges_indexed_id.csv')

start_flag = datetime.now()

# conn = create_connection()
# # conn.autocommit = False
# cursor = conn.cursor()
# cursor.execute(f'use {database}')

def aggregate_dim(dim, dimensions):
    dim = [f'[{dimensions[i]}]' for i,x in enumerate(dim) if x == '1']
    dim_columns = ','.join(dim)
    dim_aggregated = "CONCAT(" + ',\'.\','.join(dim) + ")" if len(dim) > 1 else f"{dim[0]}"
    return dim_columns, dim_aggregated


def get_single_cuboid_sql(des_dim, src_dim, dimensions):
    # dim = [f'[{dimensions[i]}]' for i,x in enumerate(des_dim) if x == '1']
    # dim_columns = ','.join(dim)
    # dim_aggregated = "CONCAT(" + ',\'.\','.join(dim) + ")" if len(dim) > 1 else f"{dim[0]}"
    dim_columns, dim_aggregated = aggregate_dim(des_dim, dimensions)

    sql_template = open(os.path.join('sql', 'query_file', 'template.sql'), 'r').read()
    sql_template = sql_template\
        .replace(r'%%dim_columns%%', dim_columns)\
        .replace(r'%%dim_aggregated%%', dim_aggregated)\
        .replace(r'%%des_dim%%', des_dim)\
        .replace(r'%%src_dim%%', src_dim)
    
    return sql_template


def construct():
    cuboid_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(1, 2**len(attrs))]
    cuboid_dims.reverse()
    construct_pairs = ''
    for cuboid_dim in cuboid_dims:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute(f'use {database}')

        cursor.execute('SELECT [dim], [v_size], [e_size] FROM [dbo].[dim_info]')
        dim_info = cursor.fetchall()

        if cuboid_dim not in [x[0] for x in dim_info]:
            best_descendant = find_lowest_cost_descendant(cuboid_dim, dim_info)
            best_descendant_dim = best_descendant[0][0]
        
            query = get_single_cuboid_sql(
                cuboid_dim,
                best_descendant_dim,
                attrs
            )

            batched_queries = query.split('GO;')
            for q in batched_queries:
                q = q.strip()
                cursor.execute(q)

            conn.commit()
            cursor.close()
            conn.close()

            with open(os.path.join('sql', 'query_file', f'{cuboid_dim}.sql'), 'w') as f:
                f.write(query)
                f.close()

            construct_pair = f'{cuboid_dim} {best_descendant_dim}'
            construct_pairs = construct_pairs + f'{construct_pair}\n'
            with open(os.path.join(data_path, 'extracted', 'construct_pairs.json'), 'w') as f:
                f.write(construct_pairs)
                f.close()

            print(f'compute cuboid {cuboid_dim} from {best_descendant_dim} success!')


def find_lowest_cost_descendant(cuboid_dim, cuboids):
    descendants = []
    for c in cuboids:
        if is_descendant(cuboid_dim, c[0]):
            size = c[1] + c[2]
            descendant = (c, size)
            descendants.append(descendant)
    descendants.sort(key=lambda x: x[1])
    return descendants[0]

# is dim1 is descendant of d0. eg 11110 is descendant of 10010
def is_descendant(dim0, dim1):
    for i, d in enumerate(dim0):
        if d == '1' and dim1[i] == '0':
            return False
    return True


def get_dual_query_sql(s_dim, e_dim, src_dim, dimensions):
    template_dual = open(os.path.join('sql', 'query_file', 'template_dual.sql'), 'r').read()
    _, s_dim_aggregated = aggregate_dim(s_dim, dimensions)
    _, e_dim_aggregated = aggregate_dim(e_dim, dimensions)
    template_dual = template_dual\
        .replace(r'%%s_dim%%', s_dim)\
        .replace(r'%%e_dim%%', e_dim)\
        .replace(r'%%s_dim_aggregated%%', s_dim_aggregated)\
        .replace(r'%%e_dim_aggregated%%', e_dim_aggregated)\
        .replace(r'%%src_dim%%', src_dim)

    return template_dual


conn = create_connection()
cursor = conn.cursor()
cursor.execute(f'use {database}')

def compute_dual(s_dim, e_dim, src_dim):


    query = get_dual_query_sql(
        s_dim,
        e_dim,
        src_dim,
        attrs
    )

    batched_queries = query.split('GO')
    for q in batched_queries:
        q = q.strip()
        # print(q)
        cursor.execute(q)



    with open(os.path.join('sql', 'query_file', f'dual_{s_dim}_{e_dim}_base_{src_dim}.sql'), 'w') as f:
        f.write(query)
        f.close()


def find_nearest_common_descendant(s_dim, e_dim):
    return bin(int(s_dim, 2)|int(e_dim, 2))[2:].rjust(len(attrs), '0')


def get_time_period():
    global start_flag
    period = datetime.now() - start_flag
    start_flag = datetime.now()
    return period


def construct_dual():
    s_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(2**len(attrs))]
    e_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(2**len(attrs))]

    for s_dim in s_dims:
        for e_dim in e_dims:
            if s_dim == '00000' or e_dim == '00000':
                continue
            
            src_dim = find_nearest_common_descendant(s_dim, e_dim)
            compute_dual(s_dim, e_dim, src_dim)

            period = get_time_period()
            print(f'In {period.total_seconds()}s: compute dual-cuboid {s_dim}_{e_dim} from {src_dim} success!')

    
    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == '__main__':
    sf = datetime.now()
    # construct()
    construct_dual()
    print(f"Finished in: {(datetime.now()-sf).seconds}")