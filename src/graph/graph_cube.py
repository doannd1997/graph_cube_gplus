import os
import sys
import json
from typing import AsyncGenerator
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(1, '.')
from sql.query.sql import create_connection, db_con_cur

load_dotenv('.pyenv')

data_path = os.environ.get('data_path')
property = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))
attrs = property['attrs']
node_path = os.path.join(data_path, 'extracted', 'refined', 'node_attr.csv')
edge_path = os.path.join(data_path, 'extracted', 'edges_indexed_id.csv')

start_flag = datetime.now()

def aggregate_dim(dim, dimensions):
    dim = [f'[{dimensions[i]}]' for i,x in enumerate(dim) if x == '1']
    dim_columns = ','.join(dim)
    dim_aggregated = "CONCAT(" + ',\'.\','.join(dim) + ")" if len(dim) > 1  \
        else (f"{dim[0]}" if len(dim) > 0 else '')
    return dim_columns, dim_aggregated


def get_single_cuboid_sql(des_dim, src_dim, dimensions):
    dim_columns, dim_aggregated = aggregate_dim(des_dim, dimensions)

    sql_template = open(os.path.join('sql', 'query_file', 'template.sql'), 'r').read()
    sql_template = sql_template\
        .replace(r'%%dim_columns%%', dim_columns)\
        .replace(r'%%dim_aggregated%%', dim_aggregated)\
        .replace(r'%%des_dim%%', des_dim)\
        .replace(r'%%src_dim%%', src_dim)
    
    return sql_template


def construct_single():
    cuboid_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(1, 2**len(attrs))]
    cuboid_dims.reverse()
    construct_pairs = ''

    conn, cursor = db_con_cur()

    for cuboid_dim in cuboid_dims:
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


            with open(os.path.join('sql', 'query_file', 'single', f'{cuboid_dim}.sql'), 'w') as f:
                f.write(query)
                f.close()

            construct_pair = f'{cuboid_dim} {best_descendant_dim}'
            construct_pairs = construct_pairs + f'{construct_pair}\n'
            with open(os.path.join(data_path, 'extracted', 'construct_pairs.json'), 'w') as f:
                f.write(construct_pairs)
                f.close()

            print(f'compute cuboid {cuboid_dim} from {best_descendant_dim} success!')
    
    cursor.close()
    conn.close()

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


def get_dual_template_path(s_dim, e_dim):
    template_file = 'template_dual_null_2_many.sql' \
        if s_dim == '00000' \
        else ('template_dual_many_2_null.sql' if e_dim == '00000' else 'template_dual.sql')
    template_dual = open(os.path.join('sql', 'query_file', template_file), 'r').read()
    return template_dual

def get_dual_query_sql(s_dim, e_dim, src_dim, dimensions):
    template_file = 'template_dual_null_2_many.sql' \
        if s_dim == '00000' \
        else ('template_dual_many_2_null.sql' if e_dim == '00000' else 'template_dual.sql')
    template_dual = open(os.path.join('sql', 'query_file', template_file), 'r').read()
    _, s_dim_aggregated = aggregate_dim(s_dim, dimensions)
    _, e_dim_aggregated = aggregate_dim(e_dim, dimensions)
    template_dual = template_dual\
        .replace(r'%%s_dim%%', s_dim)\
        .replace(r'%%e_dim%%', e_dim)\
        .replace(r'%%s_dim_aggregated%%', s_dim_aggregated)\
        .replace(r'%%e_dim_aggregated%%', e_dim_aggregated)\
        .replace(r'%%src_dim%%', src_dim)

    return template_dual


def compute_dual(cursor, s_dim, e_dim, src_dim):
    query = get_dual_query_sql(
        s_dim,
        e_dim,
        src_dim,
        attrs
    )

    batched_queries = query.split('GO')
    for q in batched_queries:
        q = q.strip()
        cursor.execute(q)

    with open(os.path.join('sql', 'query_file', 'dual_2', f'dual_{s_dim}_{e_dim}_base_{src_dim}.sql'), 'w') as f:
        f.write(query)
        f.close()
    
    cursor.commit()

def find_nearest_common_descendant(s_dim, e_dim):
    return bin(int(s_dim, 2)|int(e_dim, 2))[2:].rjust(len(attrs), '0')


def get_time_period():
    global start_flag
    period = datetime.now() - start_flag
    start_flag = datetime.now()
    return period


def get_computed_dual_dims():
    conn, cursor = db_con_cur()
    
    cursor.execute('SELECT [dim] from [dbo].[dim_info_dual_2]')
    computed_dual_dims = cursor.fetchall()
    computed_dual_dims = [d[0] for d in computed_dual_dims]
    
    return computed_dual_dims


computed_dual_dims = get_computed_dual_dims()

def is_ignore_dual_compute(s_dim, e_dim):
    if s_dim == '00000' and e_dim == '00000':
        return True

    dim = f"{s_dim}_{e_dim}"
    if dim in computed_dual_dims:
        return True


def construct_dual(batch_size=None):
    batch_size = 10 if type(batch_size) != str else int(batch_size)

    conn, cursor = db_con_cur()
    s_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(2**len(attrs))]
    e_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(2**len(attrs))]
    
    log_file = open(os.path.join('.','log', 'compute_dual_ignore.txt'), 'w')

    count = 1
    for s_dim in s_dims:
        for e_dim in e_dims:
            if is_ignore_dual_compute(s_dim, e_dim):
                log_file.write(f"Ignore {s_dim}_{e_dim}\n")
                continue
            
            src_dim = find_nearest_common_descendant(s_dim, e_dim)
            compute_dual(cursor, s_dim, e_dim, src_dim)
            
            period = get_time_period()
            print(f'In {period.total_seconds()}s: [{count}/{batch_size}] compute dual-cuboid {s_dim}_{e_dim} from {src_dim} success!')

            if count == batch_size:
                break
            count = count + 1
        
        if count == batch_size:
            break

    
    conn.commit()
    cursor.close()
    conn.close()

    log_file.close()
    
if __name__ == '__main__':
    sf = datetime.now()
    # construct()
    construct_dual(batch_size=sys.argv[1])
    print(f"Finished in: {(datetime.now()-sf).seconds}")