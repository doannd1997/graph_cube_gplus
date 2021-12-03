import os
import sys
import json
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

# conn = create_connection()
# # conn.autocommit = False
# cursor = conn.cursor()
# cursor.execute(f'use {database}')

def find_net(dim, dimensions):
    dim_alias = [d  for x, d in enumerate(dimensions) if dim[x] == 1]
    if 0 not in dim:
        nodes = pd.read_csv(node_path, sep='\t', dtype={x:int for x in dim_alias})
        edges = pd.read_csv(
            edge_path,
            sep='\t',
            dtype={
                'start': int,
                'end': int,
                })
        nodes['weight'] = 1
        edges['weight'] = 1
    else:
        nodes = pd.DataFrame(data=[], columns=dim_alias+['weight'])
        edges = pd.DataFrame(data=[], columns=['start', 'end', 'weight'])
    return (nodes, edges, dim_alias)

class graph_cube:
    def __init__(self, dimensions) -> None:
        self.dimensions = dimensions
        self.populate_cuboids()

    def populate_cuboids(self):
        cuboid_dims = [bin(x)[2:].rjust(len(attrs), '0') for x in range(2**len(attrs))]
        cuboid_dims.reverse()
        for d in cuboid_dims:
            d = [int(x) for x in list(d)]
            nodes, edges, dim_alias = find_net(d, self.dimensions)
            cuboid = single_cuboid(
                dimensions=self.dimensions,
                dim=d,
                dim_alias=dim_alias,
                nodes=nodes,
                edges=edges
            )
            break
            

def get_single_cuboid_sql(des_dim, src_dim, dimenstions):
    dim = [f'[{dimenstions[i]}]' for i,x in enumerate(des_dim) if x == '1']
    dim_columns = ','.join(dim)
    dim_aggregated = "CONCAT(" + ',\'.\','.join(dim) + ")" if len(dim) > 1 else f"{dim[0]}"

    sql_template = open(os.path.join('sql', 'query_file', 'template.sql'), 'r').read()
    sql_template = sql_template\
        .replace(r'%%dim_columns%%', dim_columns)\
        .replace('%%dim_aggregated%%', dim_aggregated)\
        .replace('%%des_dim%%', des_dim)\
        .replace('%%src_dim%%', src_dim)
    
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

            print(f'build cuboid {cuboid_dim} from {best_descendant_dim} success!')

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

if __name__ == '__main__':
    sf = datetime.now()
    construct()
    print(f"Finished in: {(datetime.now()-sf).seconds}")