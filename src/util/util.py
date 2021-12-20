from datetime import date, datetime
from typing import Counter
import itertools
import json
import os
from src.graph.graph_cube import find_nearest_common_descendant

from dotenv import load_dotenv
load_dotenv('.pyenv')

data_path = os.environ.get('data_path')
property = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))
attrs = property['attrs']

s_flag = datetime.now()

def check_point():
    global s_flag
    now = datetime.now()
    period = (now - s_flag).total_seconds()
    s_flag = now
    return period


def kbits(n, k):
    result = []
    for bits in itertools.combinations(range(n), k):
        s = ['0'] * n
        for bit in bits:
            s[bit] = '1'
        result.append(''.join(s))
    return result


def get_children_dims(dim):
    for i, d in enumerate(dim):
        if d == '0':
            _dim = dim[:i] + '1' + dim[i+1:]
            yield _dim
        

def get_dim_level(dim):
    return Counter(dim)['1']


def extract_dual_dim(dim):
    [s_dim, e_dim] = dim.split('_')
    src_dim = find_nearest_common_descendant(s_dim, e_dim)
    return s_dim, e_dim, src_dim


def get_dual_table_name(dim):
    [s_dim, e_dim, src_dim] = extract_dual_dim(dim)
    return f'dual_{s_dim}_{e_dim}_base_{src_dim}_edge_aggregated'


def parse_dim_alias(single_dim):
    if get_dim_level(single_dim) == 0:
        return '(*)'
    return f'''({','.join([attrs[i] for i, d in enumerate(single_dim) if d == '1'])})'''


def get_dim_alias(dim):
    return ' - '.join([parse_dim_alias(d) for d in dim.split('_')]).upper()


if __name__ == '__main__':
    print(get_children_dims('10100_01011'))