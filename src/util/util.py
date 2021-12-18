from datetime import date, datetime
from typing import Counter
import itertools
from src.graph.graph_cube import find_nearest_common_descendant

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
    for i, d in dim:
        if d == '0':
            dim = d[:i] + '1' + d[i+1:]
            yield dim
        

def get_dim_level(dim):
    return Counter(dim)['1']


def extract_dual_dim(dim):
    [s_dim, e_dim] = dim.split('_')
    src_dim = find_nearest_common_descendant(s_dim, e_dim)
    return s_dim, e_dim, src_dim


def get_dual_table_name(dim):
    [s_dim, e_dim, src_dim] = extract_dual_dim(dim)
    return f'dual_{s_dim}_{e_dim}_base_{src_dim}_edge_aggregated'