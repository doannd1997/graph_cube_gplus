from ntpath import join
import os
import sys
import math
import json
import getopt
import pandas as pd


from dotenv import load_dotenv
from networkx.generators import directed
load_dotenv('.pyenv')

sys.path.insert(1, '.')

from sql.query.sql import db_con_cur
from src.graph.visualize import GraphVisualization
from src.graph.graph_cube import find_nearest_common_descendant, aggregate_dim, attrs, is_descendant
from src.graph.dim_info import get_dim_dual_external_entropy, get_dim_info_dual, is_internal_computed
from src.util.util import get_children_dims, get_dim_level, kbits, extract_dual_dim, get_dual_table_name

data_path = os.environ.get('data_path')
property = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))
attrs = property['attrs']

conn, cursor = db_con_cur()

dim_unique_value = None
with open(os.path.join('sql', 'query_file', 'dim_unique_value.sql'), 'r') as f:
    cursor.execute(f.read())
    dim_unique_value = cursor.fetchone()


def get_expanded_dim(ascendant_dim, descendant_dim):
    al = ascendant_dim.split('_')[0]
    ar = ascendant_dim.split('_')[1]
    dl = descendant_dim.split('_')[0]
    dr = descendant_dim.split('_')[1]
    dim = int(dl, 2) - int(al, 2) + int(dr, 2) - int(ar, 2)
    dim = int(math.log(dim, 2))

    return len(dim_unique_value) - dim - 1, 's' if al != dl else 'e'


def get_dim_unique_value(dim):
    return dim_unique_value[dim]


class lattice():
    def __init__(self, root_dim):
        self.root_dim = root_dim
        self.navigation = list()
        self.computed_dim = dict()
        self.populate(self.root_dim)
        self.insert_2_db()


    def populate(self, ascendant_dim):
        if self.is_computed_dim(ascendant_dim):
            return
        self.computed_dim[ascendant_dim] = True

        for i, d_extra in enumerate(ascendant_dim):
            if d_extra == '0':
                dim = ascendant_dim[:i] + '1' + ascendant_dim[i+1:]
                expanded_dim, direction = get_expanded_dim(ascendant_dim, dim)
                dim_unique_value = get_dim_unique_value(expanded_dim)
                self.navigation.append((ascendant_dim, dim, f'{attrs[expanded_dim]}_{direction}', dim_unique_value))
                self.populate(dim)


    def is_computed_dim(self, dim):
        return dim in self.computed_dim.keys()

    def insert_2_db(self):
        delete_query = """
            DELETE FROM [dbo].[navigation_unique_value]
        """
        cursor.execute(delete_query)
        cursor.commit()

        template_query = """
        INSERT INTO [dbo].[navigation_unique_value] ([ascendant_dim], [descendant_dim], [expanded_dim], [expand_unique_value])
        VALUES ('%%ascendant_dim%%', '%%descendant_dim%%', '%%expanded_dim%%', '%%expand_unique_value%%')
        """

        for i, nav in enumerate(self.navigation):
            query = template_query.replace(r'%%ascendant_dim%%', str(nav[0]))           \
                .replace(r'%%descendant_dim%%', str(nav[1]))                            \
                .replace(r'%%expanded_dim%%', str(nav[2]))                               \
                .replace(r'%%expand_unique_value%%', str(nav[3]))
            cursor.execute(query)

            print(f"{i+1}/{len(self.navigation)}", flush=True, end='\r')


def construct_lattice(*karg):
    _lattice = lattice('00000_00000')
    cursor.commit()
    cursor.close()
    conn.close()


def visualize(percent):
    percent = float(percent)

    navigations_count = cursor.execute('SELECT COUNT(*) FROM [dbo].[navigation]').fetchone()[0]
    navigations_keep = int(navigations_count*percent/100)
    query = f"""
        SELECT 
        TOP({navigations_keep}) 
        [ascendant_dim], [descendant_dim], [external_rate]
        FROM [dbo].[navigation]
        ORDER BY [external_rate] DESC
    """
    navigations = cursor.execute(query).fetchall()
    graph = GraphVisualization(is_directed=True)
    for i, nav in enumerate(navigations):
        graph.addEdge(nav[0], nav[1])

    graph.visualize('00000_00000')

    cursor.commit()
    cursor.close()
    conn.close()


def clear_internal_table():
    query = """
        DELETE FROM [dbo].[internal_dim]
    """
    cursor.execute(query)
    cursor.commit()


def get_top_navigations(percent):
    navigations_count = cursor.execute('SELECT COUNT(*) FROM [dbo].[navigation]').fetchone()[0]
    navigations_keep = int(navigations_count*percent/100)
    query = f"""
        SELECT 
        TOP({navigations_keep}) 
        [ascendant_dim], [descendant_dim], [external_rate]
        FROM [dbo].[navigation]
        ORDER BY [external_rate]
    """
    navigations = cursor.execute(query).fetchall()

    return navigations


def get_dual_query_sql(s_dim, e_dim, src_dim, dimensions):
    template_file = 'template_dual_entropy_null_2_many.sql' \
        if s_dim == '00000' \
        else ('template_dual_entropy_many_2_null.sql' if e_dim == '00000' else 'template_dual_entropy.sql')
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


def execute_batch(cursor, query):
    batched_queries = query.split('GO')
    for q in batched_queries:
        q = q.strip()
        cursor.execute(q)
    cursor.commit()


def populate_internal_entropy(top_navigations):
    clear_internal_table()
    top_dims = [t[1] for t in top_navigations]
    for i, d in enumerate(top_dims):
        [s_dim, e_dim, src_dim] = extract_dual_dim(d)
        query = get_dual_query_sql(s_dim, e_dim, src_dim, attrs)
        
        execute_batch(cursor, query)
        print(f"{i+1}/{len(top_dims)}")
        # break


def insight(argv):
    [external, internal] = argv.split(':')

    top_navigations = list(get_top_navigations(float(external)))

    populate_internal_entropy(top_navigations)

    # top_sub_graphs = get_top_sub_graphs()

    # top_sub_graphs = get_sub_graph(navigations, internal)

    # print(len(top_navigations), top_navigations)


def create_cuboids_dim(level):
    """return all cuboids dim with [level] bit 1"""
    k_bits = kbits(2*len(attrs), level)
    k_bits = [k[:len(attrs)] + "_" + k[len(attrs):] for k in k_bits]
    return k_bits


# 10001_10010, 11100_10011 -> True
def exist_path(ci, cg):
    [il, ir] = ci.split('_')
    [gl, gr] = cg.split('_')
    return (int(il, 2) | int(gl, 2) == int(gl, 2)) and (int(ir, 2) | int(gr, 2) == int(gr, 2))


path_hashed = {}
def path_visited(ci, cg):
    return path_hashed.__contains__(join_node(ci, cg))

cuboids_visited = set()
def visit_path(ci, cg):
    path_hashed[join_node(ci, cg)] = 1
    cuboids_visited.add(ci)
    cuboids_visited.add(cg)


# find {ck | ck is child_of_ci and cg is child_of_ck}
def find_cuboid_k(ci, cg):
    cks = []
    for i, b in enumerate(ci):
        if b == '0':
            ck = f'{ci[:i]}1{ci[i+1:]}'
            if exist_path(ck, cg):
                cks.append(ck)
    return cks


def compute_external_h_rate_ikg(ci, ck, cg):
    expaned_dim, _ = get_expanded_dim(ci, ck)
    d_max = dim_unique_value[expaned_dim]
    return ((get_dim_dual_external_entropy(cg) - get_dim_dual_external_entropy(ci))/math.log(d_max, 2))


def join_node(ci, ck):
    return f'{ci}.{ck}'


def find_path(ci, cg):
    for i, g in cg:
        if g == '1':
            ck = cg[:i] + '0' + cg[i+1:]
            return [ck+"."+cg]+find_path(ci, ck)


def find_edges_ikg(ci, cg, delta_level=None):
    if delta_level < 1:
        return []
    if delta_level == 1:
        return [join_node(ci, cg)]
    
    edges = []
    for i, g in enumerate(cg):
        if g == '1':
            ck = cg[:i] + '0' + cg[i+1:]
            if exist_path(ci, ck):
                edge = join_node(ck, cg)
                edges.append(edge)
                edges = edges + find_edges_ikg(ci, ck, delta_level-1)
    
    return edges

def prune_lattice(nav_gcl, threshold, s, m):
    if m<s+1:
        return
    cuboids_i = create_cuboids_dim(level=s)
    cuboids_g = create_cuboids_dim(level=m)

    for ci in cuboids_i:
        for cg in cuboids_g:
            if exist_path(ci, cg) and not path_visited(ci, cg):
                visit_path(ci, cg)
                cuboids_k = find_cuboid_k(ci, cg) if s<m-1 else [cg]
                min_rate = sys.maxsize
                for ck in cuboids_k:
                    external_h_rate = compute_external_h_rate_ikg(ci, ck, cg)
                    min_rate = min(min_rate, external_h_rate)
                    k_min = ck
                if min_rate <= threshold:
                    print(f"{s} - {m} {len(nav_gcl)} {min_rate}: {ci}, {k_min}, {cg}", flush=True, end='\n')
                    edges = find_edges_ikg(ci, cg, delta_level=m-s)

                    nav_gcl[:] = nav_gcl + edges
                elif m > s+1:
                    prune_lattice(nav_gcl, threshold, s,int((s+m)/2))
                    prune_lattice(nav_gcl, threshold, int((s+m)/2), m)


def insert_nav_to_db(nav_gcl):
    delete_query = f'''
        DELETE FROM [dbo].[navigation_threshold]
        WHERE [threshold] = {nav_gcl[0][2]}
    '''
    cursor.execute(delete_query)

    insert_query = '''
        INSERT INTO [dbo].[navigation_threshold] ([start], [end], [threshold])
        VALUES (?, ?, ?)
    '''
    cursor.executemany(insert_query, nav_gcl)
    
    cursor.commit()


def compute_navigation(threshold=0.1):
    global cuboids_visited

    navigation_path = os.path.join('log', f'navigation_{threshold}.txt')
    navigation_path_attrs = os.path.join('log', f'navigation_{threshold}_attrs.txt')
    if os.path.exists(navigation_path):
        with open(navigation_path, 'r') as f:
            nav_gcl = f.readlines()
            nav_gcl = [n.strip() for n in nav_gcl]
    else:
        s = 0
        m = 2*len(attrs)
        nav_gcl = []
        prune_lattice(nav_gcl, threshold, s, m)

        with open(navigation_path, 'w') as f:
            for n in nav_gcl:
                f.write(n+"\n")
            f.close()

        with open(navigation_path_attrs, 'w') as f:
            f.write(f'{len(cuboids_visited)}\n')
            f.writelines([f'{c}\n' for c in cuboids_visited])
            f.close()

    nav_gcl = [(n.split('.')[0], n.split('.')[1], threshold) for n in nav_gcl]
    
    if len(nav_gcl) > 0:
        insert_nav_to_db(nav_gcl)


def get_navigations(threshold):
    query = f'''
        SELECT DISTINCT [end] 
        FROM [dbo].[navigation_threshold]
        WHERE [threshold] = {threshold}
    '''

    cursor.execute(query)
    navigations = list(cursor.fetchall())
    navigations = [(n[0], get_dim_level(n[0])) for n in navigations]
    return navigations


# 00000_00000:0.1
def suggest_navigate(arg):
    [dim, threshold] = arg.split(':')
    threshold = float(threshold)
    children_level = get_dim_level(dim) + 1
 
    navigations = get_navigations(threshold)
    suggested_navigations = []
    while len(suggested_navigations) == 0 and children_level < 2*len(attrs):
        suggested_navigations = [x for x in navigations if x[1] == children_level and is_descendant(dim, x[0])]
        children_level = children_level + 1

    return suggested_navigations


def get_avaiable_navigate(dim, threshold):
    children_dims = get_children_dims(dim)
    suggested_dims = suggest_navigate(f'{dim}:{threshold}')
    
    children_dims = [get_dim_info_dual(cd) for cd in children_dims]
    suggested_dims = [get_dim_info_dual(sd[0]) for sd in suggested_dims]

    return children_dims, suggested_dims


def is_dual_cuboid_computed(dim):
    query = '''
        SELECT OBJECT_ID('{}')
    '''
    [s_dim, e_dim, src_dim] = extract_dual_dim(dim)
    table_name = f'dual_{s_dim}_{e_dim}_base_{src_dim}_edge_aggregated'

    query = query.format(table_name)
    
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] != None


def compute_dual_cuboid(dim):
    [s_dim, e_dim, src_dim] = extract_dual_dim(dim)
    query = get_dual_query_sql(s_dim, e_dim, src_dim, attrs)
    

    execute_batch(cursor, query)
    cursor.commit()


def compute_internal(dim):
    template_query = '''
        INSERT INTO [dbo].[internal_dim] (
            [id],
            [dim],
            [type],
            [entropy_rate]
        )
            SELECT [{type}] AS [id],
                '{dim}' AS [dim],
                '{type}' AS [type],
                [dbo].a_entropy([weight]) AS [entropy_rate]
            FROM [{table_name}]
            GROUP BY [{type}]
    '''

    table_name = get_dual_table_name(dim)

    query_start = template_query.format_map({
        'dim': dim,
        'table_name': table_name,
        'type': 'start'
    })
    query_end = template_query.format_map({
        'dim': dim,
        'table_name': table_name,
        'type': 'end'
    })

    cursor.execute(query_start)
    cursor.execute(query_end)

    cursor.commit()


def parse_column_block(side_dim, direction_type):
    columns = [
        '[{direction_prefix}gender].[gender_name] AS [{direction_prefix}gender]',
	    '[{direction_prefix}job].[job_title] AS [{direction_prefix}job]',
	    '[{direction_prefix}place].[place_name] AS [{direction_prefix}place]',
	    '[{direction_prefix}university].[university] AS [{direction_prefix}university]',
	    '[{direction_prefix}institution].[institution] AS [{direction_prefix}institution]',
    ]

    dim_level = get_dim_level(side_dim)
    if dim_level == 0:
        return f'"All" AS [{direction_type}]'

    return ',\n'.join(
            [
                c.format_map(
                    {'direction_prefix': f'{direction_type}_'}
                ) 
                for i, c in enumerate(columns) if side_dim[i] == '1'
            ]
        )


def parse_join_block(side_dim, direction_type):
    columns = [
        '''JOIN [gender] as [{direction_prefix}gender]
            ON [{direction_prefix}gender].[gender_id] = (
                SELECT CAST('<x>' + REPLACE([{direction}],'.','</x><x>') + '</x>' AS XML).value('/x[{i}]','int')
            )''',
        '''JOIN [job] as [{direction_prefix}job]
            ON [{direction_prefix}job].[job_id] = (
                SELECT CAST('<x>' + REPLACE([{direction}],'.','</x><x>') + '</x>' AS XML).value('/x[{i}]','int')
            )''',
        '''JOIN [place] AS [{direction_prefix}place]
            ON [{direction_prefix}place].[place_id] = (
                SELECT CAST('<x>' + REPLACE([{direction}],'.','</x><x>') + '</x>' AS XML).value('/x[{i}]','int')
            )''',
        '''JOIN [university] AS [{direction_prefix}university]
            ON [{direction_prefix}university].[university_id] = (
                SELECT CAST('<x>' + REPLACE([{direction}],'.','</x><x>') + '</x>' AS XML).value('/x[{i}]','int')
            )''',
        '''JOIN [institution] AS [{direction_prefix}institution]
            ON [{direction_prefix}institution].[institution_id] = (
                SELECT CAST('<x>' + REPLACE([{direction}],'.','</x><x>') + '</x>' AS XML).value('/x[{i}]','int')
            )'''
    ]

    dim_level = get_dim_level(side_dim)
    if dim_level == 0:
        return ''

    formated_join = []
    for i, c in enumerate(columns):
        if side_dim[i] == '1':
            formated_join.append(
                columns[i].format_map({
                    'direction_prefix': f'{direction_type}_',
                    'i': len(formated_join)+1,
                    'direction': direction_type
                })
            )
    
    return '\n'.join(formated_join)


def parse_condition_block(value, direction_type):
    if value is None:
        return f'{direction_type} IS NULL'
    
    return f'''[{direction_type}] = '{value}' '''


def parse_sub_graph(sub_graph):
    [id, dim, direction_type, entropy] = sub_graph
    [start_dim, end_dim] = dim.split('_')
    table_name = get_dual_table_name(dim)

    template = open(os.path.join('sql', 'query_file', 'template_sub_graph copy.sql'), 'r').read()

    query = template.format_map({
        'table_name': table_name,
        'start_column_block': parse_column_block(start_dim, 'start'),
        'end_column_block': parse_column_block(end_dim, 'end'),
        'start_join_block': parse_join_block(start_dim, 'start'),
        'end_join_block': parse_join_block(end_dim, 'end'),
        'condition_block': parse_condition_block(id, direction_type)
    })

    df = pd.read_sql_query(query, conn)
    df.drop(columns=['start', 'end'], inplace=True)
    df['%'] = (df['weight'] / df['weight'].sum() * 100).map('{:,.2f}'.format)
    return df
    

def get_sub_graph(dim, threshold):
    query = f'''
        SELECT *
        FROM [dbo].[internal_dim]
        WHERE 
            [dim] = '{dim}'
                AND
            [entropy_rate] <= {threshold}
    '''
    cursor.execute(query)
    sub_graphs = list(cursor.fetchall())
    sub_graphs.sort(key=lambda x: x[3])
    sub_graphs = [(parse_sub_graph(sg), sg[3]) for sg in sub_graphs]

    return sub_graphs


def get_trends(dim, threshold):    
    if not is_internal_computed(dim):
        print('>> compute internal')
        if not is_dual_cuboid_computed(dim):
            print('>> compute dual')
            compute_dual_cuboid(dim)
        compute_internal(dim)

    sub_graphs = get_sub_graph(dim, threshold)
    return sub_graphs


def view_internal(arg):
    [dim, threshold] = arg.split(':')
    threshold = float(threshold)

    trends = get_trends(dim, threshold)
    return trends

def test(argv):
    print(view_internal('00100_01110:0.73'))


def main(argv):
    try:
        opts, args = getopt.getopt(argv, 'cv:i:t', ['compute_navigation=', 'suggest_navigate='])
    except:
        sys.exit(2)
    
    for opt, arg in opts:
        if opt == '-c':
            construct_lattice(arg)
        elif opt == '-v':
            visualize(arg)
        elif opt == '-i':
            insight(arg)
        elif opt == '-t':
            test(arg)
        elif opt == '--compute_navigation':
            compute_navigation(float(arg))
        elif opt == '--suggest_navigate':
            suggest_navigate(arg)
        elif opt == '--view_internal':
            view_internal(arg)

    cursor.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1:])