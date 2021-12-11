import os
import sys
import math
import json
import getopt

from dotenv import load_dotenv
from networkx.generators import directed
load_dotenv('.pyenv')

sys.path.insert(1, '.')

from sql.query.sql import db_con_cur
from src.graph.visualize import GraphVisualization

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
    # print(navigations)
    graph = GraphVisualization(is_directed=True)
    for i, nav in enumerate(navigations):
        graph.addEdge(nav[0], nav[1])
        # if i == 12:
        #     break

    graph.visualize('00000_00000')

    cursor.commit()
    cursor.close()
    conn.close()


def main(argv):
    try:
        opts, args = getopt.getopt(argv, 'cv:')
    except:
        sys.exit(2)
    
    switcher = {
        '-c': construct_lattice,
        '-v': visualize
    }

    for opt, arg in opts:
        switcher.get(opt)(arg)


if __name__ == '__main__':
    main(sys.argv[1:])