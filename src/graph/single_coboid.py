import pandas as pd
import numpy as np
import json
import os

data_path = os.environ.get('data_path')
property = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))
attrs = property['attrs']
node_path = os.path.join(data_path, 'extracted', 'refined', 'node_attr.csv')
edge_path = os.path.join(data_path, 'extracted', 'edges_indexed_id.csv')

class single_cuboid:
    def __init__(self, dimensions, dim, dim_alias, nodes, edges) -> None:
        super().__init__()
        self.dimensions = dimensions
        self.dim = dim
        self.dim_alias = dim_alias
        self.nodes = nodes
        self.edges = edges

        # print(dim)
        # print(self.dim_alias)
        self.aggregated_nodes = pd.DataFrame(data=[], columns=self.dim_alias+['weight', 'hash'])
        self.aggregated_edges = pd.DataFrame(data=[], columns=['start', 'end', 'weight'])
        self.populate()
        self.aggregated_nodes.to_csv(os.path.join('./temp', 'aggregated_nodes.csv'), index=False, sep='\t')
        pd.DataFrame(data=list(self.map_id2indexed_id.items()), columns=['id', 'indexed_id']).to_csv(
            os.path.join('./temp', 'map_id2indexed_id.csv'),
            sep='\t',
            index=False
        )

    def populate(self):
        self.map_h2index = dict()
        self.map_id2indexed_id = dict()
        self.populate_node()
        self.populate_edge()

    def populate_node(self):
        for r, row in self.nodes.iterrows():
            attrs = list(row[self.dim_alias])
            hashed = str(attrs)
            idx = self.hash_node(attrs, hashed)
            self.map_id2indexed_id[row['indexed_id']] = idx
            print(f"{r+1}/{self.nodes.shape[0]}", flush=True, end='\r')
        

    def hash_node(self, attrs, hashed):
        if hashed not in self.map_h2index:
            data = attrs + [1, len(self.map_h2index.keys())]
            columns = self.dim_alias + ['weight', 'indexed_id']
            
            new_node = pd.DataFrame(data=[data], columns=columns, dtype=object)
            self.aggregated_nodes = self.aggregated_nodes.append(new_node, ignore_index=True)
            index = self.aggregated_nodes.shape[0] - 1
            self.map_h2index[hashed] = index
        else:
            index = self.map_h2index[hashed]
            self.aggregated_nodes.at[index, 'weight'] = self.aggregated_nodes.at[index, 'weight'] + 1
        return index

    def populate_edge(self):
        pass

    def populate_external_entropy(self):
        pass

    def populate_internal_entropy(self):
        pass

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

