import pandas as pd
import numpy as np
import json
import os

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