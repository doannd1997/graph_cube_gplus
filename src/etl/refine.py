import os
import pandas as pd
import numpy as np
import random
import json
from dotenv import load_dotenv

load_dotenv('.pyenv')

data_path = os.environ.get('data_path')
net_path = os.path.join(data_path, 'g_plus')

attrs = json.load(open(os.path.join(data_path, 'extracted', 'property.json'), 'r'))['attrs']

def randomiseMissingData(df2):
    "randomise missing data for DataFrame (within a column)"
    df = df2.copy()
    for col in df.columns:
        data = df[col]
        mask = data.isnull()
        samples = random.choices( data[~mask].values , k = mask.sum() )
        data[mask] = samples

    return df


def mapping_gender(g):
    switcher = {
        '1': 'male',
        '2': 'female'
    }
    return switcher.get(g, 'other')
    

def refine():
    node_attr_df = pd.read_csv(os.path.join(data_path, 'extracted', 'attr_grouped.csv'), header=0, sep='\t', dtype=object)

    id_map_df = node_attr_df[['id', 'indexed_id']]

    node_attr_df = randomiseMissingData(node_attr_df).drop(columns=['id'])
    node_attr_df['gender'] = node_attr_df['gender'].apply(lambda g: mapping_gender(g))
    for a in attrs:
        node_attr_df[a] = node_attr_df[a].apply(lambda a: a.lower())
    node_attr_df.to_csv(os.path.join(data_path, 'extracted', 'attr_filled.csv'), index=False, sep='\t')

    edge_df = pd.read_csv(os.path.join(data_path, 'extracted', 'edges.csv'), header=0, sep='\t')
    edge_df = edge_df.merge(id_map_df, left_on='start', right_on='id')
    edge_df = edge_df.merge(id_map_df, left_on='end', right_on='id')
    edge_df = edge_df[['indexed_id_x', 'indexed_id_y']].rename(columns={'indexed_id_x': 'start', 'indexed_id_y': 'end'})
    
    edge_df.to_csv(os.path.join(data_path, 'extracted', 'edges_indexed_id.csv'), index=False, sep='\t')


if __name__ == '__main__':
    refine()