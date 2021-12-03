from datetime import datetime, time
import os
import glob
import pandas as pd
import numpy as np
from pandas.io import json
import json

from collections import Counter

from dotenv import load_dotenv

load_dotenv('.pyenv')

data_path = os.environ.get('data_path')
net_path = os.path.join(data_path, 'gplus')
ignored_attrs = ['last_name']


def feat_vector_to_attr(vector, feat_names):
    attr = dict()
    for i, v in enumerate(vector):
        if v == '1':
            feat = feat_names[i].split(':', 1)
            key = feat[0]
            if not key in ignored_attrs:
                attr[key] = feat[1]
    return attr


net_files = os.listdir(net_path)
net_files = [nf.split('.')[0] for nf in net_files]
net_files = list(dict.fromkeys(net_files))


def load_attr():
    node_attr = []
    for v, nf in enumerate(net_files):
        ego_id = nf

        feat_names_src = open(os.path.join(net_path, f'{nf}.featnames'), 'r', encoding='utf-8').readlines()
        try:
            feat_names_src = [fn.strip() for fn in feat_names_src]
            feat_names = {int(fn.split(' ', 1)[0]) : fn.split(' ', 1)[1].strip() for fn in feat_names_src}

            ego_feats = open(os.path.join(net_path, f'{nf}.egofeat'), 'r').read().split(' ')
            ego_feat = feat_vector_to_attr(vector=ego_feats, feat_names=feat_names)
            
            ego_feat['id'] = '' + ego_id
            node_attr.append(ego_feat)

            nodes_feats = open(os.path.join(net_path, f'{nf}.feat'), 'r').readlines()
            nodes_feats = [nf.strip() for nf in nodes_feats]
            for node_feat_vector in nodes_feats:
                if len(node_feat_vector.split(' ', 1)) != 2:
                    continue
                node_id = node_feat_vector.split(' ', 1)[0]
                node_feat = node_feat_vector.split(' ', 1)[1].split(' ')
                node_feat = feat_vector_to_attr(vector=node_feat, feat_names=feat_names)
                
                node_feat['id'] = node_id
                node_attr.append(node_feat)        
        except:
            pass

        print(f"Node completed: {v+1}/{len(net_files)} networks", flush=True, end="\r")

    df = pd.DataFrame(node_attr)
    return df


def create_edge(start, finish):
    return f'{start} {finish}'


def load_edges():
    edges = []
    for v, nf in enumerate(net_files):
        ego_id = nf

        followers = open(os.path.join(net_path, f'{nf}.followers'), 'r').readlines()
        followers = [f.strip() for f in followers]
        for follower in followers:
            edge = create_edge(follower, ego_id)
            edges.append(edge)

        relations = open(os.path.join(net_path, f'{nf}.edges'), 'r').readlines()
        for relation in relations:
            relation = relation.strip()
            n0 = relation.split(' ')[0]
            n1 = relation.split(' ')[1]
            edge = create_edge(n0, n1)
            edges.append(edge)

            edge = create_edge(ego_id, n0)
            edges.append(edge)

            edge = create_edge(ego_id, n1)
            edges.append(edge)

        print(f'Edge completed: {v+1}/{len(net_files)}', flush=True, end='\r')

    edges = dict(Counter(edges))
    edges = edges.keys()
    start_nodes = [s.split(' ')[0] for s in edges]
    end_nodes = [e.split(' ')[1] for e in edges]
    edges = pd.DataFrame(data={'start': start_nodes, 'end': end_nodes})
    return edges

def extract():
    attr_df = load_attr()
    attr_df.to_csv(os.path.join(data_path, 'extracted', f'attr.csv'), index=False, sep='\t')
    
    attr_df = attr_df.replace('None', np.NaN).groupby(['id'], as_index=False).first()
    attr_df['indexed_id'] = list(range(attr_df.shape[0]))
    attr_df.to_csv(os.path.join(data_path, 'extracted', f'attr_grouped.csv'), index=False, sep='\t')

    edges = load_edges()
    edges.to_csv(os.path.join(data_path, 'extracted', f'edges.csv'), index=False, sep='\t')

    attrs = list(attr_df.columns)
    attrs.remove('indexed_id')
    attrs.remove('id')
    property = {
        'attrs': attrs,
        'node_number': attr_df.shape[0],
        'edge_number': edges.shape[0]
    }
    property_file = open(os.path.join(data_path, 'extracted', 'property.json'), 'w')
    json.dump(property, property_file)
    property_file.close()

if __name__ == '__main__':
    st = datetime.now()
    extract()
    print(f"Finished in {(datetime.now()-st).seconds} seconds")
