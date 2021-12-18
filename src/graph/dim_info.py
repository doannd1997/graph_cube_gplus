import os
import sys
import math
import json
import getopt
import itertools

from dotenv import load_dotenv
from networkx.generators import directed
load_dotenv('.pyenv')

sys.path.insert(1, '.')

from sql.query.sql import db_con_cur

conn, cur = db_con_cur()

query_dim_info_dual = '''
    SELECT *
    FROM [dbo].[dim_info_dual]
    '''

dim_info_dual = cur.execute(query_dim_info_dual).fetchall()


def get_dim_dual_external_entropy(dim):
    for d in dim_info_dual:
        if d[0] == dim:
            return d[5]
    return None


# conn.close()