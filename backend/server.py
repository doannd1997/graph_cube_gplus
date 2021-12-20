import sys, glob
from flask import Flask, render_template, request
sys.path.insert(1, '.')

from src.graph.lattice import get_trends, get_avaiable_navigate, compute_trend
from src.graph.dim_info import get_dim_info_dual, get_available_thresholds
from src.util.util import get_dim_alias

app = Flask(__name__)


@app.route('/cuboid/<dim>/<threshold>', methods=['GET', 'POST'])
def request_cuboid(dim, threshold):
    dim_info = get_dim_info_dual(dim)
    children_dims, suggested_dims = get_avaiable_navigate(dim, threshold)
    return {
        'info': dim_info,
        'children_dims': children_dims,
        'suggested_dims': suggested_dims,
        'available_thresholds': get_available_thresholds(),
        'external_threshold_rate': threshold
        }


@app.route('/compute_internal/<dim>')
def request_compute_internal(dim):
    compute_trend(dim)
    return {
        'dim': dim
    }
    

@app.route('/view_internal/<dim>/<threshold>', methods=['GET', 'POST'])
def request_view_internal(dim, threshold):
    internal_trends = get_trends(dim, threshold)
    return render_template(
        'internal_view.html',
        tables=[t[0].to_html() for t in internal_trends],
        titles = ['na'] + [f'#{i+1} - Internal rate: {round(internal_trends[i][1], 6)}' for i in range(len(internal_trends))],
        count=len(internal_trends),
        cuboid_alias=get_dim_alias(dim)
        )


if __name__ == '__main__':
    app.run(debug=True)