import {
	CUBOID_REQUEST,
	CUBOID_SUCCESS,
	CUBOID_FAIL,
	CUBOID_COMPUTE_INTERNAL_REQUEST,
	CUBOID_COMPUTE_INTERNAL_SUCCESS,
	CUBOID_COMPUTE_INTERNAL_FAIL,
	CLEAR_ERRORS,
} from '../consts/cuboidConsts'

const cuboidDefaultState = {
	info: {
		dim: '???',
		dim_alias: '?????-?????',
		e_size: '???',
		v_size_s: '???',
		v_size_e: '???',
		internal_computed: '???',
	},
	children_dims: [],
	suggested_dims: [],
	loading: true,
	computing_internal: false,
	error: null,
	history_dims: [],
}

export const cuboidReducer = (state = cuboidDefaultState, action) => {
	switch (action.type) {
		case CUBOID_REQUEST:
			return { ...state, loading: true }
		case CUBOID_SUCCESS:
			return {
				last_dim: state.info.dim,
				info: action.payload.info,
				children_dims: action.payload.children_dims,
				suggested_dims: action.payload.suggested_dims,
				available_thresholds: action.payload.available_thresholds,
				external_threshold_rate: action.payload.external_threshold_rate,
				loading: false,
				history_dims: action.payload.isBack
					? state.history_dims.slice(0, -1)
					: state.history_dims.concat([state.info.dim]),
			}
		case CUBOID_COMPUTE_INTERNAL_REQUEST:
			return {
				...state,
				computing_internal: true,
			}
		case CUBOID_COMPUTE_INTERNAL_SUCCESS:
		case CUBOID_COMPUTE_INTERNAL_FAIL:
			return {
				...state,
				computing_internal: false,
			}
		case CLEAR_ERRORS:
			return {
				...state,
				error: null,
			}
		default:
			return state
	}
}
