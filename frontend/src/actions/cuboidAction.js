import axios from 'axios'

import {
	CUBOID_REQUEST,
	CUBOID_SUCCESS,
	CUBOID_FAIL,
	CUBOID_COMPUTE_INTERNAL_REQUEST,
	CUBOID_COMPUTE_INTERNAL_SUCCESS,
	CUBOID_COMPUTE_INTERNAL_FAIL,
	CLEAR_ERRORS,
} from '../consts/cuboidConsts'

export const getCuboid =
	(dim = '00000_00000', threshold = 0.1, isBack = false) =>
	async dispatch => {
		try {
			dispatch({
				type: CUBOID_REQUEST,
			})

			let url = `/cuboid/${dim}/${threshold}`

			const { data } = await axios.get(url)

			dispatch({
				type: CUBOID_SUCCESS,
				payload: { ...data, isBack: isBack },
			})
		} catch (error) {
			console.log(CUBOID_FAIL, error)
		}
	}

export const computeInternal = (dim, successCallback) => async dispatch => {
	try {
		dispatch({
			type: CUBOID_COMPUTE_INTERNAL_REQUEST,
		})

		let url = `/compute_internal/${dim}`

		const { data } = await axios.get(url)

		dispatch({
			type: CUBOID_COMPUTE_INTERNAL_SUCCESS,
		})

		successCallback()
	} catch (error) {
		console.log(CUBOID_COMPUTE_INTERNAL_FAIL, error)
	}
}

export const clearErrors = () => async dispatch => {
	dispatch({ type: CLEAR_ERRORS })
}
