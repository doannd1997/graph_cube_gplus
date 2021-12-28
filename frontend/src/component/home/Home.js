import React, { useEffect, Fragment, useState } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import { useAlert } from 'react-alert'
import { Spinner, FormCheck } from 'react-bootstrap'
import Switch from 'react-bootstrap-switch'

import {
	getCuboid,
	clearErrors,
	computeInternal,
} from '../../actions/cuboidAction'
import MetaData from '../layout/Metadata'
import DimCard from './DimCard'
import './home.css'

import { Container, Row, Col, Button, Form } from 'react-bootstrap'

const Home = () => {
	const alert = useAlert()
	const dispatch = useDispatch()
	const {
		info,
		children_dims,
		suggested_dims,
		loading,
		error,
		computing_internal,
		history_dims,
	} = useSelector(state => state.cuboid)

	const dims = ['Gender', 'Job', 'Place', 'University', 'Institution']
	const [state, setState] = useState({
		externalEntropyRate: 0.1,
		internalEntropyRate: '',
		quick_navigate_start: [...Array(dims.length).keys()].map(i => false),
		quick_navigate_end: [...Array(dims.length).keys()].map(i => false),
	})

	const onSelectExternalThresholdRate = event => {
		dispatch(getCuboid(info.dim, event.target.value))
	}

	const onChangeRate = event => {
		setState({
			...state,
			[event.target.name]: event.target.value,
		})
	}

	const onSwitchChange = event => {
		let direction = event.target.id.split('.')[0]
		let index = event.target.id.split('.')[1]

		setState({
			...state,
			[direction]: state[direction].map((d, i) => (i == index ? !d : d)),
		})
	}

	const onClickQuickNavigate = event => {
		let dim = [
			state.quick_navigate_start.map(n => (n ? 1 : 0)).join(''),
			state.quick_navigate_end.map(n => (n ? 1 : 0)).join(''),
		].join('_')
		dispatch(getCuboid(dim, state.externalEntropyRate))
	}

	const onClickBack = event => {
		if (history_dims.length > 1)
			dispatch(getCuboid(history_dims.at(-1), state.externalEntropyRate, true))
	}

	const processExternal = () => {
		dispatch(getCuboid(info.dim, state.externalEntropyRate))
	}

	const processInternal = () => {
		if (!info.internal_computed) {
			const successCallback = () => {
				dispatch(getCuboid(info.dim, state.externalEntropyRate))
			}
			dispatch(computeInternal(info.dim, successCallback))
		} else {
			let url = `http://127.0.0.1:5000/view_internal/${info.dim}/${state.internalEntropyRate}`
			window.open(url)
		}
	}

	useEffect(() => {
		if (error) {
			alert.error(error)
			dispatch(clearErrors())
		}
		dispatch(getCuboid())
	}, [])

	return (
		<div class='d-flex' id='container'>
			<MetaData title={info.dim_alias} />
			<div class='p-2 pt-2 navigation-container'>
				<div class='d-flex flex-column'>
					<br></br>
					<h4 class='text-center'>Children Cuboids({children_dims.length})</h4>
					<Row className='row-cols-1 row-cols-md-3 g-4 mx-auto mt-1 pb-3 border flex-grow-1'>
						{children_dims.map(cuboid => (
							<Col>
								<DimCard key={cuboid.dim} cuboid={cuboid} />
							</Col>
						))}
					</Row>
				</div>
				<div class='d-flex flex-column'>
					<br></br>
					<h4 class='text-center'>
						Suggested Cuboids({suggested_dims.length})
					</h4>
					<Row className='row-cols-1 row-cols-md-3 g-4 mx-auto mt-1 pb-3 border flex-grow-1'>
						{suggested_dims.map(cuboid => (
							<Col>
								<DimCard key={cuboid.dim} cuboid={cuboid} />
							</Col>
						))}
					</Row>
				</div>
			</div>
			<div
				class='d-flex flex-column action-container'
				style={{ width: '40rem' }}
			>
				<div class='p-1 m-1 flex-grow-1 d-flex flex-column border quick-navigation-container'>
					<div class='d-flex align-items- p-1 justify-content-between'>
						<Form.Control
							type='text'
							placeholder={`External Entropy Rate`}
							name='externalEntropyRate'
							required
							aria-describedby='help'
							value={state.externalEntropyRate}
							onChange={onChangeRate}
							style={{ marginRight: '2px' }}
						/>
						<Button
							onClick={processExternal}
							style={{ backgroundColor: 'SaddleBrown', outline: 'none' }}
						>
							Process
						</Button>
					</div>
					<div class='mt-2 d-flex flex-column-reverse flex-grow-1'>
						<div
							className='d-flex justify-content-center border-radius'
							style={{
								height: '2.2rem',
								cursor: 'pointer',
								backgroundColor: 'SaddleBrown',
								borderRadius: '0.3rem',
							}}
							onClick={onClickBack}
						>
							<strong class='mt-1 text-center align-middle text-white'>
								Back
							</strong>
						</div>
						<div
							className='d-flex justify-content-center border-radius mb-1'
							style={{
								height: '2.2rem',
								cursor: 'pointer',
								backgroundColor: 'SaddleBrown',
								borderRadius: '0.3rem',
							}}
							onClick={onClickQuickNavigate}
						>
							<strong class='mt-1 text-center align-middle text-white'>
								Quick Navigate
							</strong>
						</div>
						<div class='mb-2 border-top'>
							<div className='d-flex'>
								<div className='flex-grow-1 text-center font-weight-bold border-right'>
									<strong>Start</strong>
								</div>
								<div className='flex-grow-1 text-center font-weight-bold'>
									<strong>End</strong>
								</div>
							</div>
							{dims.map((dim, i) => (
								<div className='d-flex mt-2'>
									<FormCheck
										className='flex-grow-1'
										id={`quick_navigate_start.${i}`}
										type='switch'
										checked={state.quick_navigate_start[i]}
										onChange={onSwitchChange}
										label={dim}
									/>
									<FormCheck
										className='flex-grow-1'
										id={`quick_navigate_end.${i}`}
										type='switch'
										checked={state.quick_navigate_end[i]}
										onChange={onSwitchChange}
										label={dim}
									/>
								</div>
							))}
						</div>
					</div>
				</div>
				<div class='p-1 m-1 d-flex flex-column border process-internal-container'>
					<div class='container flex-grow-1'>
						<div class='d-flex'>
							<div class='flex-grow-1 text-center'>
								<strong>{info.dim_alias}</strong>
							</div>
						</div>
						<br />
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>Level:</text>
							</div>
							<div class='flex-grow-1'>{info.level}</div>
						</div>
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>Sô đỉnh bắt đầu:</text>
							</div>
							<div class='flex-grow-1'>{info.v_size_s}</div>
						</div>
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>Số đỉnh kết thúc:</text>
							</div>
							<div class='flex-grow-1'>{info.v_size_e}</div>
						</div>
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>Số cạnh:</text>
							</div>
							<div class='flex-grow-1'>{info.e_size}</div>
						</div>
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>External Entropy:</text>
							</div>
							<div class='flex-grow-1'>{info.external_entropy}</div>
						</div>
						<div class='d-flex'>
							<div class='attr-title text-left'>
								<text style={{ color: '#333' }}>Internal Entropy:</text>
							</div>
							<div class='flex-grow-1'>
								{info.internal_computed ? 'Đã tính' : 'Chưa tính'}
							</div>
						</div>
					</div>
					<br />
					<div class='d-flex align-items- p-1 justify-content-between'>
						<Form.Control
							type='text'
							placeholder={`Internal Entropy Rate ${
								info.internal_computed
									? Number(info.min_internal_entropy_rate).toFixed(2)
									: ''
							}`}
							name='internalEntropyRate'
							required
							aria-describedby='help'
							value={state.internalEntropyRate}
							onChange={onChangeRate}
							style={{ marginRight: '2px' }}
						/>
						{computing_internal ? (
							<Spinner animation='border' role='status'>
								<span className='visually-hidden'>Loading...</span>
							</Spinner>
						) : (
							<Button
								onClick={processInternal}
								style={{ backgroundColor: 'SaddleBrown', outline: 'none' }}
							>
								Process
							</Button>
						)}
					</div>
				</div>
			</div>
		</div>
	)
}

export default Home
