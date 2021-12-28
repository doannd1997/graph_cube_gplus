import React from 'react'
import { useSelector, useDispatch } from 'react-redux'
import { getCuboid, clearErrors } from '../../actions/cuboidAction'
import { Card } from 'react-bootstrap'
import { IoMdCube } from 'react-icons/all'
import './home.css'

const DimCard = ({ cuboid }) => {
	const dispatch = useDispatch()
	const { external_threshold_rate } = useSelector(state => state.cuboid)

	const onClickCard = () => {
		dispatch(getCuboid(cuboid.dim, external_threshold_rate))
	}
	return (
		<Card
			style={{ width: '22rem', margin: '2px', height: '100%' }}
			className='shadow'
			onClick={onClickCard}
		>
			<Card.Body>
				<div class='d-flex mb-2'>
					<strong style={{ fontSize: '0.8rem' }} className='text-center'>
						{cuboid.dim_alias}
					</strong>
				</div>
				<div class='d-flex'>
					<text style={{ color: '#555555' }}>Level: {cuboid.level}</text>
				</div>
				<div class='d-flex'>
					<text style={{ color: '#555555' }}>
						Số đỉnh bắt đầu: {cuboid.v_size_s}
					</text>
				</div>
				<div class='d-flex'>
					<text style={{ color: '#555555' }}>
						Số đỉnh kết thúc: {cuboid.v_size_e}
					</text>
				</div>
				<div class='d-flex'>
					<text style={{ color: '#555555' }}>Số cạnh: {cuboid.e_size}</text>
				</div>
				<div class='d-flex'>
					<text style={{ color: '#555555' }}>
						external Entropy: {cuboid.external_entropy}
					</text>
				</div>
			</Card.Body>
			<Card.Footer>
				<IoMdCube color={cuboid.internal_computed ? 'green' : '#333333'} />
				<span> </span>
				{cuboid.internal_computed ? 'Đã tính Internal' : 'Chưa tính Internal'}
			</Card.Footer>
		</Card>
	)
}

export default DimCard
