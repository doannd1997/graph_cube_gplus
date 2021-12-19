import logo from './logo.svg'
import './App.css'
import { useState, useEffect } from 'react'

function App() {
	const [data, setData] = useState([{}])

	useEffect(() => {
		fetch('/cuboid/00000_00010')
			.then(res => res.json())
			.then(data => {
				// setData(data)
				// console.log(data)
			})
	})

	return (
		<div className='App'>
			<header className='App-header'>
				<img src={logo} className='App-logo' alt='logo' />
				<p>
					Edit <code>src/App.js</code> and save to reload.
				</p>
				<a
					className='App-link'
					href='https://reactjs.org'
					target='_blank'
					rel='noopener noreferrer'
				>
					Learn React
				</a>
			</header>
		</div>
	)
}

export default App