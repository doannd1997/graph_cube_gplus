import './App.css'
import { useState, useEffect } from 'react'
import Home from './component/home/Home'
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom'

function App() {
	return (
		<Router>
			<Switch>
				<Route exact path='/' component={Home} />
			</Switch>
		</Router>
	)
}

export default App
