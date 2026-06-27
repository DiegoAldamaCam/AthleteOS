import { Routes, Route } from 'react-router-dom'
import DashboardPage from './components/DashboardPage'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
    </Routes>
  )
}
