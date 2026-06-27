import { Routes, Route } from 'react-router-dom'

function PlaceholderDashboard() {
  return (
    <main>
      <h1>AthleteOS Dashboard</h1>
      <p>Dashboard coming in WU-5.</p>
    </main>
  )
}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<PlaceholderDashboard />} />
    </Routes>
  )
}
