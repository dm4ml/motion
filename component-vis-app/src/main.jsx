import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)

// const data = JSON.parse(document.getElementById('data').textContent);
// ReactDOM.render(
//   <React.StrictMode>
//     <App data={data} />
//   </React.StrictMode>,
//   document.getElementById('root')
// );