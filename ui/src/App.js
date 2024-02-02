import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { Box, CssVarsProvider } from "@mui/joy";
import Sidebar from "./components/Sidebar";
import Header from "./components/Header";
import MainContent from "./components/MainContent";

function App() {
  const [components, setComponents] = useState([]);

  useEffect(() => {
    // Simulate fetching data
    const fetchedComponents = [
      { name: "Component1", id: "component1" },
      { name: "Component2", id: "component2" },
      // Add more components as they are fetched
    ];
    setComponents(fetchedComponents);
  }, []);

  return (
    <CssVarsProvider>
      <Router>
        <Header title="Motion" />
        <Box sx={{ display: "flex", pt: "48px" }}>
          <Sidebar components={components} />
          <Box sx={{ flex: 1 }}>
            <Routes>
              {components.map((component) => (
                <Route
                  key={component.id}
                  path={`/${component.id}`}
                  element={<MainContent componentName={component.name} />}
                />
              ))}
            </Routes>
          </Box>
        </Box>
      </Router>
    </CssVarsProvider>
  );
}

export default App;
