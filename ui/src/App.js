import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { Box, CssVarsProvider, Sheet } from "@mui/joy";
import Sidebar from "./components/Sidebar";
import MotionHeader from "./components/MotionHeader";
import MainContent from "./components/MainContent";
import axios from "axios";

axios.defaults.baseURL = "http://localhost:8000";

function App() {
  const [components, setComponents] = useState([]);

  useEffect(() => {
    axios
      .get("/components") // Assuming the endpoint is '/components'
      .then((response) => {
        // Add id to each component in response.data
        const componentsWithIds = response.data.map((component, index) => {
          return { name: component, key: `component${index + 1}` };
        });
        setComponents(componentsWithIds);
      })
      .catch((error) => {
        console.error("There was an error fetching the components", error);
      });
  }, []);

  return (
    <CssVarsProvider
      // defaultMode={mode}
      disableNestedContext
      modeStorageKey="motion-dark-mode"
    >
      <Router>
        <Sheet
          sx={{
            display: "flex",
            pt: "80px",
            bgcolor: "background.paper",
            minHeight: "100vh",
          }}
        >
          <MotionHeader title="Motion" />
          <Sidebar components={components} />
          <Box sx={{ flex: 1 }}>
            <Routes>
              {components.map((component) => (
                <Route
                  key={component.key}
                  path={`/${component.name}`}
                  element={<MainContent componentName={component.name} />}
                />
              ))}
            </Routes>
          </Box>
        </Sheet>
      </Router>
    </CssVarsProvider>
  );
}

export default App;
