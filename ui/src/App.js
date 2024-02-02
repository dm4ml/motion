import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { Box, CssVarsProvider } from "@mui/joy";
import Sidebar from "./components/Sidebar";
import Header from "./components/Header";
import MainContent from "./components/MainContent";
import axios from "axios";
import theme from "./customTheme";

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
    <CssVarsProvider theme={theme}>
      <Router>
        <Header title="Motion State Editor" />
        <Box sx={{ display: "flex", pt: "80px" }}>
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
        </Box>
      </Router>
    </CssVarsProvider>
  );
}

export default App;
