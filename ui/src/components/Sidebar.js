import React from "react";
import { Box, List, ListItem, Typography } from "@mui/joy";
import { NavLink } from "react-router-dom";
import Divider from "@mui/joy/Divider";

const Sidebar = ({ components }) => {
  // Define your active style
  const activeStyle = {
    textDecoration: "none",
    width: "100%",
    display: "block",
    backgroundColor: "#f0f0f0", // A light background color for active link
  };

  const inactiveStyle = {
    textDecoration: "none",
    color: "inherit",
    display: "block",
  };

  return (
    <Box
      sx={{
        width: "250px",
        bgcolor: "background.paper",
        borderRight: "1px solid #ddd",
        height: "100vh",
      }}
    >
      <List>
        {components.map((component) => (
          <ListItem key={component.id}>
            <NavLink
              to={`/${component.id}`}
              style={({ isActive }) => (isActive ? activeStyle : inactiveStyle)}
            >
              <Typography color="primary" variant="body">
                {component.name}
              </Typography>
            </NavLink>
            <Divider />
          </ListItem>
        ))}
      </List>
    </Box>
  );
};

export default Sidebar;
