import React from "react";
import { NavLink, useLocation } from "react-router-dom";
import { Box, List, ListItemButton, ListSubheader, Typography } from "@mui/joy";
import Divider from "@mui/joy/Divider";

const Sidebar = ({ components }) => {
  const location = useLocation();

  return (
    <Box
      sx={{
        width: "250px",
        bgcolor: "background.paper",
        // borderRight: "1px solid #ddd",
        overflowY: "auto", // Makes the sidebar scrollable
      }}
    >
      <List component="nav" variant="plain">
        <ListSubheader sticky>Components</ListSubheader>
        {components.map((component) => (
          <React.Fragment key={component.name}>
            <ListItemButton
              component={NavLink}
              to={`/${component.name}`}
              selected={location.pathname === `/${component.name}`}
            >
              <Typography>{component.name}</Typography>
            </ListItemButton>
            <Divider />
          </React.Fragment>
        ))}
      </List>
    </Box>
  );
};

export default Sidebar;
