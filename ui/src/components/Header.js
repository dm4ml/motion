import React from "react";
import { Box, Typography } from "@mui/joy";

const Header = ({ title }) => {
  return (
    <Box
      sx={{
        bgcolor: "primary.main",
        // color: "white",
        p: 2,
        width: "100vw",
        position: "fixed",
        top: 0,
        left: 0,
      }}
    >
      <Typography level="h5" component="h1" sx={{ fontWeight: "bold" }}>
        {title}
      </Typography>
    </Box>
  );
};

export default Header;
