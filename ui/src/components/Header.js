import React from "react";
import { Box, Typography, IconButton } from "@mui/joy";
import GitHubIcon from "@mui/icons-material/GitHub";
import ArticleIcon from "@mui/icons-material/Article";

const Header = ({ title }) => {
  return (
    <Box
      sx={{
        bgcolor: "primary.200",
        p: 2,
        width: "100%", // Changed from 100vw to 100%
        position: "fixed",
        top: 0,
        left: 0,
        zIndex: 2,
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        boxSizing: "border-box", // Ensure padding is included in the width
      }}
    >
      <Typography level="h2" sx={{ fontWeight: "bold" }}>
        {title}
      </Typography>
      <Box>
        <IconButton
          component="a"
          href="https://github.com/your-username/your-repo"
          target="_blank"
        >
          <GitHubIcon />
        </IconButton>
        <IconButton
          component="a"
          href="https://link-to-your-docs"
          target="_blank"
        >
          <ArticleIcon />
        </IconButton>
      </Box>
    </Box>
  );
};

export default Header;
