import React from "react";
import { Box, Typography, IconButton, Sheet } from "@mui/joy";
import GitHubIcon from "@mui/icons-material/GitHub";
import ArticleIcon from "@mui/icons-material/Article";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import { useColorScheme } from "@mui/joy/styles";

function ModeSwitcher() {
  const { mode, setMode } = useColorScheme();
  const [mounted, setMounted] = React.useState(false);

  React.useEffect(() => {
    setMounted(true);
  }, []);

  React.useEffect(() => {
    // Add or remove the 'dark' class on the body element
    if (mode === "dark") {
      document.body.classList.add("dark");
    } else {
      document.body.classList.remove("dark");
    }
  }, [mode]);

  if (!mounted) {
    return null;
  }

  const Icon = mode === "dark" ? LightModeIcon : DarkModeIcon;

  return (
    <IconButton
      component="a"
      onClick={() => setMode(mode === "dark" ? "light" : "dark")}
    >
      <Icon />
    </IconButton>
  );
}

const MotionHeader = ({ title }) => {
  return (
    <Sheet
      sx={{
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
        <ModeSwitcher />
        <IconButton
          component="a"
          href="https://github.com/dm4ml/motion"
          target="_blank"
        >
          <GitHubIcon />
        </IconButton>
        <IconButton
          component="a"
          href="https://dm4ml.github.io/motion/"
          target="_blank"
        >
          <ArticleIcon />
        </IconButton>
      </Box>
    </Sheet>
  );
};

export default MotionHeader;
