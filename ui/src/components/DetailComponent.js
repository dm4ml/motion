import React, { useState } from "react";
import { Typography, IconButton, Box, Snackbar } from "@mui/joy";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";

export default function DetailComponent({ detail }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [open, setOpen] = useState(false);

  const truncateText = (text, maxLength) => {
    if (text.length > maxLength) {
      return `${text.substring(0, maxLength)}...`;
    }
    return text;
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(detail.value);
    // Add snackbar to show that the text was copied
    setOpen(true);
  };

  const handleExpandToggle = () => {
    setIsExpanded(!isExpanded);
  };

  return (
    <Box>
      <Typography
        sx={{
          fontStyle: "italic",
          color: "text.secondary",
          width: "70%",
          flexGrow: 1, // Allows the text to fill the space
          opacity: 0.7,
        }}
      >
        {isExpanded ? detail.value : truncateText(detail.value, 100)}
      </Typography>
      <IconButton
        size="sm"
        variant="plain"
        onClick={handleExpandToggle}
        sx={{ marginLeft: "auto" }}
      >
        {isExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
      </IconButton>
      <IconButton size="sm" variant="plain" onClick={handleCopy}>
        <ContentCopyIcon />
      </IconButton>
      <Snackbar
        autoHideDuration={3000}
        open={open}
        variant="soft"
        color="primary"
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
        onClose={(event, reason) => {
          if (reason === "clickaway") {
            return;
          }
          setOpen(false);
        }}
      >
        {"Copied " + detail.key + " value to clipboard"}
      </Snackbar>
    </Box>
  );
}
