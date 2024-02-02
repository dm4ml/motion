import React, { useState } from "react";
import { Box, Input, Table, Typography } from "@mui/joy";

const MainContent = ({ componentName }) => {
  const [searchTerm, setSearchTerm] = useState("");
  const data = [
    { name: "Item 1", lastModified: "2024-01-01" },
    { name: "Item 2", lastModified: "2024-01-02" },
    // ... more data
  ];

  return (
    <Box sx={{ p: 2 }}>
      <Typography level="h4">
        This is the main page for {componentName}
      </Typography>
      <Input
        placeholder="Search..."
        value={searchTerm}
        onChange={(e) => setSearchTerm(e.target.value)}
        sx={{ mb: 2 }}
      />
      <Table aria-label="simple table">
        <thead>
          <tr>
            <th>
              <Typography>Name</Typography>
            </th>
            <th>
              <Typography>Last Modified</Typography>
            </th>
          </tr>
        </thead>
        <tbody>
          {data.map((item, index) => (
            <tr key={index}>
              <td>
                <Typography>{item.name}</Typography>
              </td>
              <td>
                <Typography>{item.lastModified}</Typography>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>
    </Box>
  );
};

export default MainContent;
