import React, { useEffect, useState } from "react";
import axios from "axios";
import {
  Box,
  Input,
  Typography,
  Stack,
  Card,
  CardContent,
  Chip,
  Modal,
  ModalDialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  CardActions,
  Divider,
} from "@mui/joy";
import WarningRoundedIcon from "@mui/icons-material/WarningRounded";
import DynamicTable from "./DynamicTable";

const MainContent = ({ componentName }) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [detailedInfo, setDetailedInfo] = useState([]); // Holds the detailed info as a list of key-value pairs
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [instances, setInstances] = useState([]);
  const [selectedItem, setSelectedItem] = useState(null);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    if (componentName) {
      axios
        .get(`/instances/${componentName}`)
        .then((response) => {
          setInstances(response.data);
        })
        .catch((error) => {
          console.error("Error fetching instances for", componentName, error);
        });
    }
  }, [componentName]);

  const handleCardClick = (item) => {
    // Fetch detailed information for the clicked instance
    axios
      .get(`/instance/${componentName}/${item.id}`) // Replace with your actual API endpoint
      .then((response) => {
        const detailedInfo = response.data;
        setDetailedInfo(detailedInfo);
        setSelectedItem(item);
        setIsModalOpen(true);
      })
      .catch((error) => {
        console.error("Error fetching details for instance", item.id, error);
      });
  };

  const handleEditConfirm = (item) => {
    // Reset error message at the start
    setErrorMessage("");

    // Send the updated detailedInfo to the backend
    axios
      .post(`/instance/${componentName}/${item.id}`, detailedInfo)
      .then((response) => {
        // Handle the response

        setIsModalOpen(false);
      })
      .catch((error) => {
        console.error("Error updating instance details", error);
        const message = error.response?.data?.detail;
        setErrorMessage(message);
      });
  };

  const handleDetailChange = (index, key, value) => {
    setDetailedInfo((prevDetails) =>
      prevDetails.map((detail, idx) =>
        idx === index ? { ...detail, [key]: value } : detail
      )
    );
  };

  const addNewKeyValuePair = () => {
    setDetailedInfo((prevDetails) => [
      ...prevDetails,
      { key: "", value: "", editable: true, type: "string" },
    ]);
  };

  const handleSearch = () => {
    axios
      .get(`/instances/${componentName}/${searchTerm}`)
      .then((response) => {
        setInstances(response.data);
      })
      .catch((error) => {
        console.error(
          "Error fetching instances for search term",
          searchTerm,
          error
        );
      });
  };

  return (
    <Box sx={{ p: 2 }}>
      <Typography level="h3">{componentName}</Typography>
      <Input
        placeholder="Search..."
        value={searchTerm}
        onChange={(e) => setSearchTerm(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            handleSearch();
          }
        }}
        sx={{ mb: 2, mt: 2 }}
      />
      <Stack spacing={2}>
        {instances.map((item, index) => (
          <Card
            key={index}
            variant="outlined"
            onClick={() => handleCardClick(item)}
            sx={{ cursor: "pointer", bgcolor: "primary.50" }}
          >
            <CardContent>
              <Typography sx={{ fontFamily: "monospace" }}>
                {item.id}
              </Typography>
            </CardContent>
            <CardActions>
              <Chip variant="soft" color="neutral" sx={{ marginLeft: "auto" }}>
                Last accessed: {item.lastAccessed}
              </Chip>
            </CardActions>
          </Card>
        ))}
      </Stack>

      {selectedItem && (
        <Modal open={isModalOpen} onClose={() => setIsModalOpen(false)}>
          <ModalDialog>
            <DialogTitle>
              <WarningRoundedIcon />
              {"Edit state for instance "}
              <Typography sx={{ fontFamily: "monospace" }}>
                {selectedItem.id}
              </Typography>
            </DialogTitle>
            <DialogContent>
              {errorMessage && (
                <Typography color="danger" sx={{ mt: 2 }}>
                  {errorMessage}
                </Typography>
              )}
              {detailedInfo.map((detail, index) => (
                <React.Fragment key={index}>
                  <Box
                    sx={{
                      display: "flex",
                      alignItems: "center",
                      gap: 2,
                      m: 1,
                    }}
                  >
                    <Chip
                      sx={{
                        fontFamily: "monospace",
                        width: "20%",
                      }}
                    >
                      {detail.type}
                    </Chip>
                    <Input
                      value={detail.key}
                      onChange={(e) =>
                        handleDetailChange(index, "key", e.target.value)
                      }
                      sx={{ fontWeight: "bold", width: "30%" }} // Style for keys
                    />
                    {/* Conditional rendering based on type and editable */}
                    {detail.type === "MDataFrame" ||
                    detail.type === "MTable" ? (
                      <DynamicTable tableData={detail.value} />
                    ) : detail.editable ? (
                      <Input
                        value={detail.value}
                        onChange={(e) =>
                          handleDetailChange(index, "value", e.target.value)
                        }
                        sx={{ fontStyle: "italic", width: "70%" }}
                      />
                    ) : (
                      <Typography
                        sx={{
                          fontStyle: "italic",
                          color: "text.secondary",
                          width: "70%",
                          // Opacity for non-editable values
                          opacity: 0.7,
                        }}
                      >
                        {detail.value}
                      </Typography>
                    )}
                  </Box>
                  {index < Object.entries(detailedInfo).length - 1 && (
                    <Divider />
                  )}
                </React.Fragment>
              ))}
              <Button onClick={addNewKeyValuePair}>
                Add New Key-Value Pair
              </Button>
            </DialogContent>
            <DialogActions>
              <Button
                variant="solid"
                color="success"
                onClick={() => handleEditConfirm(selectedItem)}
              >
                Confirm
              </Button>
              <Button
                variant="plain"
                color="neutral"
                onClick={() => setIsModalOpen(false)}
              >
                Cancel
              </Button>
            </DialogActions>
          </ModalDialog>
        </Modal>
      )}
    </Box>
  );
};

export default MainContent;
