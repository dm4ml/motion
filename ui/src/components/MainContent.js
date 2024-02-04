import React, { useEffect, useState } from "react";
import axios from "axios";
import {
  Box,
  Input,
  Typography,
  Stack,
  //   Card,
  CardContent,
  Chip,
  Modal,
  ModalDialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  CardActions,
  AccordionGroup,
  AccordionDetails,
  Divider,
  ButtonGroup,
} from "@mui/joy";
import WarningRoundedIcon from "@mui/icons-material/WarningRounded";
import DynamicTable from "./DynamicTable";
import Accordion, { accordionClasses } from "@mui/joy/Accordion";
import AccordionSummary, {
  accordionSummaryClasses,
} from "@mui/joy/AccordionSummary";
import { Tracker, BarList } from "@tremor/react";

const MainContent = ({ componentName }) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [detailedInfo, setDetailedInfo] = useState([]); // Holds the detailed info as a list of key-value pairs
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [instances, setInstances] = useState([]);
  const [selectedItem, setSelectedItem] = useState(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [expanded, setExpanded] = useState([]);
  const [accordionData, setAccordionData] = useState({});

  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 100;
  const totalPages = Math.ceil(instances.length / itemsPerPage);

  const handleNextPage = () => {
    setCurrentPage((prevPage) => Math.min(prevPage + 1, totalPages));
  };

  const handlePreviousPage = () => {
    setCurrentPage((prevPage) => Math.max(prevPage - 1, 1));
  };

  const indexOfLastItem = currentPage * itemsPerPage;
  const indexOfFirstItem = indexOfLastItem - itemsPerPage;
  const currentItems = instances.slice(indexOfFirstItem, indexOfLastItem);

  useEffect(() => {
    if (componentName) {
      axios
        .get(`/instances/${componentName}`)
        .then((response) => {
          setInstances(response.data);

          // Set current page to 1 when the component changes
          setCurrentPage(1);

          // Set expanded to empty array when the component changes
          setExpanded([]);

          // Set selected item to null when the component changes
          setSelectedItem(null);

          // Set detailed info to empty array when the component changes
          setDetailedInfo([]);

          // Set accordion data to empty object when the component changes
          setAccordionData({});
        })
        .catch((error) => {
          console.error("Error fetching instances for", componentName, error);
        });
    }
  }, [componentName]);

  const handleCardClick = (item) => {
    // Fetch detailed information for the clicked instance
    axios
      .get(`/instance/${componentName}/${item}`)
      .then((response) => {
        const detailedInfo = response.data;
        setDetailedInfo(detailedInfo);
        setSelectedItem(item);
        setIsModalOpen(true);
      })
      .catch((error) => {
        console.error("Error fetching details for instance", item, error);
      });
  };

  const handleEditConfirm = (item) => {
    // Reset error message at the start
    setErrorMessage("");

    // Send the updated detailedInfo to the backend
    axios
      .post(`/instance/${componentName}/${item}`, detailedInfo)
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

  const handleAccordionChange = (item) => {
    setExpanded((prevExpanded) => {
      if (prevExpanded.includes(item)) {
        return prevExpanded.filter((i) => i !== item); // Collapse this item
      } else {
        fetchAccordionData(item); // Fetch data when expanding
        return [...prevExpanded, item]; // Expand this item
      }
    });
  };

  const fetchAccordionData = (item) => {
    axios
      .get(`/results/${componentName}/${item}`)
      .then((response) => {
        console.log("Fetched data for", item, response.data);
        setAccordionData((prevData) => ({
          ...prevData,
          [item]: response.data,
        }));
      })
      .catch((error) => {
        console.error("Error fetching data:", error);
        setAccordionData((prevData) => ({ ...prevData, [item]: "Error" }));
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
      <AccordionGroup>
        {currentItems.map((item, index) => (
          <Accordion
            key={index}
            expanded={expanded.includes(item)}
            onChange={() => handleAccordionChange(item)}
            sx={{
              [`& .${accordionSummaryClasses.indicator}`]: {
                transition: "0.2s",
              },
              [`& [aria-expanded="true"] .${accordionSummaryClasses.indicator}`]:
                {
                  transform: "rotate(45deg)",
                },
            }}
          >
            <AccordionSummary>
              <Typography sx={{ fontFamily: "monospace" }}>{item}</Typography>
            </AccordionSummary>
            <AccordionDetails>
              {expanded.includes(item) && accordionData[item] && (
                <Box
                  sx={{
                    display: "flex",
                    flexDirection: "column",
                    height: "100%",
                  }}
                >
                  <Box sx={{ flexGrow: 1 }}>
                    {" "}
                    {/* Content container */}
                    {accordionData[item].resultsByFlow && (
                      <BarList data={accordionData[item].resultsByFlow} />
                    )}
                    <Tracker
                      data={accordionData[item].statuses}
                      className="mt-2"
                    />
                  </Box>

                  <Box
                    sx={{ display: "flex", justifyContent: "flex-end", mt: 2 }}
                  >
                    {" "}
                    {/* Button container */}
                    <Button
                      variant="plain"
                      onClick={() => handleCardClick(item)}
                    >
                      Edit state
                    </Button>
                    <Chip variant="soft" sx={{ ml: 1 }}>
                      {`v${accordionData[item].version}`}{" "}
                    </Chip>
                  </Box>
                </Box>
              )}
            </AccordionDetails>
          </Accordion>
        ))}
      </AccordionGroup>

      <Box
        sx={{
          display: "flex",
          justifyContent: "flex-end",
          alignItems: "center",
          marginTop: 2,
        }}
      >
        <ButtonGroup variant="plain" color="primary">
          <Button onClick={handlePreviousPage} disabled={currentPage === 1}>
            Previous
          </Button>
          <Button
            onClick={handleNextPage}
            disabled={currentPage === totalPages}
          >
            Next
          </Button>
        </ButtonGroup>
        <Typography variant="body1" sx={{ marginLeft: 2 }}>
          Page {currentPage} of {totalPages}
        </Typography>
      </Box>

      {selectedItem && (
        <Modal open={isModalOpen} onClose={() => setIsModalOpen(false)}>
          <ModalDialog>
            <DialogTitle>
              <WarningRoundedIcon />
              {"Edit state for instance "}
              <Typography sx={{ fontFamily: "monospace" }}>
                {selectedItem}
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
              <Button onClick={addNewKeyValuePair} variant="soft">
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
