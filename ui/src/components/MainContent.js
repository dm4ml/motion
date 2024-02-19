import React, { useEffect, useState } from "react";
import axios from "axios";
import {
  Box,
  Input,
  Typography,
  Chip,
  Modal,
  ModalDialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  AccordionGroup,
  AccordionDetails,
  Divider,
  ButtonGroup,
  Tabs,
  Tab,
  TabPanel,
  TabList,
  Sheet,
  Skeleton,
} from "@mui/joy";
import WarningRoundedIcon from "@mui/icons-material/WarningRounded";
import DynamicTable from "./DynamicTable";
import Accordion from "@mui/joy/Accordion";
import AccordionSummary, {
  accordionSummaryClasses,
} from "@mui/joy/AccordionSummary";
import { Tracker, BarList, Title, Flex, Text } from "@tremor/react";
import ComponentInfoCard from "./ComponentInfoCard";
import DetailComponent from "./DetailComponent";

const MainContent = ({ componentName }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [accordionLoading, setAccordionLoading] = useState({}); // Holds the loading state for each accordion item
  const [searchTerm, setSearchTerm] = useState("");
  const [detailedInfo, setDetailedInfo] = useState([]); // Holds the detailed info as a list of key-value pairs
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [instances, setInstances] = useState([]);
  const [numInstances, setNumInstances] = useState(0);
  const [selectedItem, setSelectedItem] = useState(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [expanded, setExpanded] = useState([]);
  const [accordionData, setAccordionData] = useState({});
  const [flowCounts, setflowCounts] = useState([]);
  const [statusCounts, setStatusCounts] = useState({});
  const [statusChanges, setStatusChanges] = useState({});
  const [statusBars, setStatusBars] = useState([]);
  const [fractionUptime, setFractionUptime] = useState(null);

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
      setIsLoading(true); // Start loading
      axios
        .get(`/instances/${componentName}`)
        .then((response) => {
          // Get instanceIds and number of instances
          const instanceIds = response.data.instanceIds;
          const numInstances = response.data.numInstances;
          const flowCounts = response.data.flowCounts;
          const statusCounts = response.data.statusCounts;
          const statusChanges = response.data.statusChanges;
          const componentStatusBars = response.data.statusBarData;
          const fractionUptime = response.data.fractionUptime;

          setInstances(instanceIds);
          setNumInstances(numInstances);
          setflowCounts(flowCounts);
          setStatusCounts(statusCounts);
          setStatusChanges(statusChanges);
          setStatusBars(componentStatusBars);
          setFractionUptime(fractionUptime);

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
        })
        .finally(() => {
          setIsLoading(false); // Stop loading
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

    // Select editable parts of the detailedInfo
    const editableDetails = detailedInfo.filter((detail) => detail.editable);

    // Get the keys and values from the detailedInfo
    const updatedDetails = editableDetails.map((detail) => {
      return { key: detail.key, value: detail.value };
    });

    // Send the updated detailedInfo to the backend
    axios
      .post(`/instance/${componentName}/${item}`, updatedDetails)
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
        idx === index ? { ...detail, [key]: value } : detail,
      ),
    );
  };

  const addNewKeyValuePair = () => {
    setDetailedInfo((prevDetails) => [
      ...prevDetails,
      { key: "", value: "", editable: true, type: "string" },
    ]);
  };

  const handleSearch = () => {
    setIsLoading(true); // Start loading
    axios
      .get(`/instances/${componentName}/${searchTerm}`)
      .then((response) => {
        // Get instanceIds and number of instances
        const instanceIds = response.data.instanceIds;
        const numInstances = response.data.numInstances;
        const flowCounts = response.data.flowCounts;
        const statusCounts = response.data.statusCounts;
        const statusChanges = response.data.statusChanges;
        const componentStatusBars = response.data.statusBarData;
        const fractionUptime = response.data.fractionUptime;

        setInstances(instanceIds);
        setNumInstances(numInstances);
        setflowCounts(flowCounts);
        setStatusCounts(statusCounts);
        setStatusChanges(statusChanges);
        setStatusBars(componentStatusBars);
        setFractionUptime(fractionUptime);

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
        console.error(
          "Error fetching instances for search term",
          searchTerm,
          error,
        );
      })
      .finally(() => {
        setIsLoading(false); // Stop loading
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
    // Set isLoading for the item
    setAccordionLoading((prevLoading) => ({ ...prevLoading, [item]: true }));

    axios
      .get(`/results/${componentName}/${item}`)
      .then((response) => {
        setAccordionData((prevData) => ({
          ...prevData,
          [item]: response.data,
        }));
      })
      .catch((error) => {
        console.error("Error fetching data:", error);
        setAccordionData((prevData) => ({ ...prevData, [item]: "Error" }));
      })
      .finally(() => {
        setAccordionLoading((prevLoading) => ({
          ...prevLoading,
          [item]: false,
        }));
      });
  };

  return (
    <Box sx={{ p: 2 }}>
      <Tabs aria-label="Basic tabs" defaultValue={0}>
        <TabList>
          <Tab>Component Info</Tab>
          <Tab>Instances</Tab>
        </TabList>

        <TabPanel value={0}>
          <ComponentInfoCard
            componentName={componentName}
            numInstances={numInstances}
            statusCounts={statusCounts}
            statusChanges={statusChanges}
            fractionUptime={fractionUptime}
            flowCounts={flowCounts}
            statusBars={statusBars}
          />
        </TabPanel>

        <TabPanel value={1}>
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
          <Skeleton loading={isLoading} variant="overlay">
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
                    <Typography
                      sx={{
                        fontFamily: "monospace",
                        fontWeight: "bold",
                        fontSize: "1.1rem",
                      }}
                    >
                      {item}
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    <Skeleton
                      loading={
                        item in accordionLoading && accordionLoading[item]
                      }
                      variant="overlay"
                    >
                      {expanded.includes(item) && accordionData[item] && (
                        <Box
                          sx={{
                            display: "flex",
                            flexDirection: "column",
                            height: "100%",
                          }}
                        >
                          <Box sx={{ flexGrow: 1, mt: 2 }}>
                            {/* Status Section */}
                            <Box sx={{ mb: 2 }}>
                              <Title>Status</Title>
                              <Flex justifyContent="end">
                                <Text>
                                  Uptime {accordionData[item].fractionUptime}%
                                </Text>
                              </Flex>
                              <Tracker
                                data={accordionData[item].statusBarData}
                                className="mt-2"
                              />
                              <Flex sx={{ mt: 2 }}>
                                <Text>24 hours ago</Text>
                                <Text>Now</Text>
                              </Flex>
                            </Box>

                            {/* Analytics Section */}
                            <Box sx={{ mt: 2 }}>
                              <Title>Distribution</Title>
                              <Flex className="mt-2">
                                <Text>Flow</Text>
                                <Text># Runs</Text>
                              </Flex>
                              <BarList
                                data={accordionData[item].flowCounts}
                                className="mt-2"
                              />
                            </Box>
                          </Box>

                          <Box
                            sx={{
                              display: "flex",
                              justifyContent: "flex-end",
                              mt: 2,
                            }}
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
                              {`Version ${accordionData[item].version}`}{" "}
                            </Chip>
                          </Box>
                        </Box>
                      )}
                    </Skeleton>
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
                <Button
                  onClick={handlePreviousPage}
                  disabled={currentPage === 1}
                >
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
                    {detailedInfo.map((detail, index) => (
                      <React.Fragment key={index}>
                        <Sheet
                          sx={{
                            display: "grid",
                            gridTemplateColumns: "1fr 3fr minmax(80px, auto)", // Adjust the '100px' to your desired chip width
                            alignItems: "center",
                            gap: 2,
                            m: 1,
                          }}
                        >
                          <Input
                            value={detail.key}
                            onChange={(e) =>
                              handleDetailChange(index, "key", e.target.value)
                            }
                            sx={{
                              fontWeight: "bold",
                              // width: "30%",
                              gridColumn: "1",
                            }} // Style for keys
                          />
                          {/* Conditional rendering based on type and editable */}
                          {detail.type === "MDataFrame" ||
                          detail.type === "MTable" ? (
                            <DynamicTable
                              tableData={detail.value}
                              sx={{
                                fontStyle: "italic",
                                //   width: "60%",
                                gridColumn: "2",
                              }}
                            />
                          ) : detail.editable ? (
                            <Input
                              value={detail.value}
                              onChange={(e) =>
                                handleDetailChange(
                                  index,
                                  "value",
                                  e.target.value,
                                )
                              }
                              sx={{
                                fontStyle: "italic",
                                //   width: "60%",
                                gridColumn: "2",
                              }}
                            />
                          ) : (
                            <Typography
                              sx={{
                                gridColumn: "2",
                              }}
                            >
                              <DetailComponent detail={detail} />
                            </Typography>
                          )}
                          <Chip
                            sx={{
                              fontFamily: "monospace",
                              gridColumn: "3",
                            }}
                          >
                            {detail.type}
                          </Chip>
                        </Sheet>
                        {index < Object.entries(detailedInfo).length - 1 && (
                          <Divider />
                        )}
                      </React.Fragment>
                    ))}

                    <Button onClick={addNewKeyValuePair} variant="soft">
                      Add New Key-Value Pair
                    </Button>
                  </DialogContent>
                  {errorMessage && (
                    <Typography color="danger" sx={{ mt: 2 }}>
                      {errorMessage}
                    </Typography>
                  )}
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
          </Skeleton>
        </TabPanel>
      </Tabs>
    </Box>
  );
};

export default MainContent;
