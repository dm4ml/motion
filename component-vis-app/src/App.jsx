import { useState } from "react";
import ReactFlow, {
  MiniMap,
  Controls,
  useNodesState,
  useEdgesState,
  Panel,
  MarkerType,
} from "reactflow";
import { ToastContainer, toast } from "react-toastify";

import "react-toastify/dist/ReactToastify.css";
import "reactflow/dist/style.css";
import "./nodes.css";

import { StateNode, KeyNode, InferNode, FitNode } from "./nodes";

const rfStyle = {
  backgroundColor: "#B8CEFF",
};
const nodeTypes = {
  state: StateNode,
  key: KeyNode,
  infer: InferNode,
  fit: FitNode,
};

export default function App() {
  const [jsonData, setJsonData] = useState({});
  const [filename, setFilename] = useState("");

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    const reader = new FileReader();

    reader.onload = (e) => {
      try {
        const content = e.target.result;
        const parsedData = JSON.parse(content);
        setJsonData(parsedData);
        setFilename(file.name);
        setNodes(parsedData.nodes);
        setEdges(parsedData.edges);
        showToast(file.name, "success", "");
      } catch (error) {
        showToast(file.name, "error", error.message);
      }
    };

    reader.readAsText(file);
  };

  const showToast = (filename, result, message) => {
    const options = { theme: "colored" };
    if (result === "error") {
      toast.error(`Error parsing ${filename}: ${message}`, options);
      return;
    } else if (result === "success") {
      toast.success(`Sucessfully parsed ${filename}!`, options);
      return;
    }
  };

  const handleButtonClick = () => {
    document.getElementById("fileInput").click();
  };

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  let modEdges = edges.map((edge) => {
    return {
      ...edge,
      labelStyle: { fontFamily: "monospace" },
      markerEnd: {
        type: MarkerType.Arrow,
        width: 20,
        height: 20,
        color: "#000",
      },
      style: {
        strokeWidth: 2,
        stroke: "#000",
      },
    };
  });

  // const onConnect = useCallback((params) => setEdges((eds) => addEdge(params, eds)), [setEdges]);

  let uploadComponent = (
    <div
      style={{
        display: "flex",
        justifyContent: "flex-end",
        alignItems: "flex-end",
      }}
    >
      <input
        id="fileInput"
        type="file"
        style={{ display: "none" }}
        onChange={handleFileChange}
      />
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "flex-end",
        }}
      >
        <button style={{ marginBottom: "10px" }} onClick={handleButtonClick}>
          Upload JSON
        </button>
        <span style={{ fontSize: "x-small", fontStyle: "italic" }}>
          {filename}
        </span>
      </div>
      <ToastContainer style={{ fontSize: "small" }} />
      {/* <span style={{fontSize: "small"}}>{filename}</span> */}
    </div>
  );

  let nameComponent = (
    <div>
      <span>{jsonData.name}</span>
    </div>
  );

  return (
    <div style={{ width: "100vw", height: "100vh" }}>
      <ReactFlow
        nodes={nodes}
        edges={modEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        style={rfStyle}
        nodesDraggable={true}
        elementsSelectable={false}
      >
        <Panel
          position="top-left"
          style={{ fontSize: "xx-large", fontWeight: "bold" }}
        >
          {nameComponent}
        </Panel>
        <Panel
          position="top-right"
          style={{ fontSize: "medium", fontWeight: "bold" }}
        >
          {uploadComponent}
        </Panel>
        <Controls showFitView={false} showInteractive={false} />
        <MiniMap zoomable pannable />
      </ReactFlow>
    </div>
  );
}
