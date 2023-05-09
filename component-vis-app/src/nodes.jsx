import React, { useState } from 'react';
import { Handle, Position } from 'reactflow';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { nord } from 'react-syntax-highlighter/dist/esm/styles/prism';

export function StateNode({ data, isConnectable }) {

    return (
        <div className="state-node">
        <div>
            <label className="node-label">state</label>
        </div>
        <Handle type="source" position={Position.Bottom} isConnectable={isConnectable} id="left"  style={{left: "70px"}}/>
        <Handle type="target" id="right" position={Position.Bottom} isConnectable={isConnectable}  style={{left: "90px"}} />
        </div>
    );
}

export function KeyNode({data, isConnectable}) {
    return (
        <div className="key-node">
        <div>
            <div className="node-rectangle" style={{backgroundColor: "#b8ffe9"}}>
                <div className="node-annotation">Key</div>
            </div>
            <label className='node-label'>{data.label}</label>
        </div>
        <Handle type="source" position={Position.Right} id="bottom" isConnectable={isConnectable} />
        </div>
    );
}

function SyntaxHighlighterWrapper({data, isExpanded}) {
    const codeLines = data.udf.split('\n');
    const visibleLines = isExpanded ? codeLines : codeLines.slice(0, 2);
    const visibleCode = visibleLines.join('\n');
    const opacity = isExpanded ? 1 : 0.25;

    return (
        <div style={{fontSize: "x-small", opacity: opacity}}>
            <SyntaxHighlighter 
                language="python" 
                style={nord} 
                wrapLongLines={true} 
            >
                {visibleCode}
            </SyntaxHighlighter>
        </div>
    );
}

function CodeNode({data, nodetype}) {

    const [isExpanded, setIsExpanded] = useState(false);

    const toggleExpand = () => {
        setIsExpanded(!isExpanded);
    };
    const color = nodetype === "Infer" ? "#ffe9b8" : "#ffb8ce";

    return (
        <div style={{width: "100%"}}>
            <div className="node-rectangle" style={{backgroundColor: color}}>
                <div className="node-annotation">{nodetype}</div>
            </div>
            <div className="code-title">
                <label>{data.label}</label>
                <div className="toggle-button-container">
                    <div className={`toggle-button ${isExpanded ? 'expanded' : ''}`} onClick={toggleExpand}>
                    <span className="arrow-icon" />
                    </div>
                </div>
            </div>
            {/* <pre style={{fontFamily: 'monospace'}}>{data.udf}</pre> */}
            <SyntaxHighlighterWrapper data={data} isExpanded={isExpanded} />
        </div>
    );
}


export function InferNode({data, isConnectable}) {

    return (
        <div className="code-node">
            <CodeNode data={data} nodetype="Infer" />
            <Handle type="target" position={Position.Top} id="top" isConnectable={isConnectable} />
            <Handle type="target" position={Position.Left} id="left" isConnectable={isConnectable} />
            <Handle type="source" position={Position.Right} id="right" isConnectable={isConnectable} />
        </div>
    );
}

export function FitNode({data, isConnectable}) {
    return (
        <div className="code-node">
            <CodeNode data={data} nodetype="Fit" />
        <Handle type="source" position={Position.Top} id="top" isConnectable={isConnectable} />

        <Handle type="target" position={Position.Left} id="left" isConnectable={isConnectable} />
        </div>
    );
}

