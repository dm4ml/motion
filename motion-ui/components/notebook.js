import { Card, Spacer, Col, Grid, Container, Row, Button, Text, Textarea, Input, Loading, Tooltip } from "@nextui-org/react";
import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useTheme } from '@nextui-org/react';
import { python } from '@codemirror/lang-python';
import { bbedit } from '@uiw/codemirror-theme-bbedit';
import { IconTrash, IconX, IconPlayerPlayFilled, IconClearAll } from '@tabler/icons-react';
import { usePythonConsole } from 'react-py';
import { ConsoleState } from 'react-py/dist/types/Console'
import CodeMirror from '@uiw/react-codemirror';


const templates = {
    "type": "@dataclass\nclass UntitledType:\n  attribute1: int\n  attribute2: float\n\n  def __array__(self) -> np.ndarray:\n    return np.array([getattr(self, field) for field in self.__dataclass_fields__])",
    "transform": "class Transform(motion.Transform):\n    featureType = ...\n    labelType = ...\n    returnType = ...\n\n    def setUp(self):\n        self.max_staleness = 0\n\n    def fit(self, features: typing.List[featureType], labels: typing.List[labelType]) -> dict:\n        # Do something here and return new state\n        state = {\"model\": ...}\n        return state\n\n    def infer(self, state, feature: featureType) -> returnType:\n        # Use state\n        return state[\"model\"]...",
    "free": "# Do whatever you'd like (read-only)\nprint('Hello world!')"
}

export function Cell({ cell, onDelete, activeCell, setActiveCell, runPython, isLoading, isRunning, cellOutputs, clearCellOutputs }) {

    if (cell.deleted) {
        return null;
    }

    const { theme } = useTheme();
    const [focused, setFocused] = useState(false);
    const onFocus = () => { setFocused(true); setActiveCell(cell.id); }
    const onBlur = () => { setFocused(false); setActiveCell(-1); }
    // const { runPython, stdout, stderr, isLoading, isRunning, banner, consoleState } = usePythonConsole({ packages: { official: ['numpy', 'pandas'] } });
    // const [output, setOutput] = useState([])

    // useEffect(() => {
    //     stdout && setOutput((prev) => [...prev, { "text": stdout, "color": "$colors$default" }])
    // }, [stdout])

    // useEffect(() => {
    //     stderr && setOutput((prev) => [...prev, { "text": stderr, "color": "$colors$error" }])
    //     // setOutput(stderr)
    // }, [stderr])


    const type = cell.type;

    let color = "$colors$success";
    let colorAlpha = "$colors$successLightActive";
    if (type === "type") {
        color = "$colors$primary";
        colorAlpha = theme.colors.primaryLightActive.value;
    } else if (type === "transform") {
        color = "$colors$secondary";
        colorAlpha = theme.colors.secondaryLightActive.value;
    }

    const basicSetup = {
        lineWrapping: true,
        lineNumbers: true,
        highlightActiveLineGutter: true,
        highlightSelectionMatches: true,
        syntaxHighlighting: true,
        bracketMatching: true,
        highlightActiveLine: true,
        closeBrackets: true,
        autocompletion: true,
        highlightSpecialChars: true,
        history: true,
        closeBracketsKeymap: true,
        defaultKeymap: true,
        searchKeymap: true,
        historyKeymap: true,
        foldKeymap: true,
        completionKeymap: true,
        lintKeymap: true,
        dropCursor: true,
        foldGutter: true,
        rectangularSelection: true,
        crosshairCursor: true,
    };

    let borderColor = focused ? "$colors$primary" : "grey";
    let opacity = focused ? 1 : 1;

    const [code, setCode] = useState(templates[type]);
    const onCodeChange = useCallback((value, viewUpdate) => {
        setCode(value);
    }, []);

    // let output = stderr !== "" ? stderr : stdout;
    // console.log("output", output)
    // let outputColor = stderr !== "" ? "$colors$error" : "$colors$neutral";
    console.log("cellOutputs", cellOutputs)
    let currCellOutputs = cellOutputs.filter((o) => (o.id === cell.id) && (o.show === true));
    console.log("currCellOutputs", currCellOutputs)
    let outputElement = currCellOutputs.length > 0 ?
        <>
            <Card.Divider /> <Card.Footer css={{ paddingTop: 5 }} >
                <Col>
                    <Row justify="flex-end"> <Tooltip content={'Clear outputs'} >
                        <span role="button" title="Clear outputs" style={{ "cursor": "pointer" }} >
                            <IconClearAll size={14} onClick={() => clearCellOutputs(cell.id)} />
                        </span>
                    </Tooltip></Row>
                    {currCellOutputs.map((line, i) => (<Row key={"outputrow_" + i}>
                        <Text css={{ fontFamily: "$mono", color: line.color }} className="output" key={"outputline_" + i} size="small">
                            {line.text}
                        </Text>
                        {/* <Spacer y={0.5} /> */}
                    </Row>
                    ))}
                </Col>
                {/* <Text css={{ fontFamily: "$mono" }}>{output}</Text> */}
            </Card.Footer>
        </> : null;

    async function run(code) {
        await runPython(code);
    }


    let playOrLoading = (isRunning || isLoading) ? <Loading type="points" size="xs" /> : <span role="button" title="Run cell" style={{ "cursor": "pointer" }} >
        <IconPlayerPlayFilled size={16} onClick={(e) => {
            e.preventDefault();
            run(code);
        }} />
    </span >;
    let playOrLoadingTooltip = (isRunning || isLoading) ? "Running" : "Run cell";

    return (
        <><Spacer y={1} />
            <Card key={"cell" + cell.id} variant="bordered" css={{ $$cardColor: colorAlpha, borderRadius: '$xs', borderColor: borderColor, opacity: opacity }} borderWeight="normal" onFocus={onFocus} onBlur={onBlur}>
                <Card.Body css={{ paddingTop: 5 }}>
                    <Row align="right" justify="flex-end" css={{}} >
                        {/* <Col>{ }</Col> */}
                        {/* <Tooltip content={playOrLoadingTooltip}>
                            {playOrLoading}
                        </Tooltip>
                        <Spacer x={0.5} /> */}
                        <Tooltip content={"Delete cell"} >
                            <span role="button" style={{ "cursor": "pointer" }} title="Delete cell" onClick={() => onDelete(cell.id)}>
                                <IconTrash size={14} />
                            </span>
                        </Tooltip>
                        {/* <Button
                            auto light color="default"
                            icon={<IconX size="12px" />}
                            onClick={() => onDelete(cell.id)} /> */}
                    </Row>
                    <Row justify="center" >
                        <Col css={{ width: "fit-content", verticalAlign: "middle" }}>
                            <Tooltip content={playOrLoadingTooltip}>
                                {playOrLoading}
                            </Tooltip>
                        </Col>
                        <Spacer x={0.5} />
                        <Col >
                            <CodeMirror
                                value={code}
                                theme={bbedit}
                                extensions={[python()]}
                                basicSetup={basicSetup}
                                onChange={onCodeChange}
                            />
                        </Col>
                    </Row>
                </Card.Body>
                {outputElement}
                {/* <Card.Divider />
                <Card.Footer> {outputElement} </Card.Footer> */}
            </Card>
            {/* {outputElement} */}
        </>
    );
}

export default function Notebook({ cells, onDelete }) {
    const { runPython, stdout, stderr, isLoading, isRunning, banner, consoleState } = usePythonConsole({ packages: { official: ['numpy', 'pandas'] } });
    const [cellOutputs, setCellOutputs] = useState([]);
    const [activeCell, setActiveCell] = useState(-1);

    useEffect(() => {
        stdout && setCellOutputs((prev) => [...prev, { "id": activeCell, "text": stdout, "color": "$colors$default", "show": true }])
    }, [stdout])

    useEffect(() => {
        stderr && setCellOutputs((prev) => [...prev, { "id": activeCell, "text": stderr, "color": "$colors$error", "show": true }])
        // setOutput(stderr)
    }, [stderr])

    const clearCellOutputs = (id) => {
        let newList = [...cells];
        newList = newList.map((item) => {
            if (item.id === id) {
                item.show = false;
            }
            return item;
        }
        );
        setCellOutputs(newList);
    }

    return (
        <>
            {cells.map((item) => (
                <Cell key={"notebookcell" + item.id} cell={item} onDelete={onDelete} activeCell={activeCell} setActiveCell={setActiveCell} runPython={runPython} isLoading={isLoading} isRunning={isRunning} cellOutputs={cellOutputs} clearCellOutputs={clearCellOutputs} />
            ))}
        </>
    );
}