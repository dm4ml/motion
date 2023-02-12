import { Card, Spacer, Col, Grid, Container, Row, Button, Text, Textarea, Input, Loading, Tooltip } from "@nextui-org/react";
import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { useTheme } from '@nextui-org/react';
import { useCodeMirror } from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { bbedit } from '@uiw/codemirror-theme-bbedit';
import { IconTrash, IconX, IconPlayerPlayFilled } from '@tabler/icons-react';
import { usePython } from 'react-py';
import CodeMirror from '@uiw/react-codemirror';


const templates = {
    "type": "@dataclass\nclass UntitledType:\n  attribute1: int\n  attribute2: float\n\n  def __array__(self) -> np.ndarray:\n    return np.array([getattr(self, field) for field in self.__dataclass_fields__])",
    "transform": "class Transform(motion.Transform):\n    featureType = ...\n    labelType = ...\n    returnType = ...\n\n    def setUp(self):\n        self.max_staleness = 0\n\n    def fit(self, features: typing.List[featureType], labels: typing.List[labelType]) -> dict:\n        # Do something here and return new state\n        state = {\"model\": ...}\n        return state\n\n    def infer(self, state, feature: featureType) -> returnType:\n        # Use state\n        return state[\"model\"]...",
    "free": "# Do whatever you'd like (read-only)\nprint('Hello world!')"
}

export default function Cell({ cell, onDelete }) {

    if (!cell.active) {
        return null;
    }

    const { theme } = useTheme();
    const [focused, setFocused] = useState(false);
    const onFocus = () => setFocused(true);
    const onBlur = () => setFocused(false);
    const [input, setInput] = useState('');
    const { runPython, stdout, stderr, isLoading, isRunning } = usePython();


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

    // const editor = useRef();

    // const { setContainer } = useCodeMirror({
    //     container: editor.current,
    //     extensions: [python()],
    //     value: templates[type],
    //     theme: bbedit,
    //     basicSetup: {
    //         lineWrapping: true,
    //         lineNumbers: true,
    //         highlightActiveLineGutter: true,
    //         highlightSelectionMatches: true,
    //         syntaxHighlighting: true,
    //         bracketMatching: true,
    //         highlightActiveLine: true,
    //         closeBrackets: true,
    //         autocompletion: true,
    //         highlightSpecialChars: true,
    //         history: true,
    //         closeBracketsKeymap: true,
    //         defaultKeymap: true,
    //         searchKeymap: true,
    //         historyKeymap: true,
    //         foldKeymap: true,
    //         completionKeymap: true,
    //         lintKeymap: true,
    //         dropCursor: true,
    //         foldGutter: true,
    //         rectangularSelection: true,
    //         crosshairCursor: true,
    //     },
    // });

    // useEffect(() => {
    //     if (editor.current) {
    //         setContainer(editor.current);
    //     }
    //     console.log("editor", editor)
    // }, [editor.current]);

    // const onUpdate = EditorView.updateListener.of((v) => {
    //     setCode(v.state.doc.toString());
    // });

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

    let output = stderr !== "" ? stderr : stdout;
    let outputColor = stderr !== "" ? "$colors$error" : "$colors$black";
    let outputElement = output !== "" ? <Text blockquote color={outputColor} css={{}}>{output}</Text> : null;

    return (
        <><Spacer y={1} />
            <Card variant="bordered" css={{ $$cardColor: colorAlpha, borderRadius: '$xs', borderColor: borderColor, opacity: opacity }} borderWeight="normal" onFocus={onFocus} onBlur={onBlur}>
                <Card.Body css={{ paddingTop: 5 }}>
                    <Row align="right" justify="space-between" css={{}} >
                        <Col>{ }</Col>
                        <Tooltip content={"Run cell"}>
                            <span role="button" title="Run cell" >
                                <IconPlayerPlayFilled size={14} onClick={(e) => {
                                    e.preventDefault();
                                    console.log(code);
                                    runPython(code);
                                }} />
                            </span>
                        </Tooltip>
                        <Spacer x={0.5} />
                        <Tooltip content={"Delete cell"}>
                            <span role="button" title="Delete cell" onClick={() => onDelete(cell.id)}>
                                <IconTrash size={14} />
                            </span>
                        </Tooltip>
                        {/* <Button
                            auto light color="default"
                            icon={<IconX size="12px" />}
                            onClick={() => onDelete(cell.id)} /> */}
                    </Row>
                    {/* <div ref={editor} /> */}
                    <CodeMirror
                        value={code}
                        theme={bbedit}
                        extensions={[python()]}
                        basicSetup={basicSetup}
                        onChange={onCodeChange}
                    />
                </Card.Body>
            </Card>
            {outputElement}
        </>
    );
}