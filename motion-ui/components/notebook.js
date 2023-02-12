import { Card, Spacer, Col, Grid, Container, Row, Button, Text, Textarea, Input, Loading, Tooltip } from "@nextui-org/react";
import { useState, useEffect, useMemo, useRef } from 'react';
import { useTheme } from '@nextui-org/react';
import { useCodeMirror } from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { bbedit } from '@uiw/codemirror-theme-bbedit';
import { IconTrash, IconX } from '@tabler/icons-react';


const templates = {
    "type": "@dataclass\nclass UntitledType:\n  attribute1: int\n  attribute2: float\n\n  def __array__(self) -> np.ndarray:\n    return np.array([getattr(self, field) for field in self.__dataclass_fields__])",
    "transform": "class Transform(motion.Transform):\n    featureType = ...\n    labelType = ...\n    returnType = ...\n\n    def setUp(self):\n        self.max_staleness = 0\n\n    def fit(self, features: typing.List[featureType], labels: typing.List[labelType]) -> dict:\n        # Do something here and return new state\n        state = {\"model\": ...}\n        return state\n\n    def infer(self, state, feature: featureType) -> returnType:\n        # Use state\n        return state[\"model\"]...",
    "free": "# Do whatever you'd like (read-only)\nprint('Hello world!')"
}

export function Cell({ cell, onDelete }) {

    if (!cell.active) {
        return null;
    }

    const { theme } = useTheme();
    const [focused, setFocused] = useState(false);
    const onFocus = () => setFocused(true);
    const onBlur = () => setFocused(false);


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

    const editor = useRef();

    const { setContainer } = useCodeMirror({
        container: editor.current,
        extensions: [python()],
        value: templates[type],
        theme: bbedit,
        basicSetup: {
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
        },
    });

    useEffect(() => {
        if (editor.current) {
            setContainer(editor.current);
        }
    }, [editor.current]);

    let borderColor = focused ? "$colors$primary" : "grey";
    let opacity = focused ? 1 : 1;


    return (
        <><Spacer y={1} />
            <Card variant="bordered" css={{ $$cardColor: colorAlpha, borderRadius: '$xs', borderColor: borderColor, opacity: opacity }} borderWeight="normal" onFocus={onFocus} onBlur={onBlur}>
                <Card.Body css={{ paddingTop: 5 }}>
                    <Row align="right" justify="space-between" css={{}} >
                        <Col>{ }</Col>
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
                    <div ref={editor} />
                </Card.Body>
            </Card>
        </>
    );
}