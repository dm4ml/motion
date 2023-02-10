import { Card, Spacer, Container, Row, Text, Textarea, Input, Loading } from "@nextui-org/react";
import { useState, useEffect, useMemo, useRef } from 'react';
import { useTheme } from '@nextui-org/react';
import { useCodeMirror } from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { bbedit } from '@uiw/codemirror-theme-bbedit';


const templates = {
    "type": "@dataclass\nclass UntitledType:\n  attribute1: int\n  attribute2: float\n\n  def __array__(self) -> np.ndarray:\n    return np.array([getattr(self, field) for field in self.__dataclass_fields__])",
    "transform": "class Transform(motion.Transform):\n    featureType = ...\n    labelType = ...\n    returnType = ...\n\n    def setUp(self):\n        self.max_staleness = 0\n\n    def fit(self, features: typing.List[featureType], labels: typing.List[labelType]) -> dict:\n        # Do something here and return new state\n        state = {\"model\": ...}\n        return state\n\n    def infer(self, state, feature: featureType) -> returnType:\n        # Use state\n        return state[\"model\"]...",
    "free": "# Do whatever you'd like (read-only)\nprint('Hello world!')"
}

export function Cell({ cell, isActive }) {
    const { theme } = useTheme();


    const type = cell.type;

    let color = "$colors$success";
    let colorAlpha = "$colors$success";
    if (type === "type") {
        color = "$colors$primary";
        colorAlpha = theme.colors.primaryLight.value;
    } else if (type === "transform") {
        color = "$colors$secondary";
        colorAlpha = theme.colors.secondaryLight.value;
    }

    let variant = isActive ? "shadow" : "bordered";
    let opacity = isActive ? 1 : 0.5;
    let borderColor = isActive ? "$colors$primary" : "$colors$greyColor";

    const editor = useRef();

    const { setContainer } = useCodeMirror({
        container: editor.current,
        extensions: [python()],
        value: templates[type],
        readOnly: !isActive,
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


    return (
        <><Spacer y={1} />
            <Card variant={variant} css={{ $$cardColor: color, borderRadius: '$xs', opacity: opacity, borderColor: borderColor }} borderWeight="light">
                <Card.Body>
                    <div ref={editor} />
                </Card.Body>
            </Card>
        </>
    );
}