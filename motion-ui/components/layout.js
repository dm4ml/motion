import { styled } from "@nextui-org/react"
import { Header } from './header';
// import { Cell } from './notebook';
import { useState, useEffect, useRef } from 'react';
import { Container } from "@nextui-org/react";
import dynamic from 'next/dynamic';


const Notebook = dynamic(() => import('./notebook'), {
    ssr: false
})


const Box = styled("div", {
    boxSizing: "border-box",
});

const templates = {
    "type": "@dataclass\nclass UntitledType:\n  attribute1: int\n  attribute2: float\n\n  def __array__(self) -> np.ndarray:\n    return np.array([getattr(self, field) for field in self.__dataclass_fields__])",
    "transform": "class Transform(motion.Transform):\n    featureType = ...\n    labelType = ...\n    returnType = ...\n\n    def setUp(self):\n        self.max_staleness = 0\n\n    def fit(self, features: typing.List[featureType], labels: typing.List[labelType]) -> dict:\n        # Do something here and return new state\n        state = {\"model\": ...}\n        return state\n\n    def infer(self, state, feature: featureType) -> returnType:\n        # Use state\n        return state[\"model\"]...",
    "free": "# Do whatever you'd like (read-only)\nprint('Hello world!')"
}


export function Layout({ children }) {


    const [cells, setCells] = useState([]);
    // const [notebookInstance, setNotebookInstance] = useState(1);
    // const [output, setOutput] = useState("(loading...)");

    // useEffect(() => {
    //     const run = async () => {
    //         const scriptText = "import numpy as np; np.__version__";
    //         const out = await runScript(scriptText);
    //         setOutput(out);
    //     }
    //     run();

    // }, []);

    function handleCodeChange(id, code) {
        let newList = [...cells];
        newList[id].code = code;
        setCells(newList);
    }

    const setHasRun = (id) => {
        let newList = [...cells];
        newList[id].hasRun = true;
        setCells(newList);
    }


    function handleAdd(type) {
        const newList = cells.concat({ type: type, id: cells.length, deleted: false, code: templates[type], hasRun: false });

        setCells(newList);

    }

    function handleDelete(id) {
        // TODO(shreyashankar): fix bug here
        // const newList = cells.filter((item) => item.id !== id);
        let newList = [...cells];
        newList[id].deleted = true;

        setCells(newList);
    }

    const clearCellRuns = () => {
        let newList = [...cells];
        newList = newList.map((item) => {
            item.hasRun = false;
            return item;
        }
        );
        setCells(newList);
    }


    return (
        <Box
            css={{
                maxW: "100%"
            }}
        >
            {/* <Header  onRunAllClick={() => { console.log("click"); notebookRef.current.restart() }} /> */}
            {/* <Container md> */}
            {/* {cells.map((item) => (
                    <Cell key={item.id} cell={item} onDelete={handleDelete} />
                ))} */}
            <Notebook cells={cells} onDelete={handleDelete} handleCodeChange={handleCodeChange} onNewClick={(type) => handleAdd(type)} setHasRun={setHasRun} clearCellRuns={clearCellRuns} />
            {/* </Container> */}
            {/* <p>{output}</p> */}
        </Box>
    );
}
