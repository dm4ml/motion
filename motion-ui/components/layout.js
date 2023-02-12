import { styled } from "@nextui-org/react"
import { Header } from './header';
// import { Cell } from './notebook';
import { useState, useEffect } from 'react';
import { Container } from "@nextui-org/react";
import dynamic from 'next/dynamic';


const Cell = dynamic(() => import('./notebook'), {
    ssr: false
})


const Box = styled("div", {
    boxSizing: "border-box",
});

// const runScript = async (code) => {
//     const pyodide = await window.loadPyodide({
//         indexURL: "https://cdn.jsdelivr.net/pyodide/v0.22.1/full/"
//     });

//     return await pyodide.runPythonAsync(code);
// }



export function Layout({ children }) {


    const [cells, setCells] = useState([]);
    // const [output, setOutput] = useState("(loading...)");

    // useEffect(() => {
    //     const run = async () => {
    //         const scriptText = "import numpy as np; np.__version__";
    //         const out = await runScript(scriptText);
    //         setOutput(out);
    //     }
    //     run();

    // }, []);

    function handleAdd(type) {
        const newList = cells.concat({ type: type, id: cells.length, active: true });

        setCells(newList);

    }

    function handleDelete(id) {
        // TODO(shreyashankar): fix bug here
        // const newList = cells.filter((item) => item.id !== id);
        let newList = [...cells];
        newList[id].active = false;

        setCells(newList);
    }


    return (
        <Box
            css={{
                maxW: "100%"
            }}
        >
            <Header onNewClick={(type) => handleAdd(type)} />
            <Container md>
                {cells.map((item) => (
                    <Cell key={item.id} cell={item} onDelete={handleDelete} />
                ))}
            </Container>
            {/* <p>{output}</p> */}
        </Box >
    );
}
