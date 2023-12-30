import pandas as pd
import pyarrow as pa


class MDataFrame(pd.DataFrame):
    """Wrapper around pandas DataFrame that allows for pyarrow-based
    serialization. This is to be used in a motion component's state.

    Simply use this class instead of pandas DataFrame. For example:
    ```python
    from motion import MDataFrame, Component

    C = Component("MyDFComponent")

    @C.init_state
    def setUp():
        df = MDataFrame({"value": [0, 1, 2]})
        return {"df": df}
    ```
    """

    def __getstate__(self) -> dict:
        # Serialize with pyarrow
        table = pa.Table.from_pandas(self)
        # Convert the PyArrow Table to a PyArrow Buffer
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()

        buffer = sink.getvalue()
        return {"table": buffer}

    def __setstate__(self, state: dict) -> None:
        # Convert the PyArrow Buffer to a PyArrow Table
        buf = state["table"]
        reader = pa.ipc.open_stream(buf)
        df = reader.read_pandas()
        self.__init__(df)  # type: ignore
