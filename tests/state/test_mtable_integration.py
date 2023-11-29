"""
This file stores an MTable in a Motion component.
"""

from motion import Component, MTable
import pandas as pd
import pyarrow as pa
import numpy as np

C = Component("MyMTableComponent")


@C.init_state
def setUp():
    df = pd.DataFrame(
        {
            "vector": [
                np.array([1.0, 2.0], dtype=np.float64),
                np.array([2.0, 3.0], dtype=np.float64),
                np.array([3.0, 4.0], dtype=np.float64),
            ],
            "label": ["a", "b", "c"],
        }
    )
    table = MTable.from_pandas(df)
    return {"data": table}


D = Component("MyMTableComponent2")


@D.init_state
def setUp():
    df = pd.DataFrame({"value": [0, 1, 2]})
    table = MTable.from_pandas(df)

    # Set the filesystem
    fs = pa.fs.LocalFileSystem()
    table.filesystem = fs
    return {"data": table}


@C.serve("search")
def search(state, props):
    # Do nearest neighbor search
    vector = props["vector"]
    table = state["data"]
    result = table.knn("vector", vector, 2, "euclidean")
    return result


def test_read_and_write_data():
    c_instance = C()
    pyarrow_table = c_instance.read_state("data")
    assert pyarrow_table.data.to_pandas()["label"].tolist() == ["a", "b", "c"]

    # Add a column
    pyarrow_table.append_column("label2", pa.array(["d", "e", "f"]))
    c_instance.write_state({"data": pyarrow_table})

    # Check that the column was added
    pyarrow_table_2 = c_instance.read_state("data")
    assert pyarrow_table_2.data.to_pandas()[["label", "label2"]].to_dict(
        orient="list"
    ) == {
        "label": ["a", "b", "c"],
        "label2": ["d", "e", "f"],
    }


def test_search():
    c_instance = C()

    query_vector = np.array([1.0, 2.0], dtype=np.float64)
    result = c_instance.run("search", props={"vector": query_vector})
    labels = result.to_pandas()["label"].tolist()
    assert sorted(labels) == ["a", "b"]


def test_filesystem_write():
    d_instance = D("instance_1")
    # Assert we can close the instance and reread the state

    d_instance.shutdown()

    d_instance = D("instance_1")
    pyarrow_table = d_instance.read_state("data")

    assert pyarrow_table.data.to_pandas()["value"].tolist() == [0, 1, 2]
