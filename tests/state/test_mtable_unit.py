# import pytest
import pyarrow as pa
import pandas as pd
import numpy as np
from motion import MTable


class TestMTable:
    def test_create_from_pandas(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        table = MTable.from_pandas(df)
        assert table.data.num_rows == 3
        assert table.data.num_columns == 2

    def test_create_from_arrow(self):
        arrow_table = pa.Table.from_pandas(
            pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        )
        table = MTable.from_arrow(arrow_table)
        assert table.data.num_rows == 3
        assert table.data.num_columns == 2

    def test_add_row(self):
        table = MTable.from_schema(
            pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())])
        )
        table.add_row({"a": 1, "b": "x"})
        assert table.data.num_rows == 1

    def test_remove_row(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        table = MTable.from_pandas(df)
        table.data = table.remove_row(1)
        assert table.data.num_rows == 2

    def test_add_and_remove_column(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        table = MTable.from_pandas(df)
        table.append_column("b", pa.array(["x", "y", "z"]))
        assert table.data.num_columns == 2
        table.remove_column_by_name("b")
        assert table.data.num_columns == 1

    def test_knn(self):
        # Modified test with vectors of type float64
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
        print(table.data)

        result = table.knn(
            "vector", np.array([1.0, 2.0], dtype=np.float64), 2, "euclidean"
        )
        assert result.num_rows == 2
        assert "distances" in result.column_names

    def test_apply_distance(self):
        # Modified test with vectors of type float64
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

        # Apply the distance calculation
        query_point = np.array([1.0, 2.0], dtype=np.float64)
        metric = "euclidean"  # or any other metric your function supports
        result_table = table.apply_distance("vector", query_point, metric)

        # Check if the distances column is added
        assert "distances" in result_table.schema.names

        # Check the number of rows remains the same
        assert result_table.num_rows == table.data.num_rows
