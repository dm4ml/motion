"""
This file contains the MTable class, which represents
a table that can be used in a motion component instance
state.

It is a wrapper around a pyarrow table.
"""
import secrets
from typing import Any, List, Optional, Union

import fastvs as fvs
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class MTable:
    def __init__(
        self,
        data: pa.Table,
        filesystem: Optional[Any] = None,
        identifier: Optional[str] = None,
        external: bool = True,
    ) -> None:
        if external:
            # Raise error asking to use the class methods for creating tables
            raise NotImplementedError("Use the class methods for creating tables")

        self._filesystem = filesystem
        self._identifier = identifier
        self._data = data

    @property
    def data(self) -> pa.Table:
        return self._data

    @data.setter
    def data(self, data: pa.Table) -> None:
        self._data = data

    @property
    def filesystem(self) -> Optional[Any]:
        return self._filesystem

    @filesystem.setter
    def filesystem(self, filesystem: Any) -> None:
        self._filesystem = filesystem

    @property
    def identifier(self) -> Optional[str]:
        return self._identifier

    @identifier.setter
    def identifier(self, identifier: str) -> None:
        self._identifier = identifier

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> "MTable":
        table = pa.Table.from_pandas(df)
        return cls(table, external=False)

    @classmethod
    def from_arrow(cls, data: pa.Table) -> "MTable":
        return cls(data, external=False)

    @classmethod
    def from_schema(cls, schema: pa.Schema) -> "MTable":
        # Creates an empty table from a schema
        table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )
        return cls(table, external=False)

    def __getstate__(self) -> dict:
        # If filesystem is set, then write to the filesystem
        if self.filesystem is not None:
            # Append parquet to the identifier
            identifier = self.identifier
            if not identifier:
                # Create a random identifier phrase
                identifier = str(secrets.token_hex(12))

            identifier = identifier + ".parquet"
            pq.write_table(self.data, identifier, filesystem=self.filesystem)
            return {
                "identifier": identifier,
                "data": None,
                "filesystem": self.filesystem,
            }

        # Convert the PyArrow Table to a PyArrow Buffer
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, self.data.schema)
        writer.write_table(self.data)
        writer.close()

        buffer = sink.getvalue()
        return {"data": buffer, "identifier": None, "filesystem": None}

    def __setstate__(self, state: dict) -> None:
        # If from filesystem, then read from the filesystem
        if state["filesystem"]:
            self.__init__(  # type: ignore
                pq.read_table(state["identifier"], filesystem=state["filesystem"]),
                identifier=state["identifier"],
                filesystem=state["filesystem"],
                external=False,
            )  # type: ignore
            return

        # Convert the PyArrow Buffer to a PyArrow Table
        buf = state["data"]
        reader = pa.ipc.open_stream(buf)
        table = reader.read_all()
        self.__init__(table, external=False)  # type: ignore

    # Methods to add rows and columns
    def add_row(self, row: dict) -> None:
        try:
            # Create a dictionary with the same schema structure
            # but with the new row's data
            new_row_data = {field.name: [row[field.name]] for field in self.data.schema}

            # Create a new table from the row data
            new_row_table = pa.Table.from_pydict(new_row_data, schema=self.data.schema)

            # Concatenate the existing table with the new row
            self.data = pa.concat_tables([self.data, new_row_table])

        except KeyError as e:
            raise KeyError(f"Error: Missing data for column '{e.args[0]}'.")
        except TypeError as e:
            raise TypeError(
                f"Error: Data type mismatch or conversion error. Details: {e}"
            )
        except Exception as e:
            raise e

    def remove_row(self, i: int) -> pa.Table:
        if i < 0 or i >= self.data.num_rows:
            raise IndexError("Row index out of bounds")

        # Slice the table into two parts, excluding the row to be removed
        part1 = self.data.slice(0, i)
        part2 = self.data.slice(i + 1)

        # Concatenate the parts to form a new table
        return pa.concat_tables([part1, part2])

    def add_column(self, i: int, field_: Union[str, pa.Field], column: Any) -> None:
        # Check if the column already exists
        if field_ in self.data.schema.names:
            raise ValueError(f"Column '{field_}' already exists.")

        # Append the new column to the table
        self.data = self.data.add_column(i, field_, column)

    def append_column(self, field_: Union[str, pa.Field], column: Any) -> None:
        # Check if the column already exists
        if field_ in self.data.schema.names:
            raise ValueError(f"Column '{field_}' already exists.")

        # Append the new column to the table
        self.data = self.data.append_column(field_, column)

    def remove_column(self, i: int) -> None:
        # Remove the column
        self.data = self.data.remove_column(i)

    def remove_column_by_name(self, name: str) -> None:
        # Remove the column
        self.data = self.data.remove_column(self.data.schema.get_field_index(name))

    # Vector search methods
    def knn(
        self,
        vector_column_name: str,
        query_point: Union[list, "np.ndarray"],
        k: int,
        metric: str,
        resulting_columns: Optional[List[str]] = None,
    ) -> pa.Table:
        indices, distances = fvs.search_arrow(
            self.data, vector_column_name, query_point, k, metric
        )

        # Slice the table for each index and store the slices
        slices = [self.data.slice(i, 1) for i in indices]
        resulting_table = (
            pa.concat_tables(slices)
            if slices
            else pa.Table.from_schema(self.data.schema)
        )

        # Add the distances column to the resulting table
        resulting_table = resulting_table.append_column(
            pa.field("distances", pa.float64()), pa.array(distances)
        )

        if resulting_columns:
            resulting_columns.append("distances")
            resulting_table = resulting_table.select(resulting_columns)

        return resulting_table

    def apply_distance(
        self,
        vector_column_name: str,
        query_point: Union[list, "np.ndarray"],
        metric: str,
    ) -> pa.Table:
        distances: pa.Array = fvs.apply_distance_arrow(
            self.data, vector_column_name, query_point, metric
        )
        new_table = self.data.append_column(
            pa.field("distances", pa.float64()), distances
        )
        return new_table
