"""
This file contains the MTable class, which represents
a table that can be used in a motion component instance
state.

It is a wrapper around a pyarrow table.
"""
import os
import secrets
from typing import Any, List, Optional, Union

import fastvs as fvs
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class MTable:
    """
    A class representing a table in a motion component instance state,
    acting as a wrapper around a PyArrow table. It provides functionalities
    for manipulating the table, including adding/removing rows and columns,
    and performing vector searches.

    Attributes:
        data (pa.Table): The PyArrow table data.
        filesystem (Optional[Any]): A filesystem object used for reading/writing data.
        identifier (Optional[str]): A unique identifier for the table.
        external (bool): Flag to indicate if the table is externally managed.
        _prefix (str): Internal prefix for storing data files.

    Methods:
        from_pandas: Class method to create an MTable instance from a pandas DataFrame.
        from_arrow: Class method to create an MTable instance from a PyArrow Table.
        from_schema: Class method to create an MTable instance from a PyArrow Schema.
        add_row: Adds a new row to the table.
        remove_row: Removes a row from the table by index.
        add_column: Adds a new column to the table at specified index.
        append_column: Appends a new column to the end of the table.
        remove_column: Removes a column from the table by index.
        remove_column_by_name: Removes a column from the table by name.
        knn: Performs k-nearest neighbor search on a specified vector column.
        apply_distance: Calculates distances for all rows in the table from a
            query point.
    """

    def __init__(
        self,
        data: pa.Table,
        filesystem: Optional[pa.fs.FileSystem] = None,
        identifier: Optional[str] = None,
        external: bool = True,
    ) -> None:
        """
        Initializes the MTable. You should not call this directly!

        Args:
            data (pa.Table): The data to be wrapped by the MTable instance.
            filesystem (Optional[pa.fs.FileSystem]): The filesystem to be used
                for reading/writing data.
            identifier (Optional[str]): A unique identifier for the table.
            external (bool): If True, the table is assumed to be externally
                managed and cannot be created directly.

        Raises:
            NotImplementedError: If external is True, indicating the table
                should be created through class methods.
        """
        if external:
            # Raise error asking to use the class methods for creating tables
            raise NotImplementedError("Use the class methods for creating tables")

        self._filesystem = filesystem
        self._identifier = identifier
        self._data = data
        self._prefix = os.path.expanduser("~/.motion")

        # Make the prefix directory if it doesn't exist
        if not os.path.exists(self._prefix):
            os.makedirs(self._prefix)

    @property
    def data(self) -> pa.Table:
        """Gets the PyArrow table data. You can modify this object directly."""
        return self._data

    @data.setter
    def data(self, data: pa.Table) -> None:
        """Sets the PyArrow table data. You can modify this object directly."""
        self._data = data

    @property
    def filesystem(self) -> Optional[pa.fs.FileSystem]:
        """
        Gets the filesystem object used for reading/writing data.
        The filesystem object must be an instance of a PyArrow FileSystem.
        """
        return self._filesystem

    @filesystem.setter
    def filesystem(self, filesystem: pa.fs.FileSystem) -> None:
        """Sets the filesystem object used for reading/writing data.
        The filesystem object must be an instance of a PyArrow FileSystem.
        This is optional, but required if you want to read/write data from/to
        the filesystem rather than serializing/deserializing the data in
        Redis or some other KV store.
        """
        self._filesystem = filesystem

    @property
    def identifier(self) -> Optional[str]:
        """Gets the unique identifier for the table."""
        return self._identifier

    @identifier.setter
    def identifier(self, identifier: str) -> None:
        """Sets the unique identifier for the table."""
        self._identifier = identifier

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> "MTable":
        """
        Creates an MTable instance from a pandas DataFrame.
        This may not be zero copy.

        Args:
            df (pd.DataFrame): The DataFrame to convert to an MTable.

        Returns:
            MTable: An MTable instance representing the DataFrame.
        """
        table = pa.Table.from_pandas(df)
        return cls(table, external=False)

    @classmethod
    def from_arrow(cls, data: pa.Table) -> "MTable":
        """
        Creates an MTable instance from a PyArrow Table. This is zero copy.

        Args:
            data (pa.Table): The PyArrow Table to convert to an MTable.

        Returns:
            MTable: An MTable instance representing the Table.
        """
        return cls(data, external=False)

    @classmethod
    def from_schema(cls, schema: pa.Schema) -> "MTable":
        """Creates an MTable instance from a PyArrow Schema.
        This will create an empty table with the specified schema.

        Args:
            schema (pa.Schema): The PyArrow Schema to create the table from.

        Returns:
            MTable: An MTable instance
        """
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
                identifier = "mtable_" + str(secrets.token_hex(12))
                identifier = self._prefix + "/" + identifier
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
        """
        Adds a new row to the table.

        Args:
            row (dict): A dictionary representing the row to add. Keys should
                match table column names.

        Raises:
            KeyError: If the provided row dictionary is missing data for any column.
            TypeError: If there is a data type mismatch or conversion error.
            Exception: For other generic exceptions.
        """
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
        """
        Removes a row from the table at the specified index.

        Args:
            i (int): The index of the row to be removed.

        Raises:
            IndexError: If the specified index is out of bounds.
        """
        if i < 0 or i >= self.data.num_rows:
            raise IndexError("Row index out of bounds")

        # Slice the table into two parts, excluding the row to be removed
        part1 = self.data.slice(0, i)
        part2 = self.data.slice(i + 1)

        # Concatenate the parts to form a new table
        return pa.concat_tables([part1, part2])

    def add_column(self, i: int, field_: Union[str, pa.Field], column: Any) -> None:
        """
        Adds a new column to the table at the specified index.

        Args:
            i (int): The index where the new column should be inserted.
            field_ (Union[str, pa.Field]): The name or PyArrow Field object
                representing the new column.
            column (Any): The data for the new column.

        Raises:
            ValueError: If a column with the same name already exists in the table.
        """
        # Check if the column already exists
        if field_ in self.data.schema.names:
            raise ValueError(f"Column '{field_}' already exists.")

        # Append the new column to the table
        self.data = self.data.add_column(i, field_, column)

    def append_column(self, field_: Union[str, pa.Field], column: Any) -> None:
        """
        Appends a new column to the end of the table.

        Args:
            field_ (Union[str, pa.Field]): The name or PyArrow Field object
                representing the new column.
            column (Any): The data for the new column.

        Raises:
            ValueError: If a column with the same name already exists in the table.
        """
        # Check if the column already exists
        if field_ in self.data.schema.names:
            raise ValueError(f"Column '{field_}' already exists.")

        # Append the new column to the table
        self.data = self.data.append_column(field_, column)

    def remove_column(self, i: int) -> None:
        """
        Removes a column from the table at the specified index.

        Args:
            i (int): The index of the column to be removed.
        """
        # Remove the column
        self.data = self.data.remove_column(i)

    def remove_column_by_name(self, name: str) -> None:
        """
        Removes a column from the table by its name.

        Args:
            name (str): The name of the column to be removed.
        """
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
        """
        Performs a k-nearest neighbors search on a vector column in the table.

        Args:
            vector_column_name (str): The name of the vector column to search against.
            query_point (Union[list, "np.ndarray"]): The query point for the search.
            k (int): The number of nearest neighbors to find.
            metric (str): The distance metric to use for the search. Can be one
                of: "euclidean", "manhattan", "cosine_similarity", and
                "dot_product".
            resulting_columns (Optional[List[str]]): A list of column names to
                include in the result.

        Returns:
            pa.Table: A new PyArrow Table containing the k-nearest neighbors,
                in order, and their distances.
        """

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
        """
        Applies a distance metric to each row in a specified vector column of
        the table.

        Args:
            vector_column_name (str): The name of the vector column.
            query_point (Union[list, "np.ndarray"]): The point to measure
                distances from.
            metric (str): The distance metric to use for the search. Can be one
                of: "euclidean", "manhattan", "cosine_similarity", and
                "dot_product".

        Returns:
            pa.Table: A new PyArrow Table with the distance calculation
                appended as a new column.
        """
        distances: pa.Array = fvs.apply_distance_arrow(
            self.data, vector_column_name, query_point, metric
        )
        new_table = self.data.append_column(
            pa.field("distances", pa.float64()), distances
        )
        return new_table
