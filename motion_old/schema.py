import inspect
import typing
from collections import namedtuple
from datetime import date, datetime
from enum import Enum

import numpy as np
import pyarrow as pa
from pydantic import BaseModel, Extra

Field = namedtuple("Field", ["name", "type_"])


def type_to_name(t: type) -> str:
    return t.__name__.split(".")[-1].upper()


TYPE_TO_PA_TYPE = {
    int: pa.int64(),
    str: pa.string(),
    float: pa.float64(),
    bool: pa.bool_(),
    date: pa.date32(),
    datetime: pa.timestamp("us"),
    bytes: pa.binary(),
}


def get_arrow_type(t: type) -> pa.DataType:
    if t in TYPE_TO_PA_TYPE.keys():
        return TYPE_TO_PA_TYPE[t]

    elif inspect.isclass(t) and issubclass(t, Enum):
        # return pa_ext.EnumType.from_enum(t)
        return pa.string()

    elif typing.get_origin(t) == list:
        sub_t = typing.get_args(t)[0]
        return pa.list_(get_arrow_type(sub_t))

    elif typing.get_origin(t) == dict:
        kt = typing.get_args(t)[0]
        vt = typing.get_args(t)[1]
        return pa.map_(get_arrow_type(kt), get_arrow_type(vt))

    # Check if numpy type
    elif issubclass(t, np.generic) or issubclass(t, np.ndarray):
        return pa.from_numpy_dtype(t)

    # TODO: Add support for other types
    raise TypeError(f"Type {t} not supported.")


class Schema(BaseModel, extra=Extra.allow):
    """Schema for a Motion relation. Read more about relations in Motion [here](/concepts/relation).

    Example:

    ```python
    class User(Schema):
        name: str
        dob: date
        hometown: str
    ```

    All schemas also have the following fields, by default:
    - `identifier` (str): A unique identifier for each record in the relation.
    - `create_at` (datetime): The time at which the record was created.
    - `derived_id` (str): The identifier of the record that was derived from, if any.
    - `session_id` (str): The identifier of the session that created the record.

    Raises:
        TypeError: If a type is not supported. Supported types are: int, str, float, bool, date, datetime, bytes, list, dict, numpy types, and enums.
    """

    identifier: str
    derived_id: str
    create_at: datetime
    session_id: str

    @classmethod
    def formatPaSchema(cls, relation: str) -> pa.Schema:
        """Formats a pyarrow schema for the given relation. This is used internally by Motion.

        Returns:
            pa.Schema: Schema for table based on annotations.
        """

        fields = [Field(key, val) for key, val in cls.__annotations__.items()]
        user_defined_fields = [
            f
            for f in fields
            if not f.name == "identifier"
            and not f.name == "create_at"
            and not f.name == "derived_id"
            and not f.name == "session_id"
        ]
        pa_fields = [
            pa.field("identifier", pa.string()),
            pa.field(
                "create_at",
                pa.timestamp("us"),
            ),
            pa.field("derived_id", pa.string()),
            pa.field("session_id", pa.string()),
        ]

        for field in user_defined_fields:
            try:
                arrow_type = get_arrow_type(field.type_)
            except TypeError as e:
                raise TypeError(f"Error in {relation}.{field.name}: {e}")
            pa_fields.append(pa.field(field.name, arrow_type))

        schema = pa.schema(pa_fields)
        return schema
