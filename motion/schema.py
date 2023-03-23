from datetime import date, datetime
from abc import ABC
from collections import namedtuple
from enum import Enum
from pydantic import BaseModel, Extra

import inspect
import pickle
import typing


class MEnum(Enum):
    @classmethod
    def list(cls):
        return [e.value for e in cls]


Field = namedtuple("Field", ["name", "type_"])


def type_to_name(t):
    return t.__name__.split(".")[-1].upper()


class Schema(BaseModel, extra=Extra.allow):
    identifier: str
    derived_id: str
    create_at: datetime
    session_id: str

    @classmethod
    def formatCreateStmts(cls, table_name: str) -> typing.List[str]:
        # Get fields

        fields = [Field(key, val) for key, val in cls.__annotations__.items()]

        user_defined_fields = [
            f
            for f in fields
            if not f.name == "identifier"
            and not f.name == "create_at"
            and not f.name == "derived_id"
            and not f.name == "session_id"
        ]

        names_and_types = [
            "identifier VARCHAR NOT NULL DEFAULT uuid()",  # TODO make this a primary key
            "derived_id VARCHAR DEFAULT NULL",
            "create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "session_id VARCHAR NOT NULL",
        ]
        enums = {}

        for field in user_defined_fields:
            # If type is int, string, float, bool, date, datetime, convert directly to SQL
            if field.type_ in [
                int,
                float,
                date,
            ]:
                names_and_types.append(
                    f"{field.name} {type_to_name(field.type_)}"
                )

            elif field.type_ == datetime:
                names_and_types.append(f"{field.name} DATETIME")
            elif field.type_ == str:
                names_and_types.append(f"{field.name} VARCHAR")
            elif field.type_ == bool:
                names_and_types.append(f"{field.name} BOOLEAN")
            elif inspect.isclass(field.type_) and issubclass(
                field.type_, MEnum
            ):
                enums[field.name] = [f"'{v}'" for v in field.type_.list()]
                names_and_types.append(f"{field.name} {field.name}")
            elif field.type_ == bytes:
                names_and_types.append(f"{field.name} BLOB")
            elif typing.get_origin(field.type_) == list:
                t = typing.get_args(field.type_)[0]

                if t == str:
                    names_and_types.append(f"{field.name} VARCHAR[]")
                elif t == int:
                    names_and_types.append(f"{field.name} INT[]")
                elif t == float:
                    names_and_types.append(f"{field.name} FLOAT[]")
                elif t == bool:
                    names_and_types.append(f"{field.name} BOOLEAN[]")
                else:
                    raise ValueError(f"Unsupported type {t} in list.")
            else:
                # Use bytes object to store pickled object
                names_and_types.append(f"{field.name} BLOB")

        create_enum_str = [
            f"CREATE TYPE {name} AS ENUM ({', '.join(values)});"
            for name, values in enums.items()
        ]

        create_table_str = (
            f"CREATE TABLE {table_name} ({', '.join(names_and_types)});"
        )

        return create_enum_str + [create_table_str]
