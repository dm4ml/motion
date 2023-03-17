import dataclasses
from dataclasses import dataclass
from datetime import date, datetime
from abc import ABC
from enum import Enum

import inspect
import pickle
import typing


class MEnum(Enum):
    @classmethod
    def list(cls):
        return [e.value for e in cls]


@dataclass(kw_only=True)
class Schema(ABC):
    identifier: str
    derived_id: str
    create_at: datetime

    def __init_subclass__(cls, **kwargs):
        return dataclass(cls, kw_only=True, **kwargs)

    def __post_init__(self):
        # Check id, ts are not None
        if self.identifier is None or self.create_at is None:
            raise ValueError("identifier and create_at must be defined.")

    @classmethod
    def formatCreateStmts(cls, table_name: str) -> typing.List[str]:
        fields = dataclasses.fields(cls)

        user_defined_fields = [
            f
            for f in fields
            if not f.name == "identifier"
            and not f.name == "create_at"
            and not f.name == "derived_id"
        ]

        names_and_types = [
            "identifier VARCHAR NOT NULL PRIMARY KEY DEFAULT uuid()",
            "derived_id VARCHAR DEFAULT NULL",
            "create_at DATETIME DEFAULT CURRENT_TIMESTAMP",
        ]
        enums = {}

        for field in user_defined_fields:
            # If type is int, string, float, bool, date, datetime, convert directly to SQL
            if field.type in [
                int,
                float,
                date,
                datetime,
            ] or isinstance(field.type, typing.TypeVar):
                names_and_types.append(
                    f"{field.name} {field.type.__name__.split('.')[-1].upper()}"
                )
            elif field.type == str:
                names_and_types.append(f"{field.name} VARCHAR")
            elif field.type == bool:
                names_and_types.append(f"{field.name} BOOLEAN")
            elif inspect.isclass(field.type) and issubclass(field.type, MEnum):
                enums[field.name] = [f"'{v}'" for v in field.type.list()]
                names_and_types.append(f"{field.name} {field.name}")
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
