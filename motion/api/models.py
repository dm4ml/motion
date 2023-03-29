from __future__ import annotations

from pydantic import BaseModel, Extra


class GetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifier: str
    keys: list

    @property
    def kwargs(self) -> dict:
        return self.__dict__


class MgetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifiers: list
    keys: list

    @property
    def kwargs(self) -> dict:
        return self.__dict__


class PartialSetRequest(BaseModel):
    relation: str
    identifier: str = ""


class SetRequest(BaseModel):
    relation: str
    identifier: str = ""
    key_values: dict


class SqlRequest(BaseModel, extra=Extra.allow):
    query: str
    as_df: bool = True


class DuplicateRequest(BaseModel):
    relation: str
    identifier: str
