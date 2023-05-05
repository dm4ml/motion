from pydantic import BaseModel, Extra


class GetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifier: str
    keys: list


class MgetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifiers: list
    keys: list


class PartialSetRequest(BaseModel):
    relation: str
    identifier: str = ""


class SetRequest(BaseModel):
    relation: str
    identifier: str = ""
    key_values: dict


class SqlRequest(BaseModel):
    query: str
    as_df: bool = True


class DuplicateRequest(BaseModel):
    relation: str
    identifier: str


class WaitRequest(BaseModel):
    trigger: str
