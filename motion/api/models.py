from pydantic import BaseModel, Extra


class GetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifier: str
    keys: list

    @property
    def kwargs(self):
        return self.__dict__


class MgetRequest(BaseModel, extra=Extra.allow):
    relation: str
    identifiers: list
    keys: list

    @property
    def kwargs(self):
        return self.__dict__


class PartialSetRequest(BaseModel):
    relation: str
    identifier: str = None


class SetRequest(BaseModel):
    relation: str
    identifier: str = None
    key_values: dict


class SqlRequest(BaseModel, extra=Extra.allow):
    query: str
