import dataclasses
import logging

logger = logging.getLogger(__name__)


def dataclass_to_sql(dataclass):
    fields = dataclasses.fields(dataclass)
    return ", ".join([f"{field.name} {field.type}" for field in fields])
