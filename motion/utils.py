import dataclasses


def dataclass_to_sql(dataclass):
    fields = dataclasses.fields(dataclass)
    return ", ".join([f"{field.name} {field.type}" for field in fields])
