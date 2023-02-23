import dataclasses
from dataclasses import dataclass
from datetime import date, datetime
from abc import ABC

import pickle


@dataclass(kw_only=True)
class Schema(ABC):
    id: int
    create_at: datetime = dataclasses.field(default_factory=datetime.now)

    def __init_subclass__(cls, **kwargs):
        return dataclass(cls, kw_only=True, **kwargs)

    @classmethod
    def _get_page_fields(cls):
        return [
            field.name
            for field in dataclasses.fields(cls)
            if field.name.endswith("_page")
        ]

    def __post_init__(self):
        # Check id, ts are not None
        if self.id is None or self.create_at is None:
            raise ValueError("id and create_at must be defined.")

        # Create unique key
        # self.unique_key = "_".join(
        #     [str(self.id)]
        #     + [str(getattr(self, f)) for f in self._get_page_fields()]
        # )

    @classmethod
    def format_create_table_sql(cls, table_name: str):
        fields = dataclasses.fields(cls)
        page_fields = [
            field.name for field in fields if field.name.endswith("_page")
        ]

        user_defined_fields = [
            f
            for f in fields
            if not f.name == "id"
            and not f.name == "create_at"
            and not f.name == "unique_key"
        ]

        names_and_types = ["id INT", "create_at DATETIME"]
        for field in user_defined_fields:
            # If type is int, string, float, bool, date, datetime, convert directly to SQL
            if field.type in [
                int,
                str,
                float,
                bool,
                date,
                datetime,
            ]:
                names_and_types.append(
                    f"{field.name} {field.type.__name__.split('.')[-1].upper()}"
                )
            else:
                # Use bytes object to store pickled object
                names_and_types.append(f"{field.name} BLOB")

        primary_keys = ["id"] + [name for name in page_fields]
        primary_key_str = f"PRIMARY KEY ({', '.join(primary_keys)})"

        create_table_str = f"CREATE TABLE {table_name} ({', '.join(names_and_types)}, {primary_key_str})"

        return create_table_str
