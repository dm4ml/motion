from motion import Schema


class Child(Schema):
    query: str
    _page: int = 0


c1 = Child(id=1, query="hello")
print(Child.format_create_table_sql("test"))
