from typing import Any, Callable

from pydantic import BaseModel, Field


class Route(BaseModel):
    key: str = Field(..., description="The keyword to which this route applies.")
    op: str = Field(..., description="The operation to perform.", regex="^(infer|fit)$")
    udf: Callable = Field(
        ...,
        description="The udf to call for the op. The udf should have 2 args:"
        + " `state` and `value` for `infer` and 3 arguments: `state`, "
        + "`values`, and `infer_results` for `fit`.",
    )

    def run(self, **kwargs: Any) -> Any:
        return self.udf(**kwargs)
