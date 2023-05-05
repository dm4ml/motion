import inspect
from typing import Any, Callable, Dict

from pydantic import BaseModel, Field, root_validator


class Route(BaseModel):
    key: str = Field(..., description="The keyword to which this route applies.")
    op: str = Field(..., description="The operation to perform.", regex="^(infer|fit)$")
    udf: Callable = Field(
        ...,
        description="The udf to call for the op. The udf should have 2 args:"
        + " `state` and `value` for `infer` and 3 arguments: `state`, "
        + "`value`, and `infer_result` for `fit`.",
    )

    @root_validator()
    def validateOp(cls, values: Dict) -> Dict:
        udf = values.get("udf")
        op = values.get("op")
        if op == "infer" and len(inspect.signature(udf).parameters) != 2:
            raise ValueError(
                f"Infer method {udf.__name__} should have 2 arguments `state` "
                + "and `value`"
            )

        if op == "fit" and len(inspect.signature(udf).parameters) != 3:
            raise ValueError(
                f"Fit method {udf.__name__} should have 3 arguments: `state`, "
                + "`values`, and `infer_results`."
            )
        return values

    def run(self, **kwargs: Dict[str, Any]) -> Any:
        return self.udf(**kwargs)
