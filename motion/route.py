import inspect
import logging
from typing import Any, Callable, Dict

from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class Route(BaseModel):
    key: str = Field(..., description="The keyword to which this route applies.")
    op: str = Field(
        ...,
        description="The operation to perform.",
        pattern="^(serve|update)$",
    )
    udf: Callable = Field(
        ...,
        description="The udf to call for the op. The udf should have at least "
        + "a `state` argument.",
    )
    _udf_params: Dict[str, Any] = PrivateAttr()

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        udf_params = inspect.signature(self.udf).parameters
        self._udf_params = {param: udf_params[param].default for param in udf_params}

    def run(self, **kwargs: Any) -> Any:
        filtered_kwargs = {
            param: kwargs[param] for param in self._udf_params if param in kwargs
        }
        try:
            result = self.udf(**filtered_kwargs)
        except Exception as e:
            logger.error(f"Error in {self.key}, {self.op} flow: {e}")
            raise e

        return result
