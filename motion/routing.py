import inspect
from typing import Callable, Dict, Union

from pydantic import BaseModel, root_validator

from motion.trigger import Trigger


class Route(BaseModel):
    relation: str
    key: str
    infer: Union[Callable, None] = None
    fit: Union[Callable, None] = None

    @root_validator()
    def key_has_no_periods(cls, values: Dict) -> Dict:
        relation = values.get("relation")
        key = values.get("key")

        if "." in key:  # type: ignore
            raise ValueError(f"Route key {key} cannot contain periods.")
        if "." in relation:  # type: ignore
            raise ValueError(f"Route relation {relation} cannot contain periods.")

        return values

    def validateTrigger(self, trigger_object: Trigger) -> None:
        if self.infer is not None:
            if getattr(trigger_object, self.infer.__name__, None) is None:
                raise ValueError(
                    f"Trigger {trigger_object.name} does not have an infer function named {self.infer.__name__}."
                )

            if len(inspect.signature(self.infer).parameters) != 2:
                raise ValueError(
                    f"Infer method {self.infer.__name__} should have 2 arguments: cursor and trigger_context."
                )

        if self.fit is not None:
            if getattr(trigger_object, self.fit.__name__, None) is None:
                raise ValueError(
                    f"Trigger {trigger_object.name} does not have a fit function named {self.fit.__name__}."
                )

            if len(inspect.signature(self.fit).parameters) != 3:
                raise ValueError(
                    f"Fit method {self.fit.__name__} should have 3 arguments: cursor, trigger_context, and infer_context."
                )
