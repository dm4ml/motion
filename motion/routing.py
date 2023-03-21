from pydantic import BaseModel
from typing import Callable, Union

import inspect


class Route(BaseModel):
    namespace: str
    key: str
    infer: Union[Callable, None]
    fit: Union[Callable, None]

    def validate(self, trigger_object):
        if self.infer is not None:
            if getattr(trigger_object, self.infer.__name__, None) is None:
                raise ValueError(
                    f"Trigger {trigger_object.name} does not have an infer function named {self.infer.__name__}."
                )

            if len(inspect.signature(self.infer).parameters) != 2:
                raise ValueError(
                    f"Infer method {self.infer.__name__} should have 2 arguments: cursor and triggered_by."
                )

        if self.fit is not None:
            if getattr(trigger_object, self.fit.__name__, None) is None:
                raise ValueError(
                    f"Trigger {trigger_object.name} does not have a fit function named {self.fit.__name__}."
                )

            if len(inspect.signature(self.fit).parameters) != 2:
                raise ValueError(
                    f"Fit method {self.infer.__name__} should have 2 arguments: cursor and triggered_by."
                )
