import numpy as np
import pandas as pd
import typing

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class Type:
    def __array__(self) -> np.ndarray:
        return np.array(
            [getattr(self, field) for field in self.__dataclass_fields__]
        )
