from typing import Any
from motion.component import Component
from motion.utils import (
    UpdateEventGroup,
    clear_instance,
    inspect_state,
    get_instances,
    RedisParams,
)
from motion.instance import ComponentInstance
from motion.migrate import StateMigrator
from motion.copy_utils import copy_db

__all__ = [
    "Component",
    "UpdateEventGroup",
    "ComponentInstance",
    "clear_instance",
    "inspect_state",
    "StateMigrator",
    "get_instances",
    "copy_db",
    "RedisParams",
]

# Conditionally import Application
try:
    from motion.server.application import Application

    __all__.append("Application")
except ImportError:

    class ApplicationImportError(ImportError):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            message = (
                "The 'Application' class requires additional dependencies. "
                "Please install the 'application' extras by running: "
                "`pip install motion[application]`"
            )
            super().__init__(message, *args, **kwargs)

    class Application:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ApplicationImportError()

    __all__.append("Application")

# Conditionally import MDataFrame and MTable
try:
    from motion.df import MDataFrame
    from motion.mtable import MTable

    __all__.extend(["MDataFrame", "MTable"])
except ImportError:

    class TableImportError(ImportError):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            message = (
                "The 'MDataFrame' and 'MTable' classes require additional dependencies. "
                "Please install the 'table' extras by running: "
                "`pip install motion[table]`"
            )
            super().__init__(message, *args, **kwargs)

    class MDataFrame:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TableImportError()

    class MTable:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TableImportError()

    __all__.extend(["MDataFrame", "MTable"])
