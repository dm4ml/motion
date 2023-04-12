import inspect
import sys
import threading
import typing
from abc import ABC, abstractmethod
from queue import SimpleQueue

from motion.cursor import Cursor
from motion.utils import TriggerElement, logger


class CustomDict(dict):
    def __init__(
        self,
        trigger_name: str,
        dict_type: str,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        self.trigger_name = trigger_name
        self.dict_type = dict_type
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: str) -> object:
        try:
            return super().__getitem__(key)
        except KeyError:
            raise KeyError(
                f"Key `{key}` not found in {self.dict_type} for trigger {self.trigger_name}."
            )


class Trigger(ABC):
    """A trigger is a class that defines the logic for a particular type of inference. Triggers are instantiated once per project and are responsible for maintaining their own state. Triggers are responsible for defining their setup, routes that they respond to, and the logic for infer and fit. See the [trigger life cycle](/concepts/trigger) for more information."""

    def __init__(
        self,
        cursor: Cursor,
        name: str,
        version: int,
        params: dict = {},
        routes_only: bool = False,
    ):
        self.name = name

        # Validate number of arguments in each trigger and set up routes
        route_list = self.routes()
        if not isinstance(route_list, list):
            raise TypeError(
                f"routes() of trigger {name} should return a list of motion.Route objects."
            )

        seen_keys = set()
        for r in route_list:
            if f"{r.relation}.{r.key}" in seen_keys:
                raise ValueError(
                    f"Duplicate route {r.relation}.{r.key} in trigger {name}."
                )

            r.validateTrigger(self)
            seen_keys.add(f"{r.relation}.{r.key}")

        self.route_map = {}
        for r in self.routes():
            if r.relation != "":
                self.route_map[f"{r.relation}.{r.key}"] = r
            else:
                self.route_map[f"_cron.{r.key}"] = r

        if routes_only:
            return

        # Set up params dictionary
        self._params = CustomDict(self.name, "params", params)

        # Set up initial state
        if len(inspect.signature(self.setUp).parameters) != 1:
            raise ValueError(f"setUp() of trigger {name} should have 1 argument")

        self._state = CustomDict(self.name, "state", {})
        self._version = version
        self._last_fit_id = -sys.maxsize - 1

        initial_state = self.setUp(cursor)
        if not isinstance(initial_state, dict):
            raise TypeError(f"setUp() of trigger {self.name} should return a dict.")
        self.update(initial_state)

        # Set up fit queue
        self._fit_queue = SimpleQueue()  # type: SimpleQueue
        self._fit_thread = threading.Thread(
            target=self.processFitQueue,
            daemon=True,
            name=f"{name}_fit_thread",
        )
        self._fit_thread.start()

    @classmethod
    def getRouteKeys(cls) -> list:
        obj: Trigger = cls(None, "", 0, routes_only=True)  # type: ignore

        return list(obj.route_map.keys())

    @abstractmethod
    def routes(self) -> list:
        """Specifies mappings from trigger keys to lifecycle functions (i.e., infer, fit methods).
        Returns:
            list: List of routes that this trigger responds to. Each route is a motion.Route object.
        """
        pass

    @abstractmethod
    def setUp(self, cursor: Cursor) -> dict:
        """Sets up the initial state of the trigger. Called only when the application is started.

        Args:
            cursor (Cursor): Cursor object to access the Motion data store.

        Raises:
            NotImplementedError: Error if this method is not implemented.

        Returns:
            dict: Initial state of the trigger.
        """
        raise NotImplementedError(f"Please implement setUp() for trigger {self.name}.")

    @property
    def params(self) -> dict:
        return self._params

    @property
    def state(self) -> dict:
        return self._state

    @property
    def version(self) -> int:
        return self._version

    @property
    def last_fit_id(self) -> int:
        return self._last_fit_id

    def update(self, new_state: dict) -> None:
        if new_state:
            self._state.update(new_state)
            self._version += 1

    def processFitQueue(self) -> None:
        while True:
            (
                cursor,
                trigger_name,
                trigger_context,
                infer_context,
                fit_event,
            ) = self._fit_queue.get()

            route = self.route_map[f"{trigger_context.relation}.{trigger_context.key}"]
            new_state = route.fit(cursor, trigger_context, infer_context)

            if not isinstance(new_state, dict):
                fit_event.set()
                raise TypeError(
                    f"fit() of trigger {self.name} should return a dict of state updates."
                )

            old_version = self.version
            self.update(new_state)

            logger.info(
                f"Finished running trigger {trigger_name} for identifier {trigger_context.identifier} and key {trigger_context.key}."
            )

            cursor.logTriggerExecution(
                trigger_name,
                old_version,
                route.fit.__name__,
                "FIT",
                trigger_context,
            )

            fit_event.set()

    def fitWrapper(
        self,
        cursor: Cursor,
        trigger_name: str,
        trigger_context: TriggerElement,
        infer_context: typing.Any,
    ) -> threading.Event:
        fit_event = threading.Event()
        self._fit_queue.put(
            (cursor, trigger_name, trigger_context, infer_context, fit_event)
        )

        return fit_event
