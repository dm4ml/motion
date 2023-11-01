import inspect
import logging
from multiprocessing import Pool
from typing import Callable, List, Optional, Tuple

import redis
from pydantic import BaseConfig, BaseModel, Field
from tqdm import tqdm

from motion.component import Component
from motion.dicts import State
from motion.utils import get_redis_params, loadState, saveState

logger = logging.getLogger(__name__)


def process_migration(
    instance_name: str,
    migrate_func: Callable,
    load_state_fn: Callable,
    save_state_fn: Callable,
) -> Tuple[str, Optional[Exception]]:
    try:
        rp = get_redis_params()
        redis_con = redis.Redis(
            **rp.dict(),
        )
        state = loadState(redis_con, instance_name, load_state_fn)
        new_state = migrate_func(state)
        assert isinstance(new_state, dict), (
            "Migration function must return a dict."
            + " Warning: partial progress may have been made!"
        )
        empty_state = State(
            instance_name.split("__")[0],
            instance_name.split("__")[1],
            {},
        )
        empty_state.update(new_state)
        saveState(empty_state, redis_con, instance_name, save_state_fn)
    except Exception as e:
        if isinstance(e, AssertionError):
            raise e
        else:
            return instance_name, e

    redis_con.close()
    return instance_name, None


class MigrationResult(BaseModel):
    instance_id: str = Field(..., description="Instance ID of the component")
    exception: Optional[Exception] = Field(
        None, description="Exception migration raised, if any"
    )

    class Config(BaseConfig):
        arbitrary_types_allowed = True


class StateMigrator:
    def __init__(self, component: Component, migrate_func: Callable) -> None:
        """Creates a StateMigrator object.

        Args:
            component (Component): Component to perform the migration for.
            migrate_func (Callable): Function to apply to the state of each
                instance of the component.

        Raises:
            TypeError: if component is not a valid Component
            ValueError: if migrate_func does not have exactly one parameter
        """

        # Type check
        if not isinstance(component, Component):
            raise TypeError("component must be a valid Component")

        signature = inspect.signature(migrate_func)
        parameters = signature.parameters
        if len(parameters) != 1:
            raise ValueError("migrate_func must have exactly one parameter (`state`)")

        self.component = component
        self.migrate_func = migrate_func

    def migrate(
        self, instance_ids: List[str] = [], num_workers: int = 4
    ) -> List[MigrationResult]:
        """Performs the migrate_func for component instances' states.
        If instance_ids is empty, then migrate_func is performed for all
        instances of the component.

        Args:
            instance_ids (List[str], optional):
                List of instance ids to perform migration for. Defaults to
                empty list.
            num_workers (int, optional):
                Number of workers to use for parallel processing the migration.
                Defaults to 4.

        Returns:
            List[MigrationResult]:
                List of objects with instance_id and exception keys, where
                exception is None if the migration was successful for that
                instance name.
        """
        # Read all the states

        rp = get_redis_params()
        redis_con = redis.Redis(
            **rp.dict(),
        )
        instance_names = [
            self.component.name + "__" + iid if "__" not in iid else iid
            for iid in instance_ids
        ]
        if not instance_names:
            instance_names = [
                key.decode("utf-8").replace("MOTION_STATE:", "")  # type: ignore
                for key in redis_con.keys(f"MOTION_STATE:{self.component.name}__*")
            ]

        if not instance_names:
            logger.warning(f"No instances for component {self.component.name} found.")

        # Create a process pool with 4 executors
        with Pool(num_workers) as executor:
            # Create a list of arguments for process_migration
            args_list = [
                (
                    instance_name,
                    self.migrate_func,
                    self.component._load_state_func,
                    self.component._save_state_func,
                )
                for instance_name in instance_names
            ]

            # Initialize the progress bar
            progress_bar = tqdm(
                total=len(args_list),
                desc=f"Migrating state for {self.component.name}",
                unit="instance",
            )

            # Process each key in parallel and update the progress bar
            # for each completed task
            results = []
            for result in executor.starmap(process_migration, args_list):
                results.append(result)
                progress_bar.update(1)

            # Close the progress bar
            progress_bar.close()

        # Strip component name from instance names
        redis_con.close()
        mresults = [
            MigrationResult(instance_id=instance_name.split("__")[-1], exception=e)
            for instance_name, e in results
        ]
        return mresults
