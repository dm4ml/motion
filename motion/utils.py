import copy
import logging
from collections import namedtuple

import pyarrow as pa

logger = logging.getLogger(__name__)

TriggerElement = namedtuple(
    "TriggerElement", ["relation", "identifier", "key", "value"]
)
TriggerFn = namedtuple("TriggerFn", ["name", "fn"])

PRODUCTION_SESSION_ID = "PRODUCTION"


def update_params(mconfig: dict, params: dict) -> dict:
    """Updates the mconfig with the new trigger params.

    Args:
        mconfig (dict): config dict in mconfig.py
        params (dict): dict from trigger name to updated params.

    Returns:
        dict: new mconfig with updated params.
    """
    cp = copy.deepcopy(mconfig)
    cp["trigger_params"] = cp.get("trigger_params", {})

    for trigger_name, trigger_params in params.items():
        exists = False
        for trigger, existing_params in cp["trigger_params"].items():
            if trigger.__name__ == trigger_name:
                existing_params.update(trigger_params)
                exists = True
                break
        if not exists:
            cp["trigger_params"][trigger_name] = trigger_params

    return cp


def print_schema_error(
    application_name: str,
    relation_name: str,
    old_schema: pa.Schema,
    new_schema: pa.Schema,
) -> None:
    """Prints a schema error message.

    Args:
        application_name (str): name of the application
        relation_name (str): name of the relation
        old_schema (pa.schema): old schema
        new_schema (pa.schema): new schema
    """
    old_schema_str = old_schema.to_string(show_field_metadata=False)
    new_schema_str = new_schema.to_string(show_field_metadata=False)

    logger.error(
        f"Relation {relation_name} already exists with a different schema. Please clear the data store with `motion clear {application_name}` and try again.\n\tOld schema:\n{old_schema_str}\n\tNew schema:\n{new_schema_str}"
    )
