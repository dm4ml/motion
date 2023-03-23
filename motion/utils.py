import copy
import dataclasses
import logging

logger = logging.getLogger(__name__)


def dataclass_to_sql(dataclass):
    fields = dataclasses.fields(dataclass)
    return ", ".join([f"{field.name} {field.type}" for field in fields])


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
