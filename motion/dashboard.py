"""
This file serves a fastapi app that provides a dashboard for the motion system.
The dashboard is a web interface that allows users to inspect the state of
any component instance and edit the state of any component instance.
"""

from importlib import resources
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from motion.dashboard_utils import (
    get_component_instance_usage,
    get_component_usage,
    writeState,
)
from motion.df import MDataFrame
from motion.mtable import MTable
from motion.utils import get_components, inspect_state

dashboard_app = FastAPI()

dashboard_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["*"] for all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class EditableStateKV(BaseModel):
    key: str
    # Value can be str or to_dict() of pandas DF
    value: Union[str, List[dict]]
    editable: bool
    type: str


class UpdateInstanceRequest(BaseModel):
    key: str
    value: str


class StatusTracker(BaseModel):
    color: str
    tooltip: str


class BarListType(BaseModel):
    name: str
    value: int


class ComponentInstanceUsage(BaseModel):
    version: int
    flowCounts: List[BarListType]
    statusBarData: List[StatusTracker]
    fractionUptime: Optional[float]


class ComponentUsage(BaseModel):
    numInstances: int
    instanceIds: List[str]
    flowCounts: List[BarListType]
    statusCounts: Dict[str, int]
    statusChanges: Dict[str, Dict[str, Any]]
    statusBarData: List[StatusTracker]
    fractionUptime: Optional[float]


@dashboard_app.get("/components")
def list_components() -> List[str]:
    """Lists all components."""
    component_list = get_components()

    # Filter all DEV: components
    component_list = [
        component for component in component_list if not component.startswith("DEV:")
    ]

    return component_list


@dashboard_app.get("/instances/{component}")
def list_instances(component: str) -> ComponentUsage:
    """Lists all instances of a component."""
    component_usage = get_component_usage(component)

    flowCounts = [
        BarListType(name=elem["flow"], value=elem["count"])
        for elem in component_usage["flowCounts"]
    ]
    cu = ComponentUsage(
        numInstances=component_usage["numInstances"],
        instanceIds=component_usage["instanceIds"],
        flowCounts=flowCounts,
        statusCounts=component_usage["statusCounts"],
        statusChanges=component_usage["statusChanges"],
        statusBarData=[
            StatusTracker(color=elem["color"], tooltip=elem["tooltip"])
            for elem in component_usage["statusBarData"]
        ],
        fractionUptime=component_usage["fractionUptime"],
    )
    return cu


@dashboard_app.get("/instances/{component}/{search}")
def filter_instances(component: str, search: str = "") -> ComponentUsage:
    """Lists all instances of a component."""
    component_usage = get_component_usage(component)
    instances = [
        instance for instance in component_usage["instanceIds"] if search in instance
    ]
    flowCounts = [
        BarListType(name=elem["flow"], value=elem["count"])
        for elem in component_usage["flowCounts"]
    ]
    cu = ComponentUsage(
        numInstances=len(instances),
        instanceIds=instances,
        flowCounts=flowCounts,
        statusCounts=component_usage["statusCounts"],
        statusChanges=component_usage["statusChanges"],
        statusBarData=[
            StatusTracker(color=elem["color"], tooltip=elem["tooltip"])
            for elem in component_usage["statusBarData"]
        ],
        fractionUptime=component_usage["fractionUptime"],
    )

    return cu


@dashboard_app.get("/results/{component}/{instance_id}")
def retrieve_result_status(component: str, instance_id: str) -> ComponentInstanceUsage:
    """Retrieves all results for a component instance: success, failure, or pending."""

    # Get the usage of the component instance
    usage = get_component_instance_usage(component, instance_id)
    version = usage["version"]

    flowCounts = [
        BarListType(name=elem["flow"], value=elem["count"])
        for elem in usage["flowCounts"]
    ]
    statusBarData = [
        StatusTracker(color=elem["color"], tooltip=elem["tooltip"])
        for elem in usage["statusBarData"]
    ]

    return ComponentInstanceUsage(
        version=version,
        flowCounts=flowCounts,
        statusBarData=statusBarData,
        fractionUptime=usage["fractionUptime"],
    )


@dashboard_app.post("/instance/{component}/{instance}")
def update_instance(
    component: str, instance: str, update: List[UpdateInstanceRequest]
) -> Response:
    """Updates the state of a component instance."""
    # Convert update to a dictionary
    update_dict: Dict[str, Any] = {kv.key: kv.value for kv in update}

    # Load the state and make sure types are compatible
    state = inspect_state(f"{component}__{instance}")

    if state is None:
        raise HTTPException(
            status_code=404,
            detail=f"Instance `{instance}` of component `{component}` does not exist.",
        )

    try:
        for key, value in update_dict.items():
            if key not in state:
                continue  # Skip keys that are not in the state

            original_value = state[key]

            if isinstance(original_value, bool):
                update_dict[key] = bool(value)
            elif isinstance(original_value, int):
                update_dict[key] = int(value)
            elif isinstance(original_value, float):
                update_dict[key] = float(value)
            elif isinstance(original_value, str):
                update_dict[key] = str(value)
            elif isinstance(original_value, list):
                # Convert value to a list and check if all elements
                # are of the correct type
                update_list = eval(value)  # Ensure value is a list
                if not isinstance(update_list, list):
                    raise TypeError("Value is not a list")
                update_dict[key] = update_list
            elif isinstance(original_value, dict):
                # Convert value to a dict and check if all values
                # are of the correct type
                update_dict_obj = eval(value)  # Ensure value is a dict
                if not isinstance(update_dict_obj, dict):
                    raise TypeError("Value is not a dict")
                update_dict[key] = update_dict_obj
            else:
                raise ValueError(
                    f"Type for key `{key}` is not compatible with existing state."
                )

        # Now write the new state
        writeState(f"{component}__{instance}", update_dict)

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )

    return Response(status_code=200)


@dashboard_app.get("/instance/{component}/{instance}")
def inspect_instance(component: str, instance: str) -> List[EditableStateKV]:
    """Inspects the state of a component instance."""
    state = inspect_state(f"{component}__{instance}")

    if state is None:
        raise HTTPException(
            status_code=404,
            detail=f"Instance `{instance}` of component `{component}` does not exist.",
        )

    state_serialized = []

    for key, value in state.items():
        # Check if value is of type list or dict and contains only primitive
        # types (str, int, float)
        if (
            isinstance(value, list)
            and all(isinstance(item, (str, int, float, bool)) for item in value)
        ) or (
            isinstance(value, dict)
            and all(
                isinstance(item, (str, int, float, bool)) for item in value.values()
            )
        ):
            # Process list or dict of primitives as editable
            state_serialized.append(
                EditableStateKV(
                    key=key, value=str(value), editable=True, type=type(value).__name__
                )
            )

        # If type is a string, int, or float, it is editable
        elif isinstance(value, (str, int, float, bool)):
            # Get the type of the value
            t = type(value)
            t_str = t.__name__

            # Convert to a readable string
            if t == str:
                t_str = "string"
            elif t == int:
                t_str = "int"
            elif t == float:
                t_str = "float"
            elif t == bool:
                t_str = "bool"

            state_serialized.append(
                EditableStateKV(key=key, value=str(value), editable=True, type=t_str)
            )

        # If type is an MDataframe or MTable, it is not editable,
        # But the serialized value should be a list of records
        elif isinstance(value, MDataFrame):
            state_serialized.append(
                EditableStateKV(
                    key=key,
                    value=value.astype(str).to_dict(orient="records"),
                    editable=False,
                    type="MDataFrame",
                )
            )

        elif isinstance(value, MTable):
            # value.data is a pyarrow.Table
            # Convert to df and then to_dict
            df = value.data.to_pandas().astype(str)
            state_serialized.append(
                EditableStateKV(
                    key=key,
                    value=df.to_dict(orient="records"),
                    editable=False,
                    type="MTable",
                )
            )

        else:
            state_serialized.append(
                EditableStateKV(
                    key=key, value=str(value), editable=False, type=type(value).__name__
                )
            )

    return state_serialized


def get_frontend_build_folder() -> str:
    # Determine the package directory
    package_dir = resources.files("motion")

    # Construct the path to the static files
    frontend_build_folder = package_dir / "static"

    return str(frontend_build_folder)


# Mount the static files
dashboard_app.mount(
    "/", StaticFiles(directory=get_frontend_build_folder(), html=True), name="static"
)
