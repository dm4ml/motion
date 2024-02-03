"""
This file serves a fastapi app that provides a dashboard for the motion system.
The dashboard is a web interface that allows users to inspect the state of
any component instance and edit the state of any component instance.
"""


from importlib import resources
from typing import List, Union

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from motion.df import MDataFrame
from motion.mtable import MTable
from motion.utils import get_components, get_instances, inspect_state, writeState

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


class UpdateInstanceRquest(BaseModel):
    key: str
    value: str


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
def list_instances(component: str) -> List[str]:
    """Lists all instances of a component."""
    return get_instances(component)


@dashboard_app.get("/instances/{component}/{search}")
def filter_instances(component: str, search: str = "") -> List[str]:
    """Lists all instances of a component."""
    instances = get_instances(component)
    instances = [instance for instance in instances if search in instance]

    return instances


@dashboard_app.get("/results/{component}/{instance_id}")
def retrieve_result_status(component: str, instance_id: str) -> List[str]:
    """Retrieves all results for a component instance: success, failure, or pending."""
    # TODO: fill this in
    return ["success"] * 100


@dashboard_app.post("/instance/{component}/{instance}")
def update_instance(
    component: str, instance: str, update: List[UpdateInstanceRquest]
) -> Response:
    """Updates the state of a component instance."""
    # Convert update to a dictionary
    update_dict = {kv.key: kv.value for kv in update}

    # Load the state and make sure types are compatible
    state = inspect_state(f"{component}__{instance}")

    for key, value in update_dict.items():
        if key in state:
            # If the type of the value is different from the type of the state,
            # convert the value to the type of the state
            try:
                if isinstance(state[key], int):
                    update_dict[key] = int(value)
                elif isinstance(state[key], float):
                    update_dict[key] = float(value)
                elif isinstance(state[key], str):
                    update_dict[key] = str(value)
                else:
                    raise ValueError(
                        f"Type for key `{key}` is not compatible with existing state."  # noqa: E501
                    )
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Type for key `{key}` is not compatible with existing state.",  # noqa: E501
                )

    # Now write the new state
    writeState(f"{component}__{instance}", update_dict)

    return Response(status_code=200)


@dashboard_app.get("/instance/{component}/{instance}")
def inspect_instance(component: str, instance: str) -> List[EditableStateKV]:
    """Inspects the state of a component instance."""
    state = inspect_state(f"{component}__{instance}")

    state_serialized = []

    for key, value in state.items():
        # If type is a string, int, or float, it is editable
        if isinstance(value, (str, int, float)):
            # Get the type of the value
            t = type(value)

            # Convert to a readable string
            if t == str:
                t = "string"
            elif t == int:
                t = "int"
            elif t == float:
                t = "float"
            else:
                t = str(t)

            state_serialized.append(
                EditableStateKV(key=key, value=str(value), editable=True, type=t)
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
                    key=key, value=str(value), editable=False, type=str(type(value))
                )
            )

    return state_serialized


def get_frontend_build_folder():
    # Determine the package directory
    package_dir = resources.files("motion")

    # Construct the path to the static files
    frontend_build_folder = package_dir / "static"

    return frontend_build_folder


# Mount the static files
dashboard_app.mount(
    "/", StaticFiles(directory=get_frontend_build_folder(), html=True), name="static"
)
