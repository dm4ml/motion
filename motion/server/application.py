"""
This file creates a FastAPI application instance for a group of components."""

import secrets
from typing import Any, Callable, Dict, List

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator

from motion.component import Component


# Pydantic model for request payload
class RunRequest(BaseModel):
    """
    A Pydantic model representing a request to run a component within the application.

    Attributes:
        instance_id (str): The unique identifier for the component instance.
        dataflow_key (str): A key representing the specific dataflow or
            operation to be run on the component.
        is_async (bool): Flag to indicate if the operation should be performed
            asynchronously. Use this if you would call the component with
            `component.arun` instead of `component.run` (i.e., the operation is
            an async function). Default is False.
        props (Dict[str, Any]): A dictionary of properties specific to the
            component's dataflow that you want to run.
        run_kwargs (Dict[str, Any]): Additional keyword arguments to pass to
            the run method of the component.
        creation_kwargs (Dict[str, Any]): Keyword arguments to pass for
            component creation.
    """

    instance_id: str
    dataflow_key: str
    is_async: bool = False
    props: Dict[str, Any]
    run_kwargs: Dict[str, Any] = {}  # kwargs to pass to the run method
    creation_kwargs: Dict[str, Any] = {}  # kwargs to pass to component creation

    @validator("props")
    def validate_props(cls: Any, v: Any) -> Dict[str, Any]:
        if not isinstance(v, dict):
            raise ValueError("Props must be a dictionary")
        return v


class UpdateStateRequest(BaseModel):
    """
    A Pydantic model representing a request to update the state of a component.

    Attributes:
        instance_id (str): The unique identifier for the component instance.
        state_update (Dict[str, Any]): A dictionary representing the state
            updates to be applied to the component.
        kwargs (Dict[str, Any]): Additional keyword arguments relevant to the
            state update.
    """

    instance_id: str
    state_update: Dict[str, Any]
    kwargs: Dict[str, Any]


# Authentication dependency
def api_key_auth(secret_token: str) -> Callable:
    """
    Dependency for API key authentication. Validates the provided API key
    against the expected secret token.

    Args:
        secret_token (str): The secret token used for API key validation.

    Returns:
        Callable: A function that validates the API key in the request header.

    Raises:
        HTTPException: If the API key is not provided or is invalid.
    """

    def validate_api_key(request: Request) -> bool:
        if "Authorization" not in request.headers:
            raise HTTPException(
                status_code=401, detail="No Authorization header provided"
            )

        api_key = request.headers["Authorization"].split("Bearer ")[-1]
        expected_key = f"{secret_token}"
        if secrets.compare_digest(api_key, expected_key):
            return True
        else:
            raise HTTPException(
                status_code=403, detail="Could not validate credentials"
            )

    return validate_api_key


class Application:
    """
    The main application class that sets up FastAPI routes for the given
        components.

    Attributes:
        components (List[Component]): A list of component instances to be
            included in the application.
        secret_token (str): The secret token used for API key validation.

    Methods:
        get_app: Returns the FastAPI app instance.
        get_credentials: Returns the application's credentials, including the
            secret token.
    """

    def __init__(self, components: List[Component], secret_token: str = "") -> None:
        """
        Initializes the Application instance.

        Args:
            components (List[Component]): List of component instances to be
                managed by the application.
            secret_token (str, optional): Secret token for API key
                authentication. If not provided, a new token is generated.
        """
        self.app = FastAPI()
        self.components = components
        self.secret_token = (
            secret_token if secret_token else "sk_" + str(secrets.token_urlsafe(32))
        )
        self._generate_routes()

    def _generate_routes(self) -> None:
        """
        Generates API routes for each component in the application. It sets up
        endpoints for component dataflows and state management.
        """
        for component in self.components:
            component_name = component.name
            endpoint = self.create_component_endpoint(component)
            route = f"/{component_name}"
            self.app.post(route)(endpoint)

            update_route = f"/{component_name}/update"
            read_route = f"/{component_name}/read"

            self.app.post(update_route)(self.create_write_state_endpoint(component))
            self.app.get(read_route)(self.create_read_state_endpoint(component))

    def create_read_state_endpoint(self, component: Component) -> Callable:
        """
        Creates an endpoint for reading the state of a given component. This is
        called automatically when an application is created.

        Args:
            component (Component): The component instance for which to create
                the read state endpoint.

        Returns:
            Callable: An asynchronous function that serves as the endpoint for
                state reading.
        """

        async def read_state_endpoint(
            instance_id: str,
            key: str,
            _: Any = Depends(api_key_auth(self.secret_token)),  # type: ignore
        ) -> JSONResponse:
            try:
                with component(
                    instance_id, disable_update_task=True
                ) as component_instance:
                    value = component_instance.read_state(key)
                    # Return as JSON
                    return JSONResponse(content={key: value})
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        return read_state_endpoint

    def create_write_state_endpoint(self, component: Component) -> Callable:
        """
        Creates an endpoint for updating the state of a given component. This
        is called automatically when an application is created.

        Args:
            component (Component): The component instance for which to create
                the write state endpoint.

        Returns:
            Callable: An asynchronous function that serves as the endpoint for
                state updating.
        """

        async def write_state_endpoint(
            request: UpdateStateRequest,
            _: Any = Depends(api_key_auth(self.secret_token)),  # type: ignore
        ) -> Response:
            instance_id = request.instance_id
            state_update = request.state_update
            kwargs = request.kwargs

            # Assuming you have a method in your Component class to handle
            # state updates
            try:
                with component(
                    instance_id, disable_update_task=True
                ) as component_instance:
                    component_instance.write_state(state_update, **kwargs)
                    return Response(
                        status_code=200,
                        content=f"Successfully updated {component.name} "
                        + f"state for {instance_id}",
                    )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        return write_state_endpoint

    def create_component_endpoint(self, component: Component) -> Callable:
        """
        Creates an endpoint for running dataflows on a given component. This is
        called automatically when an application is created.

        Args:
            component (Component): The component instance for which to create
                the dataflow endpoint.

        Returns:
            Callable: An asynchronous function that serves as the endpoint for
                running component dataflows.
        """

        async def endpoint(
            request: RunRequest,
            background_tasks: BackgroundTasks,
            _: Any = Depends(api_key_auth(self.secret_token)),  # type: ignore
        ) -> Any:
            instance_id = request.instance_id
            dataflow_key = request.dataflow_key
            is_async = request.is_async
            props = request.props
            run_kwargs = request.run_kwargs
            creation_kwargs = request.creation_kwargs

            # Validate that the action is correct
            component_instance = None
            try:
                component_instance = component(instance_id, **creation_kwargs)

                # Run the relevant action
                if is_async:
                    result = await component_instance.arun(
                        dataflow_key=dataflow_key, props=props, **run_kwargs
                    )

                else:
                    result = component_instance.run(
                        dataflow_key=dataflow_key, props=props, **run_kwargs
                    )

                # Add flush update task to background
                if run_kwargs.get("flush_update", False):
                    background_tasks.add_task(
                        component_instance.flush_update,
                        **{"dataflow_key": dataflow_key},
                    )

                background_tasks.add_task(component_instance.shutdown)

                # Return direct result
                return result

            except Exception as e:
                if component_instance:
                    background_tasks.add_task(component_instance.shutdown)
                raise HTTPException(status_code=500, detail=str(e))

        return endpoint

    def get_app(self) -> FastAPI:
        """
        Returns the FastAPI application instance.

        Returns:
            FastAPI: The application's FastAPI instance.
        """
        return self.app

    def get_credentials(self) -> Dict[str, str]:
        """
        Returns the credentials of the application, including the secret token.

        Returns:
            Dict[str, str]: A dictionary containing the application's
                credentials. E.g., {"secret_token": "sk_abc123"}
        """
        return {"secret_token": self.secret_token}
