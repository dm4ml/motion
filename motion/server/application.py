"""
This file creates a FastAPI application instance for a group of components."""

import secrets
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List

import jwt
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
        flow_key (str): A key representing the specific flow or
            operation to be run on the component.
        is_async (bool): Flag to indicate if the operation should be performed
            asynchronously. Use this if you would call the component with
            `component.arun` instead of `component.run` (i.e., the operation is
            an async function). Default is False.
        props (Dict[str, Any]): A dictionary of properties specific to the
            component's flow that you want to run.
        run_kwargs (Dict[str, Any]): Additional keyword arguments to pass to
            the run method of the component.
        creation_kwargs (Dict[str, Any]): Keyword arguments to pass for
            component creation.
    """

    instance_id: str
    flow_key: str
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


class AuthRequest(BaseModel):
    """A pydantic model representing a request to authenticate an instance id.

    Attributes:
        instance_id (str): The unique identifier for the component instance.
        token_expiration_days (int): The number of days for which the token
            should be valid. Default is 1 day.
    """

    instance_id: str
    token_expiration_days: int = 1


# Authentication dependency
def api_key_auth(api_key: str) -> Callable:
    """
    Dependency for API key authentication. Validates the provided API key
    against the expected secret token.

    Args:
        api_key (str): The secret token used for API key validation.

    Returns:
        Callable: A function that validates the API key in the request header.

    Raises:
        HTTPException: If the API key is not provided or is invalid.
    """

    def validate_api_key(request: Request) -> bool:
        if "X-API-Key" not in request.headers:
            raise HTTPException(status_code=401, detail="No X-API-Key header provided")

        api_key = request.headers["X-API-Key"]
        expected_key = f"{api_key}"
        if secrets.compare_digest(api_key, expected_key):
            return True
        else:
            raise HTTPException(
                status_code=403, detail="Could not validate credentials"
            )

    return validate_api_key


def jwt_auth(api_key: str) -> Callable:
    def _jwt_validator(request: Request) -> Any:
        # Extract the JWT token from the request headers
        token = request.headers.get("Authorization")
        if not token or not token.startswith("Bearer "):
            raise HTTPException(
                status_code=401, detail="Token is missing or invalid format."
            )

        try:
            # Decode the JWT
            payload = jwt.decode(token.split(" ")[1], api_key, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired.")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token.")

    return _jwt_validator


class Application:
    """
    The main application class that sets up FastAPI routes for the given
        components.

    Attributes:
        components (List[Component]): A list of component instances to be
            included in the application.
        api_key (str): The secret token used for API key validation.

    Methods:
        get_app: Returns the FastAPI app instance.
        get_credentials: Returns the application's credentials, including the
            secret token.
    """

    def __init__(self, components: List[Component], api_key: str = "") -> None:
        """
        Initializes the Application instance.

        Args:
            components (List[Component]): List of component instances to be
                managed by the application.
            api_key (str, optional): Secret token for API key
                authentication. If not provided, a new token is generated.
        """
        self.app = FastAPI()
        self.components = components
        self.api_key = api_key if api_key else "sk_" + str(secrets.token_urlsafe(32))
        self._generate_routes()

    def _generate_routes(self) -> None:
        """
        Generates API routes for each component in the application. It sets up
        endpoints for component flows and state management.
        """
        # Create an endpoint for logging into an instance id
        self.app.post("/auth")(self.create_instance_id_endpoint())

        for component in self.components:
            component_name = component.name
            endpoint = self.create_component_endpoint(component)
            route = f"/{component_name}"
            self.app.post(route)(endpoint)

            update_route = f"/{component_name}/update"
            read_route = f"/{component_name}/read"

            self.app.post(update_route)(self.create_write_state_endpoint(component))
            self.app.get(read_route)(self.create_read_state_endpoint(component))

    def create_instance_id_endpoint(self) -> Callable:
        """
        Creates an endpoint for logging into a given instance id.

        Returns:
            Callable: An asynchronous function that serves as the endpoint for
                logging into an instance id.
        """

        async def auth_instance_id_endpoint(
            request: AuthRequest,
            _: Any = Depends(api_key_auth(self.api_key)),  # type: ignore
        ) -> JSONResponse:
            # Create a jwt token for the instance id
            instance_id = request.instance_id
            token_expiration_days = request.token_expiration_days

            try:
                # Define the payload of the JWT
                payload = {
                    "instance_id": instance_id,
                    "exp": datetime.utcnow()
                    + timedelta(days=token_expiration_days),  # Token expiration time
                }

                # Encode the JWT
                token = jwt.encode(payload, self.api_key, algorithm="HS256")

                # Return the token in a JSON response
                return JSONResponse(content={"token": token})
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        return auth_instance_id_endpoint

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
            tp: Dict[str, Any] = Depends(jwt_auth(self.api_key)),  # type: ignore
            _: Any = Depends(api_key_auth(self.api_key)),  # type: ignore
        ) -> JSONResponse:
            # Validate that the instance_id in the token matches the request
            if tp["instance_id"] != instance_id:
                raise HTTPException(
                    status_code=400, detail="Instance ID does not match token."
                )

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
            tp: Dict[str, Any] = Depends(jwt_auth(self.api_key)),  # type: ignore
            _: Any = Depends(api_key_auth(self.api_key)),  # type: ignore
        ) -> Response:
            instance_id = request.instance_id

            # Validate that the instance_id in the token matches the request
            if tp["instance_id"] != instance_id:
                raise HTTPException(
                    status_code=400, detail="Instance ID does not match token."
                )

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
        Creates an endpoint for running flows on a given component. This is
        called automatically when an application is created.

        Args:
            component (Component): The component instance for which to create
                the flow endpoint.

        Returns:
            Callable: An asynchronous function that serves as the endpoint for
                running component flows.
        """

        async def endpoint(
            request: RunRequest,
            background_tasks: BackgroundTasks,
            token_payload: Dict[str, Any] = Depends(jwt_auth(self.api_key)),
            _: Any = Depends(api_key_auth(self.api_key)),  # type: ignore
        ) -> Any:
            instance_id = request.instance_id

            # Validate that the instance_id in the token matches the request
            if token_payload["instance_id"] != instance_id:
                raise HTTPException(
                    status_code=400, detail="Instance ID does not match token."
                )

            flow_key = request.flow_key
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
                        flow_key=flow_key, props=props, **run_kwargs
                    )

                else:
                    result = component_instance.run(
                        flow_key=flow_key, props=props, **run_kwargs
                    )

                # Add flush update task to background
                if run_kwargs.get("flush_update", False):
                    background_tasks.add_task(
                        component_instance.flush_update,
                        **{"flow_key": flow_key},
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
                credentials. E.g., {"api_key": "sk_abc123"}
        """
        return {"api_key": self.api_key}
