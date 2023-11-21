"""This file creates a FastAPI application instance for a group of components."""

import secrets
from typing import Any, Dict, List

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, validator

from motion.component import Component


# Pydantic model for request payload
class RunRequest(BaseModel):
    instance_id: str
    dataflow_key: str
    async_action: bool = False
    props: Dict[str, Any]
    run_kwargs: Dict[str, Any] = {}  # kwargs to pass to the run method
    creation_kwargs: Dict[str, Any] = {}  # kwargs to pass to component creation

    @validator("props")
    def validate_props(cls, v):
        if not isinstance(v, dict):
            raise ValueError("Props must be a dictionary")
        return v


class UpdateStateRequest(BaseModel):
    instance_id: str
    state_update: Dict[str, Any]
    kwargs: Dict[str, Any]


# Authentication dependency
def api_key_auth(secret_token: str):
    def validate_api_key(request: Request):
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


# Application class
class Application:
    def __init__(
        self,
        components: List[Component],
        secret_token: str = None,
    ):
        self.app = FastAPI()
        self.components = components
        self.secret_token = (
            secret_token if secret_token else "sk_" + str(secrets.token_urlsafe(32))
        )
        self._generate_routes()

    def _generate_routes(self):
        for component in self.components:
            component_name = component.name
            endpoint = self.create_component_endpoint(component)
            route = f"/{component_name}"
            self.app.post(route)(endpoint)

            update_route = f"/{component_name}/update"
            read_route = f"/{component_name}/read"

            self.app.post(update_route)(self.create_update_state_endpoint(component))
            self.app.get(read_route)(self.create_read_state_endpoint(component))

    def create_read_state_endpoint(self, component):
        async def read_state_endpoint(
            instance_id: str,
            key: str,
            _=Depends(api_key_auth(self.secret_token)),
        ):
            try:
                with component(
                    instance_id, disable_update_task=True
                ) as component_instance:
                    value = component_instance.read_state(key)
                    # Return as JSON
                    return {key: value}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        return read_state_endpoint

    def create_update_state_endpoint(self, component):
        async def update_state_endpoint(
            request: UpdateStateRequest,
            _=Depends(api_key_auth(self.secret_token)),
        ):
            instance_id = request.instance_id
            state_update = request.state_update
            kwargs = request.kwargs

            # Assuming you have a method in your Component class to handle state updates
            try:
                with component(
                    instance_id, disable_update_task=True
                ) as component_instance:
                    component_instance.update_state(state_update, **kwargs)
                    return {
                        "status": "success",
                        "message": "State updated successfully.",
                    }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        return update_state_endpoint

    def create_component_endpoint(self, component):
        async def endpoint(
            request: RunRequest,
            background_tasks: BackgroundTasks,
            _=Depends(api_key_auth(self.secret_token)),
        ) -> Any:
            instance_id = request.instance_id
            dataflow_key = request.dataflow_key
            async_action = request.async_action
            props = request.props
            run_kwargs = request.run_kwargs
            creation_kwargs = request.creation_kwargs

            # Validate that the action is correct
            component_instance = None
            try:
                component_instance = component(instance_id, **creation_kwargs)

                # Run the relevant action
                if async_action:
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

    def get_app(self):
        return self.app

    def get_credentials(self):
        return {"secret_token": self.secret_token}


# Example usage
# app_instance = Application(components=[ComponentA, ComponentB])
# credentials = app_instance.get_credentials()
# app = app_instance.get_app()

# # Credentials are instance-specific and not global
# print("Credentials for this instance of the app:")
# print(credentials)

# To run the app, use uvicorn as follows:
# uvicorn.run(app, host="0.0.0.0", port=8000)
