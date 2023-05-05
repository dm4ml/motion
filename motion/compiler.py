import inspect
from typing import Callable, Dict, List, Tuple

from motion.route import Route


class RouteCompiler:
    def __init__(self, component: object):
        self.component = component

    def get_decorated_methods(self, decorator_name: str) -> List[Callable]:
        methods = inspect.getmembers(self.component, predicate=inspect.ismethod)
        decorated_methods = [
            m[1] for m in methods if decorator_name in getattr(m[1], "__qualname__", "")
        ]
        return decorated_methods

    def compile_routes(self) -> Tuple[Dict[str, Route], Dict[str, Route]]:
        infer_methods = self.get_decorated_methods("infer")
        fit_methods = self.get_decorated_methods("fit")

        infer_routes = {}
        fit_routes = {}
        for method in infer_methods:
            inp = getattr(method, "_input_key", None)
            if not inp:
                raise ValueError(f"{method.__name__} must have a route key.")
            if inp in infer_routes:
                raise ValueError(
                    f"Infer route for key `{inp}` in component "
                    + f"{self.component.__class__.__name__} already exists."
                )
            infer_routes[inp] = Route(key=inp, op="infer", udf=method)

        for method in fit_methods:
            inp = getattr(method, "_input_key", None)
            if not inp:
                raise ValueError(f"{method.__name__} must have a route key.")
            if inp in fit_routes:
                raise ValueError(
                    f"Fit route for key `{inp}` in component "
                    + f"{self.component.__class__.__name__} already exists."
                )
            fit_routes[inp] = Route(key=inp, op="fit", udf=method)

        return infer_routes, fit_routes
