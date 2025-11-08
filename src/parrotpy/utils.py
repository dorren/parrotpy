from functools import wraps
from typing import Any
from dataclasses import dataclass

@dataclass
class Snapshot:
    """ 
        fn_module: module path, like "parrotpy.generators.stats.normal"
        fn_name: function name.
        fn_result: original function call result, 
        params: parameters used by the function call.
    """
    fn_module: str
    fn_name: str
    fn_params: dict
    result: Any

    def to_dict(self):
        return {
            "fn_path": self.fn_module + "." + self.fn_name,
            "fn_params": self.fn_kwargs
        }

def snapshot(func):
    """ Decorator that captures function path, parameter signatures, and function result """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 0:
            raise ValueError("Please use named parameters only. like param1=123")

        result = func(*args, **kwargs)
        return Snapshot(func.__module__, func.__name__, kwargs, result)

    return wrapper 