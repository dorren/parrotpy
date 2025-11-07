from functools import wraps
from typing import Any
from dataclasses import dataclass

@dataclass
class Invocation:
    """ 
        fn_path: module path, like "parrotpy.generators.stats.normal"
        fn_result: original function call result, 
        params: parameters used by the function call.
    """
    fn_path: str
    fn_result: Any
    params: dict

def snapshot(func):
    """ Decorator that captures function path, result, and parameter signatures """
    @wraps(func)
    def wrapper(*args, **kwargs):
        fn_path = f"{func.__module__}.{func.__name__}"
        result = func(*args, **kwargs)
        return Invocation(fn_path, result, kwargs)

    return wrapper