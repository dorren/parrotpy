from functools import wraps
import importlib
from typing import Any
from dataclasses import dataclass
import logging

def fn_path(func: callable):
    return f"{func.__module__}.{func.__name__}"

def get_fn(fn_path: str):
    """ get function reference from path """
    mod_name, name = fn_path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, name)
    return fn

@dataclass
class Snapshot:
    """ 
        fn_path: module path, like "parrotpy.generators.stats.normal"
        params: parameters used by the function call.
    """
    fn_path: str
    fn_params: dict

    def to_dict(self):
        return {
            "fn_path": self.fn_path,
            "fn_params": self.fn_params
        }
    
    def invoke(self):
        """ invoke the stored function and return result """
        fn = get_fn(self.fn_path)
        return fn(**self.fn_params)

def snapshot(func):
    """ Decorator that captures function path, parameter signatures, and function result """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 0:
            logging.error("""
                Because column building code may be converted to json config file, 
                for clarity, only named parameters are allowed, like param1=123.
            """)
            raise ValueError("Please use named parameters only. like param1=123")

        return Snapshot(fn_path(func), kwargs)

    return wrapper


__all__ = ["fn_path", "get_fn", "Snapshot", "snapshot"]