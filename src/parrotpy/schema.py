from functools import wraps
from typing import Any
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame

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
    
class DfColumn:
    def __init__(self, name: str, dtype: str, **kwargs: dict):
        self.name = name
        self.dtype = dtype
        self.kwargs = kwargs

    def generate(self, df: DataFrame) -> DataFrame:
        col_val = self.col_fn(**self.args)
        df = df.withColumn(self.name, col_val.cast(self.dtype))
        
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype,
            "gen":  self.gen
        }
        result = {**result, **self.kwargs}
        return result

class ComputedColumn:
    def __init__(self, name: str, dtype: str, col_val: Column):
        self.name = name
        self.dtype = dtype
        self.col_val = col_val

    def generate(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.name, self.col_val.cast(self.dtype))
        return df

    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype,
            "gen":  'TBD'
        }
        return result

class InvocationColumn:
    def __init__(self, name: str, dtype: str, invk: Invocation):
        self.name = name
        self.dtype = dtype
        self.invocation = invk

    def generate(self, df):
        df = df.withColumn(self.name, self.invocation.fn_result.cast(self.dtype))
        return df
    
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype,
            "gen": self.invocation.fn_path
        }
        combined = {**result, **self.invocation.params}

        return combined

class DfSchema:
    def __init__(self):
        self.columns = []
    
    def add_column(self, col: DfColumn):
        self.columns.append(col)
        return self

    def to_dict(self):
        result = {}
        result["columns"] = [col.to_dict() for col in self.columns]

        return result
