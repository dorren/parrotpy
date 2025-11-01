from functools import partial
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from typing import Union

from ..schema import BaseColumn
from ..functions.core import rand_array, rand_elem_or_array
from ..functions.stats import normal

class Uniform(BaseColumn):
    def __init__(self, name: str, dtype: str, n: int = 1, min_value: float = 0.0, max_value: float = 1.0, seed: int = None, to_int: bool = False):
        super().__init__(name, dtype)
        self.n = n
        self.min_value = min_value
        self.max_value = max_value
        self.seed = seed
        self.to_int = to_int

    def rand_num(self, min_value: float, max_value: float, seed: int, to_int: bool) -> Column:
        diff = max_value - min_value
        value = F.rand(seed) * diff + min_value
        value = value.cast("int") if to_int else value
        return value
    
    def generate(self, df: DataFrame) -> Column:
        gen_fn = partial(self.rand_num, min_value=self.min_value, max_value=self.max_value, to_int=self.to_int)
        col_val = rand_elem_or_array(self.n, gen_fn, self.seed)
        df = df.withColumn(self.name, col_val)
        
        return df
    
    def to_dict(self):
        attrs = {
            "name": self.name,
            "type": self.dtype,
            "gen": "uniform",
            "n": self.n,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "seed": self.seed,
            "to_int": self.to_int
        }
         
        return attrs
    

class Normal(BaseColumn):
    def __init__(self, name: str, dtype: str, n: int = 1, mean: float = 0.0, stddev: float = 1.0, seed: int = None, to_int: bool = False):
        super().__init__(name, dtype)
        self.n = n
        self.mean = mean
        self.stddev = stddev
        self.seed = seed
        self.to_int = to_int

    def generate(self, df: DataFrame) -> Column:
        gen_fn = partial(normal, mean=self.mean, stddev=self.stddev, to_int=self.to_int)
        col_val = rand_elem_or_array(self.n, gen_fn, self.seed)
        df = df.withColumn(self.name, col_val)
        
        return df
    
    def to_dict(self):
        attrs = {
            "name": self.name,
            "type": self.dtype,
            "gen": "normal",
            "n": self.n,
            "mean": self.mean,
            "stddev": self.stddev,
            "seed": self.seed,
            "to_int": self.to_int
        }
         
        return attrs