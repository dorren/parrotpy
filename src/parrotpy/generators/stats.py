from functools import partial
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from typing import Union

from ..schema import DfColumn
from ..functions.stats import uniform, normal

class Uniform(DfColumn):
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
        col_val = uniform(n=self.n, min_value=self.min_value, max_value=self.max_value, seed=self.seed, to_int=self.to_int)
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
    

class Normal(DfColumn):
    def __init__(self, name: str, dtype: str, n: int = 1, mean: float = 0.0, stddev: float = 1.0, seed: int = None, to_int: bool = False):
        super().__init__(name, dtype)
        fn = self.__init__
        print(fn.__code__.co_varnames[:fn.__code__.co_argcount])
        print(self.__init__.__defaults__)
        self.n = n
        self.mean = mean
        self.stddev = stddev
        self.seed = seed
        self.to_int = to_int

    def generate(self, df: DataFrame) -> Column:
        col_val = normal(n=self.n, mean=self.mean, stddev=self.stddev, seed=self.seed, to_int=self.to_int)
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
    

# experimental
def normal_gen(df: DataFrame, name: str, dtype: str, 
    n: int = 1, mean: float = 0.0, stddev: float = 1.0, seed: int = None, to_int: bool = False) -> DataFrame:
    return normal(n=n, mean=mean, stddev=stddev, seed=seed, to_int=to_int)
    
def col_gen(df: DataFrame, name: str, dtype: str, col_val: Column) -> DataFrame:
    return df.withColumn(name, col_val.cast(dtype))

