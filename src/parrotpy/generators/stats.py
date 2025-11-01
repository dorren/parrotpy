from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from typing import Union

from ..schema import BaseColumn
from ..functions.core import rand_array
from ..functions.stats import normal

class Uniform(BaseColumn):
    def __init__(self, name: str, dtype: str, n: int = 1, min_value: float = 0.0, max_value: float = 1.0, seed: int = None):
        super().__init__(name, dtype)
        self.n = n
        self.min_value = min_value
        self.max_value = max_value
        self.seed = seed

    def generate(self, df: DataFrame) -> Column:
        diff = self.max_value - self.min_value
        value = F.rand(self.seed) * diff + self.min_value
        if self.n == 1:
            df = df.withColumn(self.name, value)
        else:
            df = df.withColumn(self.name, rand_array(self.n, self.min_value, self.max_value, self.seed))

        return df
    
    def to_dict(self):
        attrs = {
            "name": self.name,
            "type": self.dtype,
            "gen": "uniform",
            "n": self.n,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "seed": self.seed
        }
         
        return attrs
    

class Normal(BaseColumn):
    def __init__(self, name: str, dtype: str, n: int = 1, mean: float = 0.0, stddev: float = 1.0, seed: int = None):
        super().__init__(name, dtype)
        self.n = n
        self.mean = mean
        self.stddev = stddev
        self.seed = seed

    def generate(self, df: DataFrame) -> Column:
        df = df.withColumn(self.name, normal(self.n, self.mean, self.stddev, self.seed))
        
        return df
    
    def to_dict(self):
        attrs = {
            "name": self.name,
            "type": self.dtype,
            "gen": "normal",
            "n": self.n,
            "mean": self.mean,
            "stddev": self.stddev,
            "seed": self.seed
        }
         
        return attrs